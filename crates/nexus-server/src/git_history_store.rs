//! Git-backed version history for artifacts the user builds in the
//! Canvas — pipelines (`pipeline_store.rs`) and prompt templates
//! (`prompt_template_store.rs`). See the git-versioning follow-up plan
//! (LLMOPS_IMPLEMENTATION_PLAN.md's L4/L7 successors) for why: a
//! `PipelineSpec` update today just overwrites the row in `pipelines`,
//! with zero history — this module gives it a real, append-only commit
//! log instead, without requiring a GitHub account (that's an optional
//! push mirror layered on top elsewhere, gated by the
//! `git-history-github-sync` capability).
//!
//! Backed by a single **bare** repository (no working directory — every
//! read/write goes through libgit2's object database directly), one
//! branch, `refs/heads/main`. Callers pick the path convention:
//! `pipelines/{id}.json` (content = the same `spec_ciphertext` already
//! persisted in SQL — never the decrypted JSON, see `pipeline_store.rs`'s
//! `encode_spec`) or `prompts/{name}/v{version}.txt` (plain text, prompts
//! carry no secrets). This module has no opinion on either convention or
//! on encryption — it just versions byte blobs at string paths.
//!
//! `git2` (libgit2) is a synchronous, blocking library — every method
//! here wraps its git2 calls in `tokio::task::spawn_blocking` so it never
//! stalls the async runtime.

use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum GitHistoryError {
    #[error("git operation failed: {0}")]
    Git(#[from] git2::Error),
    #[error("git background task panicked: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("path {0:?} not found at commit {1}")]
    PathNotFound(String, String),
    #[error("commit {0:?} not found")]
    CommitNotFound(String),
}

/// One entry in a path's history — one per commit that actually changed
/// (or created) the blob at that path, newest first.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct VersionMeta {
    /// Full commit SHA (hex), stable identifier for `blob_at`/rollback.
    pub commit: String,
    pub message: String,
    /// Git author name — callers pass `Claims.sub` in here, not a real
    /// email (see `commit_blob`'s doc comment).
    pub author: String,
    /// Unix timestamp (seconds), the author time git recorded the commit.
    pub timestamp: i64,
}

/// Wraps a bare `git2::Repository`. `Clone` is cheap — `git2::Repository`
/// isn't `Sync`, so each clone reopens its own handle from the same path
/// (matches `PipelineStore`/`PromptTemplateStore`'s own `#[derive(Clone)]`
/// pattern of sharing a cheap handle, here a `PathBuf` instead of a pool).
#[derive(Clone)]
pub struct GitHistoryStore {
    repo_path: Arc<PathBuf>,
}

const BRANCH: &str = "main";
const BRANCH_REF: &str = "refs/heads/main";

impl GitHistoryStore {
    /// Opens the bare repo at `repo_path`, initializing it (bare, empty,
    /// no first commit yet — `commit_blob` handles the parentless-commit
    /// case) if it doesn't exist yet. Synchronous (repo init/open is cheap
    /// and only happens once at boot), matches `PipelineStore::connect`'s
    /// shape otherwise.
    pub fn open(repo_path: impl Into<PathBuf>) -> Result<Self, GitHistoryError> {
        let repo_path = repo_path.into();
        if repo_path.exists() {
            git2::Repository::open_bare(&repo_path)?;
        } else {
            std::fs::create_dir_all(repo_path.parent().unwrap_or(Path::new(".")))
                .map_err(|e| git2::Error::from_str(&e.to_string()))?;
            git2::Repository::init_bare(&repo_path)?;
        }
        Ok(Self {
            repo_path: Arc::new(repo_path),
        })
    }

    fn open_handle(&self) -> Result<git2::Repository, git2::Error> {
        git2::Repository::open_bare(&*self.repo_path)
    }

    /// Commits `content` at `path` on `main`, preserving every other path
    /// already in the tree (rebuilt from the current `HEAD` tree, or an
    /// empty tree for the very first commit). `author` becomes both the
    /// git author and committer name — this repo is an internal audit
    /// log, not meant to be a real clone-able project identity, so a
    /// synthetic `{author}@nexusflow.local` email is used rather than
    /// requiring one from the caller. Returns the new commit's full SHA.
    pub async fn commit_blob(
        &self,
        path: &str,
        content: &[u8],
        message: &str,
        author: &str,
    ) -> Result<String, GitHistoryError> {
        let this = self.clone();
        let path = path.to_string();
        let content = content.to_vec();
        let message = message.to_string();
        let author = author.to_string();
        tokio::task::spawn_blocking(move || {
            this.commit_blob_sync(&path, &content, &message, &author)
        })
        .await?
    }

    fn commit_blob_sync(
        &self,
        path: &str,
        content: &[u8],
        message: &str,
        author: &str,
    ) -> Result<String, GitHistoryError> {
        let repo = self.open_handle()?;
        let blob_oid = repo.blob(content)?;

        let parent_commit = repo
            .find_branch(BRANCH, git2::BranchType::Local)
            .ok()
            .and_then(|b| b.get().target())
            .map(|oid| repo.find_commit(oid))
            .transpose()?;
        let base_tree = parent_commit.as_ref().map(|c| c.tree()).transpose()?;

        let new_tree_oid = insert_blob_at_path(&repo, base_tree.as_ref(), path, blob_oid)?;
        let new_tree = repo.find_tree(new_tree_oid)?;

        let email = format!("{author}@nexusflow.local");
        let signature = git2::Signature::now(author, &email)?;
        let parents: Vec<&git2::Commit> = parent_commit.iter().collect();
        let commit_oid = repo.commit(
            Some(BRANCH_REF),
            &signature,
            &signature,
            message,
            &new_tree,
            &parents,
        )?;

        Ok(commit_oid.to_string())
    }

    /// Every commit on `main` that changed (or created) the blob at
    /// `path`, newest first — `git log -- path`, reimplemented by hand
    /// since `git2` has no single call for it: walks history comparing
    /// each commit's tree entry at `path` against its first parent's.
    pub async fn history_for(&self, path: &str) -> Result<Vec<VersionMeta>, GitHistoryError> {
        let this = self.clone();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || this.history_for_sync(&path)).await?
    }

    fn history_for_sync(&self, path: &str) -> Result<Vec<VersionMeta>, GitHistoryError> {
        let repo = self.open_handle()?;
        let Some(head_oid) = repo
            .find_branch(BRANCH, git2::BranchType::Local)
            .ok()
            .and_then(|b| b.get().target())
        else {
            return Ok(Vec::new());
        };

        let mut revwalk = repo.revwalk()?;
        revwalk.push(head_oid)?;
        revwalk.set_sorting(git2::Sort::TIME)?;

        let mut out = Vec::new();
        for oid in revwalk {
            let oid = oid?;
            let commit = repo.find_commit(oid)?;
            let entry_oid = tree_entry_oid(&commit.tree()?, path);
            let parent_entry_oid = commit
                .parent(0)
                .ok()
                .and_then(|p| p.tree().ok())
                .and_then(|t| tree_entry_oid(&t, path));
            if entry_oid.is_some() && entry_oid != parent_entry_oid {
                let author = commit.author();
                out.push(VersionMeta {
                    commit: oid.to_string(),
                    message: commit.message().unwrap_or("").trim().to_string(),
                    author: author.name().unwrap_or("unknown").to_string(),
                    timestamp: commit.time().seconds(),
                });
            }
        }
        Ok(out)
    }

    /// Best-effort mirror of `main` to an external remote (GitHub, in
    /// practice — "caso o usuário queira", the git-versioning follow-up
    /// plan's Part 4). `token` authenticates as `x-access-token`, the
    /// convention GitHub personal-access/App tokens use over HTTPS.
    /// Callers (see `lib.rs`) only invoke this when a remote is
    /// configured *and* licensed for the `git-history-github-sync`
    /// capability — this method itself has no opinion on licensing, it
    /// just pushes.
    pub async fn push_to_remote(
        &self,
        remote_url: &str,
        token: &str,
    ) -> Result<(), GitHistoryError> {
        let this = self.clone();
        let remote_url = remote_url.to_string();
        let token = token.to_string();
        tokio::task::spawn_blocking(move || this.push_to_remote_sync(&remote_url, &token)).await?
    }

    fn push_to_remote_sync(&self, remote_url: &str, token: &str) -> Result<(), GitHistoryError> {
        let repo = self.open_handle()?;
        let mut remote = repo.remote_anonymous(remote_url)?;
        let mut callbacks = git2::RemoteCallbacks::new();
        let token = token.to_string();
        callbacks.credentials(move |_url, _username_from_url, _allowed| {
            git2::Cred::userpass_plaintext("x-access-token", &token)
        });
        let mut push_options = git2::PushOptions::new();
        push_options.remote_callbacks(callbacks);
        remote.push(
            &[format!("{BRANCH_REF}:{BRANCH_REF}")],
            Some(&mut push_options),
        )?;
        Ok(())
    }

    /// The blob content at `path` as of `commit` (a full SHA from
    /// `history_for`). Used for diffing against another version and for
    /// rollback.
    pub async fn blob_at(&self, commit: &str, path: &str) -> Result<Vec<u8>, GitHistoryError> {
        let this = self.clone();
        let commit = commit.to_string();
        let path = path.to_string();
        tokio::task::spawn_blocking(move || this.blob_at_sync(&commit, &path)).await?
    }

    fn blob_at_sync(&self, commit: &str, path: &str) -> Result<Vec<u8>, GitHistoryError> {
        let repo = self.open_handle()?;
        let oid = git2::Oid::from_str(commit)
            .map_err(|_| GitHistoryError::CommitNotFound(commit.to_string()))?;
        let commit = repo
            .find_commit(oid)
            .map_err(|_| GitHistoryError::CommitNotFound(commit.to_string()))?;
        let tree = commit.tree()?;
        let entry = tree
            .get_path(Path::new(path))
            .map_err(|_| GitHistoryError::PathNotFound(path.to_string(), oid.to_string()))?;
        let blob = repo.find_blob(entry.id())?;
        Ok(blob.content().to_vec())
    }
}

fn tree_entry_oid(tree: &git2::Tree, path: &str) -> Option<git2::Oid> {
    tree.get_path(Path::new(path)).ok().map(|e| e.id())
}

/// Rebuilds a tree with `content_oid` inserted (or replaced) at `path`,
/// preserving every other entry in `base_tree` — including entries under
/// different subdirectories (e.g. inserting `pipelines/p1.json` leaves
/// `prompts/x/v1.txt` untouched). `git2::TreeBuilder` only operates one
/// level at a time, so a multi-segment path is handled by recursing down
/// existing subtrees (or starting an empty one) and rebuilding each
/// parent level bottom-up.
fn insert_blob_at_path(
    repo: &git2::Repository,
    base_tree: Option<&git2::Tree>,
    path: &str,
    content_oid: git2::Oid,
) -> Result<git2::Oid, git2::Error> {
    let mut segments: Vec<&str> = path.split('/').collect();
    let file_name = segments
        .pop()
        .expect("path always has at least one segment");

    // Recurses down to the deepest directory segment, inserting the blob
    // there, then rebuilds each parent tree bottom-up with the new child
    // subtree oid — every existing sibling entry at each level (including
    // ones under a completely different top-level prefix, e.g. `prompts/`
    // while writing under `pipelines/`) is preserved by `treebuilder`
    // starting from that level's existing tree.
    fn build(
        repo: &git2::Repository,
        tree: Option<&git2::Tree>,
        dirs: &[&str],
        file_name: &str,
        content_oid: git2::Oid,
    ) -> Result<git2::Oid, git2::Error> {
        let mut builder = repo.treebuilder(tree)?;
        if dirs.is_empty() {
            builder.insert(file_name, content_oid, git2::FileMode::Blob.into())?;
            return builder.write();
        }
        let (head, rest) = (dirs[0], &dirs[1..]);
        let existing_subtree = tree
            .and_then(|t| t.get_name(head))
            .filter(|e| e.kind() == Some(git2::ObjectType::Tree))
            .and_then(|e| repo.find_tree(e.id()).ok());
        let new_subtree_oid = build(
            repo,
            existing_subtree.as_ref(),
            rest,
            file_name,
            content_oid,
        )?;
        builder.insert(head, new_subtree_oid, git2::FileMode::Tree.into())?;
        builder.write()
    }

    build(repo, base_tree, &segments, file_name, content_oid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (GitHistoryStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = GitHistoryStore::open(dir.path().join("history.git")).unwrap();
        (store, dir)
    }

    #[tokio::test]
    async fn first_commit_creates_one_history_entry() {
        let (store, _dir) = store();
        let commit = store
            .commit_blob("pipelines/p1.json", b"v1", "create pipeline p1", "alice")
            .await
            .unwrap();

        let history = store.history_for("pipelines/p1.json").await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].commit, commit);
        assert_eq!(history[0].author, "alice");
        assert_eq!(history[0].message, "create pipeline p1");
    }

    #[tokio::test]
    async fn second_commit_to_the_same_path_adds_a_second_entry_newest_first() {
        let (store, _dir) = store();
        let c1 = store
            .commit_blob("pipelines/p1.json", b"v1", "create pipeline p1", "alice")
            .await
            .unwrap();
        let c2 = store
            .commit_blob("pipelines/p1.json", b"v2", "update pipeline p1", "bob")
            .await
            .unwrap();

        let history = store.history_for("pipelines/p1.json").await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].commit, c2, "newest first");
        assert_eq!(history[1].commit, c1);
    }

    #[tokio::test]
    async fn blob_at_an_old_commit_still_returns_the_old_content() {
        let (store, _dir) = store();
        let c1 = store
            .commit_blob("pipelines/p1.json", b"v1", "create", "alice")
            .await
            .unwrap();
        store
            .commit_blob("pipelines/p1.json", b"v2", "update", "alice")
            .await
            .unwrap();

        let content = store.blob_at(&c1, "pipelines/p1.json").await.unwrap();
        assert_eq!(content, b"v1");
    }

    #[tokio::test]
    async fn separate_paths_do_not_pollute_each_others_history() {
        let (store, _dir) = store();
        store
            .commit_blob("pipelines/p1.json", b"v1", "create p1", "alice")
            .await
            .unwrap();
        store
            .commit_blob("pipelines/p2.json", b"v1", "create p2", "alice")
            .await
            .unwrap();
        store
            .commit_blob("pipelines/p1.json", b"v2", "update p1", "alice")
            .await
            .unwrap();

        assert_eq!(
            store.history_for("pipelines/p1.json").await.unwrap().len(),
            2
        );
        assert_eq!(
            store.history_for("pipelines/p2.json").await.unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn unrelated_prefixes_coexist_in_the_same_tree() {
        let (store, _dir) = store();
        store
            .commit_blob(
                "pipelines/p1.json",
                b"pipeline content",
                "create p1",
                "alice",
            )
            .await
            .unwrap();
        store
            .commit_blob(
                "prompts/summarize/v1.txt",
                b"prompt content",
                "create prompt",
                "alice",
            )
            .await
            .unwrap();

        assert_eq!(
            store
                .blob_at(
                    &store.history_for("pipelines/p1.json").await.unwrap()[0].commit,
                    "pipelines/p1.json"
                )
                .await
                .unwrap(),
            b"pipeline content"
        );
        assert_eq!(
            store
                .blob_at(
                    &store.history_for("prompts/summarize/v1.txt").await.unwrap()[0].commit,
                    "prompts/summarize/v1.txt"
                )
                .await
                .unwrap(),
            b"prompt content"
        );
    }

    #[tokio::test]
    async fn history_for_a_never_written_path_is_empty() {
        let (store, _dir) = store();
        store
            .commit_blob("pipelines/p1.json", b"v1", "create p1", "alice")
            .await
            .unwrap();

        assert!(store
            .history_for("pipelines/does-not-exist.json")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn blob_at_unknown_commit_errors() {
        let (store, _dir) = store();
        let err = store
            .blob_at("0".repeat(40).as_str(), "pipelines/p1.json")
            .await;
        assert!(matches!(err, Err(GitHistoryError::CommitNotFound(_))));
    }

    /// Reopening the store at the same path (simulating a server restart)
    /// still sees everything committed before — proves this is real
    /// on-disk persistence, not an in-memory stand-in.
    #[tokio::test]
    async fn history_survives_reopening_the_store_at_the_same_path() {
        let dir = tempfile::tempdir().unwrap();
        let repo_path = dir.path().join("history.git");
        {
            let store = GitHistoryStore::open(&repo_path).unwrap();
            store
                .commit_blob("pipelines/p1.json", b"v1", "create p1", "alice")
                .await
                .unwrap();
        }
        let reopened = GitHistoryStore::open(&repo_path).unwrap();
        assert_eq!(
            reopened
                .history_for("pipelines/p1.json")
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
