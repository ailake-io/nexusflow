# Plano de Segurança e Qualidade — NexusFlow

Este documento consolida o estado atual de segurança e qualidade do repositório `ailake-io/nexusflow` e propõe ações corretivas priorizadas.

## 1. Estado atual (baseline)

### 1.1 Dependabot alerts — 9 abertos

| # | Severidade | Ecossistema | Pacote | CVE / Título | Bloqueante? |
|---|---|---|---|---|---|
| #9 | medium | Rust | `thrift` | CVE-2026-43868 — Memory Allocation with Excessive Size Value | Indireto (via iceberg/lance) |
| #4 | medium | Rust | `thrift` | CVE-2026-43868 (duplicado) | Indireto |
| #8 | low | Rust | `lru` | `IterMut` violates Stacked Borrows | Indireto (via datafusion/lance) |
| #3 | low | Rust | `lru` | Duplicado | Indireto |
| #7 | medium | Rust | `ring` | CVE-2025-4432 — AES panic com overflow checking | Indireto (via rustls / milvus-sdk) |
| #2 | medium | Rust | `ring` | Duplicado | Indireto |
| #6 | low | Rust | `lexical-core` | Múltiplos soundness issues | Indireto (via arrow/datafusion) |
| #1 | low | Rust | `lexical-core` | Duplicado | Indireto |
| #5 | medium | npm | `hono` | CVE-2026-69207 — ReDoS no CORS middleware | Direto? Verificar uso |

**Observação:** Os alertas estão duplicados porque os pacotes aparecem tanto no workspace root quanto no workspace aninhado `crates/nexus-connectors`.

### 1.2 GitHub Advanced Security — desabilitado

- **Code scanning:** não habilitado (`Advanced Security must be enabled`).
- **Secret scanning:** desabilitado.
- **Branch protection:** indisponível no plano gratuito para repositórios privados (requer GitHub Pro ou repositório público).

### 1.3 CI / Actions

- Node.js 20 deprecation warnings em todos os jobs que usam `actions/checkout@v4`, `Swatinem/rust-cache@v2`, `actions/setup-node@v4`, etc.
- Cache do GitHub Actions apresentou instabilidade no último run (`Failed to save/restore cache`).
- Actions de terceiros já estão pinadas por SHA (boa prática), mas algumas versões são antigas.

### 1.4 Qualidade de código

- `cargo clippy --workspace --all-targets --all-features -- -D warnings` passa.
- `npm run lint` no frontend passa.
- Cobertura de testes: backend razoável; frontend ainda com poucos testes comportamentais.

## 2. Plano de ação

### Fase A — Correções imediatas (esta semana)

| # | Ação | Responsável | Prova de conclusão |
|---|---|---|---|
| SQ-1 | Verificar se `hono` é dependência direta ou apenas transitiva do frontend; se direta, atualizar para versão corrigida. | Dev | `npm audit` sem alertas de `hono`; ou confirmação de que é dev-only/transitivo |
| SQ-2 | Atualizar todas as GitHub Actions para versões que rodem em Node.js 20+ e eliminem os deprecation warnings. | Dev | CI sem warnings de Node.js 20 |
| SQ-3 | Adicionar `permissions: contents: read` explícitas onde ainda faltar e revisar scopes de cada workflow. | Dev | `actionlint` ou inspeção manual sem gaps |
| SQ-4 | Corrigir SHA inválido do `Swatinem/rust-cache` no working tree (se ainda pendente). | Dev | `git diff` limpo; CI passa |

### Fase B — Hardening de segurança (próximas 2 semanas)

| # | Ação | Responsável | Prova de conclusão |
|---|---|---|---|
| SQ-5 | Habilitar GitHub Advanced Security: code scanning (CodeQL) e secret scanning. Se o repo for privado sem GitHub Pro, avaliar torná-lo público ou adquirir licença. | Admin | Aba "Security > Code scanning" e "Secret scanning" ativas e sem alertas críticos |
| SQ-6 | Adicionar workflow de CodeQL para Rust + TypeScript. | Dev | Workflow rodando sem erro em toda PR/push |
| SQ-7 | Resolver Dependabot alerts de Rust. Como são quase todos indiretos, requer `cargo update -p <crate>` ou atualização dos crates pais (datafusion, lance, iceberg). | Dev | Zero alerts abertos de severidade medium/high; low documentados se não houver fix |
| SQ-8 | Implementar branch protection equivalente via regras de merge (merge queue + required reviewers) ou mover para GitHub Pro. | Admin | PRs exigem aprovação e CI verde |

### Fase C — Qualidade contínua (semanas 3-4)

| # | Ação | Responsável | Prova de conclusão |
|---|---|---|---|
| SQ-9 | Adicionar testes de comportamento no frontend: `dag.ts`, `useRunProgress`, `LoginForm`, `PipelinesList`. | Dev | `npm test` executando e passando |
| SQ-10 | Adicionar smoke test no workflow de release: extrair tarball e rodar `--version`. | Dev | Step no `release.yml` verificando o binário |
| SQ-11 | Adicionar `cargo audit` como gate no CI principal (já existe job separado; garantir que falhas bloqueiem merge). | Dev | CI falha em advisories não ignorados |
| SQ-12 | Configurar Dependabot para atualizações automáticas de segurança (Rust + npm). | Admin | PRs automáticos de security updates visíveis |

## 3. Riscos e dependências

- **Risco 1:** Atualizar `datafusion`/`lance`/`iceberg` para resolver `thrift`/`lru`/`lexical-core` pode quebrar APIs. Requer testes de integração.
- **Risco 2:** Tornar o repositório público expõe código e histórico. Avaliar impacto comercial.
- **Risco 3:** GitHub Advanced Security requer licença para repositórios privados.

## 4. Métricas de sucesso

- Zero alerts Dependabot de severidade `medium`/`high` abertos por mais de 7 dias.
- CI sem warnings de deprecation do Node.js.
- Code scanning e secret scanning habilitados.
- Frontend com pelo menos 4 suites de testes comportamentais rodando no CI.
