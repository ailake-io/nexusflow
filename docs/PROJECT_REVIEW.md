# Revisão Geral do Projeto NexusFlow

Documento único de backlog técnico. Consolida:

- O plano de revisão anterior (`docs/REVIEW_ACTION_PLAN.md`), absorvendo os itens ainda abertos.
- A auditoria atual do backend Rust, frontend, documentação e infraestrutura (CI/CD, Dockerfile, scripts, packaging).
- Bugs reais encontrados em execução.
- Próximas iniciativas (Store de Plugins / Enterprise Connectors).

> **Escopo da auditoria:** branch `develop`, estado atual do working tree. Os achados foram verificados no código-fonte; referências (`path:linha`) se referem ao ponto atual do histórico.

---

## 1. Resumo Executivo

| Severidade | Quantidade | Risco resumido |
|---|---|---|
| Crítico | 0 | Todos os itens críticos originais foram resolvidos ou mitigados. |
| Alto | 2 | Imagem Docker `:full` não publicada; revision ONNX não fixada. |
| Moderado | 14 | SSRF via DNS, DeltaSink mascarando erros, docs de CDC/enterprise desatualizadas, i18n de erros no DAG, isolamento de runners. |
| Baixo | 16 | Dívida técnica diversa (índices, cache headers, validações de schema, typos/docs). |

**Conclusão imediata:** o projeto está estável para release de Linux x86_64 (tarball/.deb/.rpm/AppImage). Os principais riscos residuais são documentação prometendo imagem Docker que ainda não é publicada (A09/M30) e o `DeltaSink` mascarando erros de abertura de tabela (M09).

---

## 2. Backlog Ativo — Itens Pendentes

### 2.1 Alto impacto

| ID | Problema | Evidência | Impacto | Ação recomendada |
|---|---|---|---|---|
| **A08** | **Revisão do modelo ONNX não é configurável**, contradiz `ARCHITECTURE.md` que promete revision fixada. | `ARCHITECTURE.md:117`; `crates/nexus-ai/src/embedding/pipeline.rs:68` | Reprodutibilidade quebrada; modelo pode mudar silenciosamente. | Adicionar campo `revision` ao `EmbeddingModelSpec` ou corrigir a doc. |
| **A09** | **Imagem Docker publicada não tem `connectors-all`** e release não publica imagem Docker, contradizindo guias do usuário. | `docs/USER_GUIDE.md:35`; `docs/GETTING_STARTED.md:78`; `.github/workflows/release.yml`; `Dockerfile:22` | Documentação promete imagem `:full`/multi-registry que não existe. | Publicar imagem com `FEATURES=embed-ui,connectors-all` no GHCR ou corrigir docs. |

### 2.2 Moderados

#### Backend

| ID | Problema | Evidência | Ação recomendada |
|---|---|---|---|
| **M03** | **SSRF: `is_internal_host` não resolve DNS**; domínio público para IP privado passa. | `crates/nexus-core/src/dag.rs:607-674` | Validar IP resolvido na conexão ou documentar limitação claramente. |
| **M05** | **`parquet` não está na lista de conectores de path local.** | `crates/nexus-core/src/dag.rs:539-543`; `nexus-connector-parquet/src/config.rs:12` | Incluir `"parquet"` em `is_local_path_connector`. |
| **M09** | **`DeltaSink::open()` mascara qualquer erro como "tabela não existe".** | `nexus-connector-deltalake/src/sink.rs:39` | Distinguir `NotFound` de outros erros. |
| **M13** | **`alerts.rs` fire-and-forget em produção** (falhas são logadas, mas não há `JoinHandle` aguardado). | `crates/nexus-server/src/alerts.rs:35-46` | Documentar como by design ou adicionar opcional de await em shutdown. |
| **M18** | **Mensagens de erro no `dag.ts` ignoram i18n** (sempre em inglês). | `frontend/src/lib/dag.ts:178-347` | Usar chaves de tradução em todos os erros visíveis. |
| **M26** | **6 conectores CDC não têm referência de config no `USER_GUIDE`.** | `docs/USER_GUIDE.md` §4; configs em `nexus-connector-postgres/src/config.rs:36-58`, etc. | Adicionar seção §4.9 com config de cada CDC. |
| **M27** | **Features `embeddings`/`embeddings-api`/`*-cdc` não são forwardadas pelo crate raiz.** | `Cargo.toml` raiz:20-47 | Adicionar forwards ou documentar a limitação. |
| **M28** | **Chunking "semantic" documentado mas não selecionável no DAG.** | `CLAUDE.md:127`; `ARCHITECTURE.md:113`; `ROADMAP.md:63`; `crates/nexus-ai/src/chunking.rs:155`; `crates/nexus-core/src/dag.rs:127-139` | Adicionar variante ao `ChunkingSpec` ou marcar como biblioteca-only. |
| **M29** | **`ENTERPRISE_LICENSING.md` desatualizado**: menciona `LicenseStore::is_connector_licensed` e rotas como implementadas; a função não existe. | `docs/ENTERPRISE_LICENSING.md:3,63-64` | Corrigir doc para refletir estado real: gate de catálogo pronto, gate de runtime e serviço de pagamento pendentes. |
| **M30** | **Docker Hub ainda descrito como publicação ativa** em `CLAUDE.md`/`ROADMAP`. | `CLAUDE.md:163`; `ROADMAP.md:16,94` | Atualizar para "GHCR quando configurado; imagem Docker não publicada automaticamente no release atual". |
| **M31** | **`install.sh` anuncia macOS** sem assets correspondentes no release. | `docs/GETTING_STARTED.md:36`; `scripts/install.sh:2,34` | Restringir script a Linux-x86_64-only por ora. |
| **M37** | **Runners self-hosted sem isolamento para PRs.** | `.github/workflows/ci.yml` | Usar GitHub-hosted para PRs, adicionar environment de aprovação, ou documentar risco (repo privado hoje). |
| **B31** | **`pipeline_store.rs` não cria índices** explícitos além da PK. | `crates/nexus-server/src/pipeline_store.rs:120-148` | Adicionar `CREATE INDEX` em `pipeline_runs.pipeline_id`, `pipelines.id`, etc. |

#### Frontend

| ID | Problema | Evidência | Ação recomendada |
|---|---|---|---|
| **M18** | Mensagens de erro em `dag.ts` em inglês. | `frontend/src/lib/dag.ts:178-347` | i18n. |
| **B12** | Caso `idle` morto e lógica duplicada de status. | `frontend/src/components/ExecutionPanel.tsx:30,35`; `PipelineStatusBoard.tsx` | Remover caso morto; extrair helper. |
| **B39** | `handleImport` não valida JSON. | `frontend/src/components/PipelineIoPanel.tsx:71-78` | Validar schema mínimo. |
| **B40** | `index.html` lang="en" fixo. | `frontend/index.html:2` | Sincronizar com idioma selecionado. |

#### Infraestrutura

| ID | Problema | Evidência | Ação recomendada |
|---|---|---|---|
| **M34** | `release.yml` baixa `appimagetool` de release `continuous` (tag flutuante). | `.github/workflows/release.yml` | Pinar tag explícita, mesmo que SHA-256 já esteja verificado. |
| **M37** | Runners self-hosted sem isolamento para PRs. | `.github/workflows/ci.yml` | Ver tabela de backend. |
| **B16** | `actions/upload-artifact@v4` sem pin de SHA. | `.github/workflows/release.yml:189,224` | Pinar SHA como as demais actions. |
| **B42** | Build de Windows inacabado (`.msi` não produzido). | `packaging/windows/main.wxs` | Validar e finalizar scripts ou remover do release. |

### 2.3 Baixos

| ID | Problema | Evidência | Ação recomendada |
|---|---|---|---|
| **B01** | Race check-then-insert em `PipelineStore::create`. | `crates/nexus-server/src/pipeline_store.rs:186-208` | Capturar violação UNIQUE e retornar 409. |
| **B02** | Upsert SQL com `SET` vazio quando a única coluna é a PK. | `nexus-connector-postgres/src/sink.rs:70-83`; `nexus-connector-sqlite/src/sink.rs:62-75` | Usar `DO NOTHING` ou rejeitar na validação. |
| **B03** | Overflow no backoff de retry (`2u32.pow(attempt)`) no `RestSource`. | `nexus-connector-rest/src/source.rs:132` | Cap em `retries`; usar `saturating_pow`/backoff limitado. |
| **B05** | Audit log de login nunca registra IP. | `crates/nexus-server/src/lib.rs:364` | Extrair IP via `ConnectInfo` e gravar. |
| **B06** | Rate limiter de login usa IP do peer direto (problema atrás de proxy). | `crates/nexus-server/src/rate_limit.rs:84-88` | Documentar/validar config de proxy confiável. |
| **B23** | Mecanismo de conector enterprise descrito de formas contraditórias. | `CLAUDE.md:59`; `ARCHITECTURE.md:134`; `LICENSING.md:28`; `ENTERPRISE_LICENSING.md` | Alinhar texto; ver `docs/PLUGIN_STORE_PLAN.md`. |
| **B24** | USER_GUIDE contradiz-se sobre lancedb pré-existente vs criado automaticamente. | `docs/USER_GUIDE.md:381` | Explicitar exceção do lancedb. |
| **B25** | ROADMAP cita "14 conectores" desatualizado. | `ROADMAP.md:103` | Atualizar ou remover número. |
| **B27** | `embedded_ui.rs` sem cache headers. | `crates/nexus-server/src/embedded_ui.rs:17-27` | Adicionar `cache-control`. |
| **B33** | `telemetry.rs` `try_init()` não é idempotente. | `crates/nexus-server/src/telemetry.rs:34` | Tornar idempotente. |
| **B34** | `nexus-ai` não valida assinatura/checksum do modelo. | `crates/nexus-ai/src/embedding/model.rs:30-40` | Documentar risco; planejar pin de hash. |

---

## 3. Itens Críticos Resolvidos

Os itens abaixo foram bloqueadores na revisão anterior e foram corrigidos:

| ID | Problema | Resolução |
|---|---|---|
| **C01** | Fan-out perdia batches silenciosamente (`Lagged` tratado como `Closed`). | `crates/nexus-core/src/pipeline.rs:387-391` — `Lagged(n)` agora é erro fatal. |
| **C03** | `LicenseStore` era SQLite-only. | `crates/nexus-server/src/license_store.rs:14-66` — usa `MetadataPool` (SQLite/Postgres). |
| **C04** | Step de AppImage falhava. | `scripts/package-appimage.sh:18-21` — separa binário e flags. |

### C02 — CDC nativo (atualizado)

**Status:** mitigado, não totalmente resolvido.

- Conectores CDC nativos agora operam em **micro-batch** (`max_batch_events` default 1000) — não mais streams infinitos no caminho de execução batch.
- A retomada ainda depende de cursor estático na config (`starting_version`/`starting_snapshot_id`/slot/token/binlog position), não de checkpoint automático de cursor/LSN gerenciado pelo NexusFlow.
- **Recomendação:** manter documentado como limitação até implementar checkpoint explícito de cursor CDC.

---

## 4. Itens de Alto Impacto Resolvidos

| ID | Problema | Resolução |
|---|---|---|
| **A01** | `IcebergSink` append-only duplicava linhas. | `nexus-connector-iceberg/src/sink.rs:186-225` — dedup por PK quando configurado. |
| **A02** | Overlap de run manual com run agendada. | `crates/nexus-server/src/lib.rs:450-459` — `has_running_run` retorna 409. |
| **A03** | Embedding corrompia NULLs. | `crates/nexus-ai/src/embedding/pipeline.rs:137-139,260-264` — NULLs preservados. |
| **A04** | `new_empty_array` dava `panic!`. | `crates/nexus-ai/src/embedding/pipeline.rs:387-450` — retorna `Err`; teste cobre. |
| **A05** | Exemplos de API do `GETTING_STARTED` quebravam. | `docs/GETTING_STARTED.md:199-214` — exemplos corrigidos. |
| **A06** | Exemplo de embedding quebrava. | `docs/GETTING_STARTED.md:96-115` — exemplo corrigido. |
| **A07** | `NEXUS_ALLOW_INTERNAL_HOSTS` não documentado. | `docs/GETTING_STARTED.md:140` — na tabela de env vars. |
| **A10** | Job `docker-image` vazava container. | `.github/workflows/ci.yml:159-162` — `docker rm -f` + `trap`. |

---

## 5. Itens Moderados Resolvidos (amostra)

| ID | Problema | Resolução |
|---|---|---|
| **M01** | Ordenação por `started_at` ambígua. | `pipeline_store.rs:567` — `ORDER BY id DESC`. |
| **M02** | Scheduler lia 10.000 runs. | `scheduler.rs:127` — `LIMIT 1`. |
| **M04** | `WebhookSink` seguia redirects. | `source.rs:31`, `sink.rs:25` — `Policy::none()`. |
| **M06** | Migração não cobria `pipeline_run_logs`/`license`. | `migrate.rs:24-31,61,63`. |
| **M07** | ONNX síncrono no async. | `pipeline.rs:37` — `spawn_blocking`. |
| **M08** | Opcode CDC desconhecido virava upsert. | `cdc.rs:62-69` — rejeita via `Opcode::from_letter`. |
| **M10** | Colisão de nome de arquivo no Iceberg. | `sink.rs:137-140` — `write_counter` + `call_id`. |
| **M11** | Re-mapeamento posicional no Iceberg. | `sink.rs:124-131` — reescrita contra schema field-id. |
| **M12** | `ProgressHub` com `std::sync::Mutex`. | `progress.rs:7,110` — `tokio::sync::Mutex`. |
| **M13** | Alertas fire-and-forget sem observar falhas. | `alerts.rs:159-172` — falhas logadas; testes aguardam. |
| **M14/M15** | `pipeline_id`/`NodeSpec.name` sem validação. | `dag.rs:210-227,243-258`. |
| **M16** | `sanitize_error` não removia query strings. | `error.rs:166-188` — redige com `url::Url`. |
| **M17** | Execução travada em `running` na UI. | `useRunProgress.ts:58-118` — timeout + `setError` i18n. |
| **M19** | Testes do frontend não rodavam no CI. | `ci.yml:204` — `npm test`. |
| **M20** | `dag.ts` sem testes. | `frontend/src/lib/dag.test.ts` existe. |
| **M21** | Labels sem associação acessível. | `UsersPanel.tsx`, `PipelineIoPanel.tsx` — `htmlFor`/`id`. |
| **M22/M23** | Dependências do frontend. | `package.json` — radix individual, shadcn em `devDependencies`. |
| **M24** | Zero testes de comportamento. | `dag.test.ts`, `api.test.ts`, `I18nProvider.test.tsx`. |
| **M25** | "18 conectores" desatualizado. | `GETTING_STARTED.md:42,80` — 24 conectores. |
| **M32** | Drivers ADBC sem pin. | `scripts/build-adbc-*.sh` — `ADBC_REF` pinado; cache key com ref. |
| **M33** | `install.sh` sem verificação de checksum. | `install.sh:64-85` — falha fechado + `.asc`. |
| **M34** | `appimagetool` sem verificação. | SHA-256 pinado no release. |
| **M35** | CI não rodava em PRs. | `ci.yml:6` — `pull_request`. |
| **M36** | `.deb`/`.rpm` sem systemd. | `package-deb.sh`, `package-rpm.sh` — unit, usuário, postinst. |
| **M38** | Tags flutuantes no Dockerfile. | `Dockerfile:15,24,35,44` — pin por digest. |

---

## 6. Itens Baixos Resolvidos (amostra)

| ID | Problema | Resolução |
|---|---|---|
| **B04** | Porta 8080 hardcoded. | `lib.rs:1394-1397` — `NEXUS_PORT`. |
| **B07** | Fallback "Loading…" em inglês. | `App.tsx:29` — `t('common.loading')`. |
| **B08** | Tooltip hardcoded. | `PipelinesList.tsx:42` — chave i18n. |
| **B09** | Placeholder de cron em português. | `PipelineIoPanel.tsx:154` — chave i18n. |
| **B11** | Headline do login frágil. | `LoginForm.tsx:47-51` — dividido. |
| **B13/B14** | Manifestos Windows/macOS inacabados. | Windows só `main.wxs`; macOS removido. |
| **B15** | Comentário do release.yml desatualizado. | Comentário atualizado. |
| **B17** | Comentários de `cargo-audit` contraditórios. | Reescritos. |
| **B18** | Swarm permite senha vazia. | `docker-stack.yml:28` — `${NEXUS_ADMIN_PASSWORD:?...}`. |
| **B19** | K8s tag `:latest`. | `deployment.yaml:35` — `${NEXUSFLOW_VERSION:?...}`. |
| **B20** | Fallback silencioso no clone ADBC. | Scripts falham direto. |
| **B21** | README não indexava docs. | `README.md:31-43`. |
| **B22** | Tabela de env vars incompleta. | `GETTING_STARTED.md:127-142`. |
| **B26** | Rate limit retornava texto puro. | `rate_limit.rs:90-92` — JSON. |
| **B28** | Username arbitrário. | `auth_store.rs:8-22` — `validate_username`. |
| **B29** | `seed_admin_if_empty` com corrida. | `auth_store.rs:140-141` — single query. |
| **B30** | `.expect` na serialização de `Role`. | `auth_store.rs:135-138,175-178`. |
| **B35** | `ConnectorPalette` não acessível. | `ConnectorPalette.tsx:62-76`. |
| **B36** | `FieldHint` não acessível. | `FieldHint.tsx:55-64`. |
| **B37** | Polling não pausa em background. | `PipelineStatusBoard.tsx:73-78`. |
| **B38** | IDs de node globais. | `DagCanvas.tsx:42-46` — `crypto.randomUUID()`. |
| **B41** | Falta smoke test nos releases. | `release.yml:240-254` — `smoke-test`. |

---

## 7. Próximas Iniciativas

### 7.1 Store de Plugins / Conectores Enterprise

Ver documento dedicado: `docs/PLUGIN_STORE_PLAN.md`.

Resumo da recomendação:
- **Mecanismo de entrega:** feature flag `enterprise` + crate privado via `git` + binário/imagem Docker enterprise separada.
- **Serviço de pagamento:** `nexus-licensing` (Rust/Axum + Postgres) com checkout Mercado Pago Pro e webhook.
- **Primeira fase:** spike técnico no OSS com conector enterprise fake (não depende de Mercado Pago).
- **Primeiro conector real:** recomenda-se **Excel** (rápido, baixo risco) ou **Salesforce** (ticket enterprise).

Tarefas técnicas pendentes no OSS antes da store:
- Implementar gate de license em runtime (`validate_source_config`, `build_source`, `build_sink`, `preview`).
- Consumir campo `licensed` no frontend (`ConnectorPalette`, tela de licença).
- Corrigir divergências documentais sobre enterprise (`M29`, `B23`).

### 7.2 Priorização sugerida de backlog técnico

1. **A09** — decidir se publica imagem Docker no release ou corrige docs.
2. **M09** — `DeltaSink` não mascarar erros.
3. **A08** — revision ONNX fixada ou doc corrigida.
4. **M03** — SSRF via DNS (documentar ou resolver).
5. **M37** — isolamento de runners para PRs.
6. **B31** — índices no metadata store.
7. Itens baixos restantes (B01-B06, B12, B16, B24, B25, B27, B33, B34, B39, B40, B42).

---

## 8. Verificado e OK (amostra de cobertura)

- RBAC hierárquico (`Read < Execute < Write < Admin`) com middleware, JWT aud/iss/exp, blocklist de logout, Argon2, rate limit de login.
- Sanitização de credenciais em erros persistidos/alertados; respostas 500 genéricas.
- Criptografia AES-256-GCM de segredos de conector em repouso; summaries nunca expõem config.
- Identificadores SQL validados/quotados; validações de SSRF, path traversal e identificadores seguros.
- Ciclo de vida de runs: supervisora grava estado terminal, reaper de boot, leader election por advisory lock no Postgres, upserts idempotentes nos principais sinks.
- Frontend: JWT no subprotocolo WebSocket (não query string), `strict: true` no tsconfig, i18n pt/en com paridade de chaves, tratamento de erro em chamadas de API, confirmação em ações destrutivas.
- Dockerfile: roda como não-root, tem `HEALTHCHECK`, sem segredos com default.
- Cadeia CI → connectors-heavy → Release via `workflow_run` com checkout do `head_sha` correto.

---

## 9. Notas

- Recomenda-se manter este documento vivo: marcar itens como `[x]` à medida que forem resolvidos e abrir issues vinculadas aos IDs.
- A Store de Plugins deve ser tratada como uma iniciativa separada, com seu próprio documento de planejamento (`docs/PLUGIN_STORE_PLAN.md`).
