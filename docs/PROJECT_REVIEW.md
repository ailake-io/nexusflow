# Revisão Geral do Projeto NexusFlow

Este documento é o backlog técnico único do projeto. Ele consolida:

- O plano de revisão anterior (`docs/REVIEW_ACTION_PLAN.md`), absorvendo os itens ainda abertos.
- A auditoria atual do backend Rust, frontend, documentação e infraestrutura (CI/CD, Dockerfile, scripts, packaging).
- Bugs reais encontrados em execução (ex.: `release.yml` falhando no step de AppImage).

> **Escopo da auditoria:** branch `release-test`, commit atual. Os achados foram verificados no código-fonte; linhas de referência (`path:linha`) se referem a esse ponto do histórico.

---

## 1. Resumo Executivo

| Severidade | Quantidade | Risco resumido |
|---|---|---|
| Crítico | 4 | Perda silenciosa de dados, runs órfãs eternas, boot impossível em deploy documentado, release totalmente bloqueado. |
| Alto | 10 | Exemplos de documentação quebram em runtime, duplicatas em retry, corrupção de NULLs/tipos, vazamento de container no CI, overlap de runs concorrentes. |
| Moderado | 22 | Gargalos de performance (leitura de 10k runs a cada 30s, ONNX síncrono), SSRF via DNS, falta de i18n em erros, testes de frontend não rodando no CI. |
| Baixo | 16 | Polimento de docs, acessibilidade, placeholders hardcoded, contradições menores. |

**Conclusão imediata:** não há condições de mergear `release-test` em `main` enquanto o step de AppImage falhar (bloqueia toda a cadeia de release) e enquanto `NEXUS_AUTH_DB=postgres://` quebrar o boot (impossibilita o deploy multi-réplica documentado).

---

## 2. Achados Críticos

| ID | Problema | Evidência | Impacto | Ação recomendada |
|---|---|---|---|---|
| C01 | **Fan-out perde batches silenciosamente** quando um sink fica lento. O canal `broadcast` sinaliza `Lagged`, mas `write_one_sink_stream` trata como `Closed`, commita checkpoint e retorna `Ok`. | `crates/nexus-core/src/pipeline.rs:286-312`, `:366`, `:386` | Perda de dados silenciosa em sinks lentos; run marcada como sucesso. | Tratar `Lagged(n)` como erro fatal do sink, nunca como fim de stream. |
| C02 | **Fontes CDC nativas nunca terminam** no caminho de execução atual. `drain_sources` lê até EOF; streams CDC (`postgres-cdc`, `mongodb-cdc`, `mysql-cdc`) só terminam em erro. | `crates/nexus-server/src/runner.rs:287`; `crates/nexus-core/src/pipeline.rs:229-244`; conectores `*-cdc` | Run fica `running` para sempre, memória cresce sem bound, scheduler passa a pular a pipeline. | Criar caminho streaming/micro-batch para fontes sem EOF, ou rejeitar conectores `*-cdc` na validação até existir. |
| C03 | **`LicenseStore` é SQLite-only** mas recebe `NEXUS_AUTH_DB`, que a documentação ensina a apontar para Postgres. Boot falha. | `crates/nexus-server/src/license_store.rs:23-25`; `crates/nexus-server/src/lib.rs:1182`; `docs/GETTING_STARTED.md:132,141-149` | Deploy multi-réplica documentado não sobe. | Portar `LicenseStore` para `MetadataPool` como os demais stores. |
| C04 | **Step de AppImage sempre falha**, derrubando todo o job `build` do release. `APPIMAGETOOL` recebe valor com espaço e flag; `command -v "$APPIMAGETOOL"` falha. | `scripts/package-appimage.sh:24,38`; `.github/workflows/release.yml:188,194` | Nenhuma release é publicada; artefatos .deb/.rpm também se perdem. | Separar binário e flags no script (ex.: `APPIMAGETOOL_BIN` + `--appimage-extract-and-run` explícito). |

---

## 3. Achados de Alto Impacto

| ID | Problema | Evidência | Impacto | Ação recomendada |
|---|---|---|---|---|
| A01 | **`IcebergSink` é append-only**; retry duplica linhas. | `nexus-connector-iceberg/src/sink.rs:166-182`; comentário em `:21-28` | Viola contrato de idempotência do engine; retry silencioso corrompe tabela. | Implementar dedup por PK antes do append, ou marcar como não-idempotente e impedir resume. |
| A02 | **Overlap de run manual com run agendada** da mesma pipeline. `run_pipeline_handler` não verifica runs em andamento. | `crates/nexus-server/src/lib.rs:417-454`; `crates/nexus-server/src/scheduler.rs:131-133` | Sinks read-modify-write (CSV, Parquet, Delta) perdem escritas concorrentes. | Rejeitar/enfileirar em `start_pipeline_run` quando já houver run `running` para o `pipeline_id`. |
| A03 | **Embedding corrompe NULLs** e suporta poucos tipos. `replicate_array` não checa `is_null`; tipos fora de Utf8/Int64/Float64/Boolean/Float32 falham a run. | `crates/nexus-ai/src/embedding/pipeline.rs:236-296` | Dados nulos viram `0`/`""`; colunas comuns (`int4`, `timestamptz`) quebram o pipeline. | Preservar `append_null` e ampliar cobertura de tipos (ou cast explícito com mensagem clara). |
| A04 | **`new_empty_array` dá `panic!`** em tipo não suportado. Como roda dentro da task supervisora, a run fica `running` eternamente sem erro. | `crates/nexus-ai/src/embedding/pipeline.rs:318`; `crates/nexus-server/src/lib.rs:480-483` | Run órfã sem mensagem de erro. | Retornar `Err(EmbeddingError::Arrow(...))`; adicionar guarda contra panic na supervisora. |
| A05 | **Exemplos de API do `GETTING_STARTED` quebram**: omitem `primary_key`, usam `path` em vez de `uri`, referenciam tabela inexistente `events`. | `docs/GETTING_STARTED.md:193-210`; configs em `nexus-connector-postgres/src/config.rs:16` e `nexus-connector-sqlite/src/config.rs:15` | Primeiro pipeline via API não funciona copiando e colando. | Reescrever exemplos alinhados ao `docs/USER_GUIDE.md:100-107`. |
| A06 | **Exemplo de embedding do `GETTING_STARTED` quebra**: falta tag `"backend": "onnx"`, falta `uri`/`primary_key` na source, coluna `chunk_text` não existe. | `docs/GETTING_STARTED.md:96-115`; `crates/nexus-core/src/dag.rs:99-111`; `crates/nexus-ai/src/embedding/pipeline.rs:201` | Exemplo de feature central do produto falha. | Reescrever exemplo alinhado a `docs/USER_GUIDE.md` §6. |
| A07 | **`NEXUS_ALLOW_INTERNAL_HOSTS` não está documentado**; default `false` rejeita `localhost`/IP de LAN sem explicação. | `crates/nexus-server/src/lib.rs:1329-1330`; `crates/nexus-core/src/dag.rs:564-572,598-665` | Primeiro teste local recebe 400 sem saber como opt-out. | Adicionar variável à tabela de env vars do `GETTING_STARTED.md`. |
| A08 | **Revisão do modelo ONNX não é configurável**, contradiz `ARCHITECTURE.md` que promete revision fixada. | `ARCHITECTURE.md:117`; `crates/nexus-core/src/dag.rs:100-111`; `crates/nexus-ai/src/embedding/pipeline.rs:59` | Reprodutibilidade quebrada; modelo pode mudar silenciosamente. | Adicionar campo `revision` ao `EmbeddingModelSpec` ou corrigir a doc. |
| A09 | **Imagem Docker publicada não tem `connectors-all`**, contradizindo guias do usuário. | `docs/USER_GUIDE.md:35`; `docs/GETTING_STARTED.md:78`; `.github/workflows/release.yml:271-279`; `Dockerfile:22` | Imagem `:full`/`:latest` não lista todos os conectores. | Publicar imagem com `FEATURES=embed-ui,connectors-all` ou corrigir docs. |
| A10 | **Job `docker-image` do CI vaza container** quando o healthcheck falha, travando runs seguintes no runner compartilhado. | `.github/workflows/ci.yml:156-170` | Falha permanente até intervenção manual no self-hosted runner. | Adicionar `docker rm -f nexusflow-ci` antes do run ou `trap ... EXIT`. |

---

## 4. Achados Moderados

### Backend

| ID | Problema | Evidência | Ação recomendada |
|---|---|---|---|
| M01 | Ordenação por `started_at` (segundos) torna "último run" ambíguo; guarda anti-overlap pode furar. | `crates/nexus-server/src/pipeline_store.rs:292-296,358-364,540-544`; `crates/nexus-server/src/scheduler.rs:130` | Ordenar por `id DESC` (monotônico). |
| M02 | Scheduler lê até 10.000 runs por pipeline agendada a cada 30s. | `crates/nexus-server/src/scheduler.rs:98,125-128`; `crates/nexus-server/src/pipeline_store.rs:576-587` | Query dedicada `LIMIT 1` do último run. |
| M03 | SSRF: `is_internal_host` não resolve DNS; domínio público para IP privado passa. | `crates/nexus-core/src/dag.rs:598-665` | Validar IP resolvido na conexão ou documentar limitação. |
| M04 | `WebhookSink` segue redirects, ao contrário do `RestSource`. | `nexus-connector-rest/src/sink.rs:23-26`; `nexus-connector-rest/src/source.rs:29-31` | Desabilitar redirects no sink. |
| M05 | `parquet` não está na lista de conectores de path local. | `crates/nexus-core/src/dag.rs:530-535`; `nexus-connector-parquet/src/config.rs:12` | Incluir `"parquet"` em `is_local_path_connector`. |
| M06 | Migração SQLite→Postgres não cobre `pipeline_run_logs` nem `license`. | `crates/nexus-server/src/migrate.rs:22-69`; `crates/nexus-server/src/run_log_store.rs:28-43`; `crates/nexus-server/src/license_store.rs:29-34` | Incluir as duas tabelas no `MigrationSummary`. |
| M07 | Inferência ONNX roda síncrona no executor async, bloqueando o worker tokio. | `crates/nexus-ai/src/embedding/inference.rs:94-189`; `crates/nexus-ai/src/embedding/pipeline.rs:207` | Rodar em `tokio::task::spawn_blocking`. |
| M08 | Opcode CDC desconhecido vira upsert silencioso. | `crates/nexus-core/src/cdc.rs:62-67` | Validar com `Opcode::from_letter` e rejeitar valores inválidos. |
| M09 | `DeltaSink::open()` mascara qualquer erro como "tabela não existe". | `nexus-connector-deltalake/src/sink.rs:36-42` | Distinguir `NotFound` de outros erros. |
| M10 | Colisão de nome de arquivo do `IcebergSink` entre runs. | `nexus-connector-iceberg/src/sink.rs:36`; `nexus-connector-iceberg/src/sink.rs:30-36` | Usar UUID/timestamp por chamada. |
| M11 | Re-mapeamento posicional no Iceberg pode trocar colunas de mesmo tipo. | `nexus-connector-iceberg/src/sink.rs:111-119` | Reordenar colunas do batch pelo nome antes de escrever. |
| M12 | `ProgressHub` usa `std::sync::Mutex` em async. | `crates/nexus-server/src/progress.rs:14-37` | Migrar para `tokio::sync::Mutex`. |
| M13 | `alerts.rs` fire-and-forget sem observar falhas. | `crates/nexus-server/src/alerts.rs:35-46` | Logar falhas; aguardar `JoinHandle` em testes. |
| M14 | `pipeline_id` sem restrição de caracteres/comprimento. | `crates/nexus-core/src/dag.rs:162-164` | Validar `[A-Za-z0-9_-]{1,128}`. |
| M15 | `NodeSpec.name` não validado como identificador SQL seguro. | `crates/nexus-core/src/dag.rs:161-201` | Aplicar `validate_identifier`. |
| M16 | `sanitize_error` não remove credenciais em query strings. | `crates/nexus-server/src/error.rs:80-113` | Usar `url::Url` para sanitizar query params. |

### Frontend

| ID | Problema | Evidência | Ação recomendada |
|---|---|---|---|
| M17 | Execução pode ficar travada em `running` na UI sem retry visível. | `frontend/src/hooks/useRunProgress.ts:110-115`; `frontend/src/components/DagCanvas.tsx:251` | Exibir erro e reabilitar o botão Run após timeout. |
| M18 | Mensagens de erro ignoram i18n (sempre em inglês). | `frontend/src/hooks/useRunProgress.ts:63,159`; `frontend/src/lib/dag.ts:171-310` | Usar chaves de tradução em todos os erros visíveis. |
| M19 | Testes do frontend existem mas não rodam no CI. | `.github/workflows/ci.yml:196-204`; `frontend/package.json:12` | Adicionar `- run: npm test` ao job `frontend`. |
| M20 | Cobertura mínima: `dag.ts` (461 linhas) não tem testes. | `frontend/src/lib/dag.ts` | Priorizar testes de round-trip `toPipelineSpec`/`fromPipelineSpec`. |
| M21 | Labels do formulário de usuário e textarea do IoPanel sem associação acessível. | `frontend/src/components/UsersPanel.tsx:129-155`; `frontend/src/components/PipelineIoPanel.tsx:198-205` | Adicionar pares `htmlFor`/`id` ou `aria-label`. |
| M22 | `radix-ui` inteiro como dependência. | `frontend/package.json:20` | Trocar por pacotes individuais. |
| M23 | `shadcn` em `dependencies`. | `frontend/package.json:23` | Mover para `devDependencies` ou remover. |
| M24 | Zero testes de comportamento (só `utils.test.ts`). | `frontend/src/lib/utils.test.ts` (único) | Adicionar testes para hooks e componentes principais. |

### Documentação

| ID | Problema | Evidência | Ação recomendada |
|---|---|---|---|
| M25 | "18 conectores" está desatualizado; catálogo real tem 24 entradas com `connectors-all`. | `README.md:5`; `docs/GETTING_STARTED.md:42,80`; `docs/USER_GUIDE.md:3,35,274`; `crates/nexus-server/Cargo.toml:51-56` | Atualizar contagem ou explicitar "18 batch + 6 CDC". |
| M26 | 6 conectores CDC não têm referência de config no `USER_GUIDE`. | `docs/USER_GUIDE.md` §4; configs em `nexus-connector-postgres/src/config.rs:36-58`, etc. | Adicionar seção §4.9 com config de cada CDC. |
| M27 | Features `embeddings`/`embeddings-api`/`*-cdc` não são forwardadas pelo crate raiz. | `docs/GETTING_STARTED.md:80-88`; `Cargo.toml` raiz:20-47 | Adicionar forwards ou documentar a limitação. |
| M28 | Chunking "semantic" documentado mas não selecionável no DAG. | `CLAUDE.md:127`; `ARCHITECTURE.md:113`; `ROADMAP.md:63`; `crates/nexus-ai/src/chunking.rs:155`; `crates/nexus-core/src/dag.rs:127-139` | Adicionar variante ao `ChunkingSpec` ou marcar como biblioteca-only. |
| M29 | `ENTERPRISE_LICENSING.md` com rota, método e criptografia errados. | `docs/ENTERPRISE_LICENSING.md:3,63-64`; `crates/nexus-server/src/lib.rs:204-207`; `crates/nexus-server/src/license_store.rs:5-6` | Corrigir rota para `/license`, remover menção a `is_connector_licensed` como feito, alinhar storage. |
| M30 | Docker Hub ainda descrito como publicação ativa em `CLAUDE.md`/`ROADMAP`. | `CLAUDE.md:163`; `ROADMAP.md:18,96`; `.github/workflows/release.yml:255-256` | Atualizar para "GHCR apenas; Docker Hub desabilitado temporariamente". |
| M31 | `install.sh` anuncia macOS/arm64 sem assets correspondentes no release. | `docs/GETTING_STARTED.md:36`; `scripts/install.sh:2,33-42`; `.github/workflows/release.yml:86-107` | Restringir script a Linux-x86_64-only por ora. |

### Infraestrutura

| ID | Problema | Evidência | Ação recomendada |
|---|---|---|---|
| M32 | Drivers ADBC compilados de `main` sem pin nem checksum. | `scripts/build-adbc-postgresql-driver.sh:32`; `scripts/build-adbc-sqlite-driver.sh:30`; `.github/workflows/ci.yml:241-242` | Pinar tag/commit do `arrow-adbc` e incluir ref na cache key. |
| M33 | `install.sh` degrada para "sem verificação" silenciosamente e ignora assinaturas GPG. | `scripts/install.sh:74-76` | Falhar fechado quando checksum não puder ser verificado; verificar `.asc` se `gpg` disponível. |
| M34 | `release.yml` baixa `appimagetool` de release móvel `continuous` sem verificação. | `.github/workflows/release.yml:185-187` | Pinar tag e verificar SHA256. |
| M35 | CI não roda em pull requests. | `.github/workflows/ci.yml:3-6` | Adicionar `pull_request:` aos triggers. |
| M36 | Pacotes .deb/.rpm instalam servidor sem unit systemd, usuário ou pós-inst. | `scripts/package-deb.sh:29-61`; `scripts/package-rpm.sh:50-66` | Incluir unit systemd + postinst ou remover `.desktop`. |
| M37 | Runners self-hosted sem isolamento para PRs. | `.github/workflows/ci.yml` | Usar GitHub-hosted para PRs ou isolar. |
| M38 | Tags flutuantes de imagens base no Dockerfile. | `Dockerfile:17,26,37,46,80` | Pin por digest SHA-256. |

---

## 5. Achados de Baixo Impacto

| ID | Problema | Evidência | Ação recomendada |
|---|---|---|---|
| B01 | Race check-then-insert em `PipelineStore::create`. | `crates/nexus-server/src/pipeline_store.rs:191-228`; `crates/nexus-server/src/lib.rs:101` | Capturar violação UNIQUE e retornar 409. |
| B02 | Upsert SQL com `SET` vazio quando a única coluna é a PK. | `nexus-connector-postgres/src/sink.rs:70-83`; `nexus-connector-sqlite/src/sink.rs:62-75` | Usar `DO NOTHING` ou rejeitar na validação. |
| B03 | Overflow no backoff de retry (`2u32.pow(attempt)`). | `nexus-connector-rest/src/source.rs:140`; `nexus-connector-rest/src/sink.rs:66` | Cap em `retries`; usar `saturating_pow`/backoff limitado. |
| B04 | Porta 8080 hardcoded. | `crates/nexus-server/src/lib.rs:1378` | Ler `NEXUS_PORT` com default 8080. |
| B05 | Audit log de login nunca registra IP. | `crates/nexus-server/src/auth_store.rs:56`; `crates/nexus-server/src/lib.rs:362-365` | Extrair IP via `ConnectInfo` e gravar. |
| B06 | Rate limiter de login usa IP do peer direto (problema atrás de proxy). | `crates/nexus-server/src/rate_limit.rs:84-88` | Documentar/validar config de proxy confiável. |
| B07 | Fallback "Loading…" hardcoded em inglês. | `frontend/src/App.tsx:28` | Usar chave i18n. |
| B08 | Tooltip de máscara de credencial hardcoded em inglês. | `frontend/src/components/PipelinesList.tsx:41` | Extrair para `translations.ts`. |
| B09 | Placeholder de cron só em português. | `frontend/src/components/PipelineIoPanel.tsx:144` | Usar chave i18n ou placeholder neutro. |
| B10 | Tipo de `runPipeline` mais fraco que contrato do servidor. | `frontend/src/lib/api.ts:161-170` | Tipar como `PipelineSpec`. |
| B11 | Destaque do headline do login frágil. | `frontend/src/components/LoginForm.tsx:47-55` | Dividir chave em duas partes. |
| B12 | Mapeamento `idle` morto e lógica duplicada de status. | `frontend/src/components/ExecutionPanel.tsx:35`; `frontend/src/components/PipelineStatusBoard.tsx:78-79,131-138` | Remover caso morto; extrair helper. |
| B13 | Manifesto winget referencia MSI que não é mais produzido. | `packaging/windows/winget/manifests/n/nexusflow/nexusflow/0.1.0/nexusflow.nexusflow.installer.yaml:11-12` | Remover manifesto até MSI voltar. |
| B14 | Fórmula Homebrew com SHA256 placeholder. | `packaging/macos/nexusflow.rb:18-21` | Atualizar no release ou remover até haver macOS no escopo. |
| B15 | Header do `release.yml` contradiz build Docker arm64 via QEMU. | `.github/workflows/release.yml:14-21` vs `:274` | Atualizar comentário. |
| B16 | `actions/upload-artifact@v4` sem pin de SHA. | `.github/workflows/release.yml:165,197` | Pinar SHA como as demais actions. |
| B17 | Comentários do job `cargo-audit` contradizem código. | `.github/workflows/ci.yml:33-36,39-43` | Reescrever comentários. |
| B18 | Stack Swarm permite senha de admin vazia. | `packaging/swarm/docker-stack.yml:28` | Usar `${NEXUS_ADMIN_PASSWORD:?...}`. |
| B19 | Manifestos Kubernetes fixam tag mutável `:latest`. | `packaging/kubernetes/deployment.yaml:35` | Parametrizar tag. |
| B20 | Fallback de `git clone` do ADBC mascara falhas de rede. | `scripts/build-adbc-postgresql-driver.sh:39-40` | Só fazer fallback em "ref not found" ou falhar direto. |
| B21 | README não indexa 4 docs existentes. | `README.md:31-40` | Adicionar `USER_GUIDE.md`, `ENTERPRISE_*.md`, `PROJECT_REVIEW.md`. |
| B22 | Tabela de env vars do `GETTING_STARTED` incompleta. | `docs/GETTING_STARTED.md:127-137` | Incluir variáveis de alertas, `NEXUS_ALLOW_INTERNAL_HOSTS`, etc. |
| B23 | Mecanismo de conector enterprise descrito de formas contraditórias. | `CLAUDE.md:59`; `ARCHITECTURE.md:134`; `LICENSING.md:28` | Alinhar texto. |
| B24 | USER_GUIDE contradiz-se sobre lancedb pré-existente vs criado automaticamente. | `docs/USER_GUIDE.md:235,381,497` | Explicitar exceção do lancedb. |
| B25 | ROADMAP cita "14 conectores" desatualizado. | `ROADMAP.md:105` | Atualizar ou remover número. |
| B26 | `LoginRateLimiter` responde 429 com texto puro. | `crates/nexus-server/src/rate_limit.rs:61-66` | Retornar JSON. |
| B27 | `embedded_ui.rs` sem cache headers. | `crates/nexus-server/src/embedded_ui.rs:17-27` | Adicionar `cache-control`. |
| B28 | `auth_store.rs` permite username arbitrário. | `crates/nexus-server/src/auth_store.rs:70-93` | Validar formato. |
| B29 | `seed_admin_if_empty` com condição de corrida. | `crates/nexus-server/src/auth_store.rs:55-68` | Usar transação ou `INSERT OR IGNORE`. |
| B30 | `auth_store.rs` usa `.expect` na serialização de `Role`. | `crates/nexus-server/src/auth_store.rs:82-84,141-144` | Propagar erro. |
| B31 | `pipeline_store.rs` não cria índices. | `crates/nexus-server/src/pipeline_store.rs:94-122` | Adicionar `CREATE INDEX`. |
| B32 | `pipeline_store.rs` `encode_spec` usa `.expect`. | `crates/nexus-server/src/pipeline_store.rs:393` | Propagar erro. |
| B33 | `telemetry.rs` `try_init()` não é idempotente. | `crates/nexus-server/src/telemetry.rs:34-91` | Tornar idempotente. |
| B34 | `nexus-ai` não valida assinatura/checksum do modelo. | `crates/nexus-ai/src/embedding/model.rs:30-40` | Documentar risco; planejar pin de hash. |
| B35 | `ConnectorPalette` não acessível por teclado. | `frontend/src/components/ConnectorPalette.tsx` | Adicionar `role`, `tabIndex`, handlers. |
| B36 | `FieldHint` não acessível. | `frontend/src/components/FieldHint.tsx` | `aria-describedby`, fechar com Escape/clique fora. |
| B37 | Polling do status board não pausa em background. | `frontend/src/components/PipelineStatusBoard.tsx:52-55` | Usar `document.visibilityState`. |
| B38 | IDs de node globais e mutáveis. | `frontend/src/components/DagCanvas.tsx:39`; `frontend/src/lib/dag.ts:191` | Usar `crypto.randomUUID()` ou contador no componente. |
| B39 | `handleImport` não valida JSON. | `frontend/src/components/DagCanvas.tsx:188-191` | Validar schema mínimo. |
| B40 | `index.html` lang="en" fixo. | `frontend/index.html:2` | Sincronizar com idioma selecionado. |
| B41 | Falta smoke test nos releases. | `.github/workflows/release.yml` | Extrair tarball e rodar `--version`. |
| B42 | Build de Windows/macOS inacabado. | `packaging/windows/`, `packaging/macos/` | Validar e finalizar scripts. |

---

## 6. Documentação vs Código — Divergências Resolvidas Recentemente

As divergências abaixo foram verificadas como **corrigidas** na branch atual e permanecem aqui apenas para auditoria:

- Stack: React + Vite (antes: "Next.js ou Vite").
- Matriz de conectores em `CLAUDE.md` atualizada; conectores não implementados (MySQL batch, DuckDB, Snowflake, BigQuery, ClickHouse ADBC) marcados explicitamente.
- Arrow Flight SQL marcado como aspiracional.
- Alertas: Slack, MS Teams, PagerDuty, Email (SMTP STARTTLS) e Webhook genérico implementados.
- Stats de hardware no WebSocket implementados.
- CDC nativo (`postgres-cdc`, `mongodb-cdc`, `mysql-cdc`) implementado; Debezium+Kafka removido.
- Features `api`/`cuda`/`metal` registradas e compiláveis.
- Exemplo cross-connector do `GETTING_STARTED` inclui nó transform; regra de path absoluto isenta sinks locais por design.
- README atualizado com nota de validação Linux e lista real de conectores.
- RBAC por rota, criptografia AES-256-GCM, sanitização de erros, validações de SSRF/path/identificadores SQL, e ciclo de vida de runs com reaper/leader election verificados como OK.

---

## 7. Limpeza de Documentos Obsoletos

| Arquivo | Status | Motivo |
|---|---|---|
| `docs/REVIEW_ACTION_PLAN.md` | **Deletado** | Itens abertos foram absorvidos por este documento; itens resolvidos viraram ruído. |
| `IMPLEMENTATION_PLAN.md` | **Mantido como histórico** | Marcos 0–11 e 13 já atingidos; Marco 12 (enterprise) coberto por `ROADMAP.md` Fase 12 + `docs/ENTERPRISE_*.md`. Permanece porque ainda é referenciado em dezenas de comentários de código como origem de decisões. |

Limpeza feita:
- `docs/REVIEW_ACTION_PLAN.md` removido; todo backlog ativo migrado para cá.
- Referências em `README.md`, `ROADMAP.md`, `docs/GETTING_STARTED.md`, `ARCHITECTURE.md` e `crates/nexus-connectors/README.md` atualizadas para não apontarem mais para `docs/REVIEW_ACTION_PLAN.md` nem promover `IMPLEMENTATION_PLAN.md` como documento ativo.
- Comentários de código que citavam `docs/REVIEW_ACTION_PLAN.md` atualizados para `docs/PROJECT_REVIEW.md`.

---

## 8. Proposta de Execução por Fases

### Fase 1 — Desbloqueio do Release e do Deploy (1–2 dias)
1. Corrigir `scripts/package-appimage.sh` (C04).
2. Portar `LicenseStore` para `MetadataPool` (C03) ou, como mitigação urgente, documentar/falhar claramente que `NEXUS_AUTH_DB` só aceita SQLite.
3. Corrigir `docker-image` CI para nunca vazar container (A10).

### Fase 2 — Confiabilidade de Dados (semana 1)
1. Fan-out: tratar `Lagged` como erro fatal (C01).
2. CDC: rejeitar conectores `*-cdc` na validação até haver caminho streaming, ou implementar micro-batch com commit de cursor (C02).
3. Iceberg: dedup por PK ou bloquear resume parcial (A01).
4. Overlap de runs: rejeitar/enfileirar run manual sobre run em andamento (A02).

### Fase 3 — Embeddings e Frontend (semana 2)
1. Embedding: preservar NULLs, ampliar tipos, remover panic (A03, A04).
2. Frontend: i18n de erros, testes no CI, testes para `dag.ts`, labels acessíveis (M17–M21).

### Fase 4 — Documentação e Infra (semana 3)
1. Corrigir exemplos do `GETTING_STARTED` (A05, A06, A07, A08, A09).
2. Atualizar contagem de conectores, documentar CDC e `NEXUS_ALLOW_INTERNAL_HOSTS` (M25–M31).
3. Pin de ADBC, AppImage, Dockerfile; .deb/.rpm com systemd; CI em PRs (M32–M38).

### Fase 5 — Dívida Técnica e Plataformas (semana 4+)
1. Itens moderados restantes (M01–M16, M22–M24).
2. Itens baixos por área (B01–B42).
3. Windows/macOS packaging finalizado (B42).

---

## 9. Verificado e OK (amostra de cobertura)

- RBAC hierárquico (`Read < Execute < Write < Admin`) com middleware, JWT aud/iss/exp, blocklist de logout, Argon2, rate limit de login.
- Sanitização de credenciais em erros persistidos/alertados; respostas 500 genéricas.
- Criptografia AES-256-GCM de segredos de conector em repouso; summaries nunca expõem config.
- Identificadores SQL validados/quotados; validações de SSRF, path traversal e identificadores seguros.
- Ciclo de vida de runs: supervisora grava estado terminal, reaper de boot, leader election por advisory lock no Postgres, upserts idempotentes nos principais sinks.
- Frontend: JWT no subprotocolo WebSocket (não query string), `strict: true` no tsconfig, i18n pt/en com paridade de chaves, tratamento de erro em chamadas de API, confirmação em ações destrutivas.
- Dockerfile: roda como não-root, tem `HEALTHCHECK`, sem segredos com default, `.dockerignore` adequado.
- Pinning de actions por SHA (exceto `upload-artifact@v4`, listado em B16).
- Cadeia CI → connectors-heavy → Release via `workflow_run` com checkout do `head_sha` correto.

---

## 10. Notas

- Os achados críticos C01, C02, C03 e C04 devem ser considerados **bloqueadores de release**.
- Muitos itens baixos podem ser paralelizados e convertidos em bons primeiros issues para contribuidores.
- Recomenda-se manter este documento vivo: marcar itens como `[x]` à medida que forem resolvidos e abrir issues vinculadas aos IDs.
