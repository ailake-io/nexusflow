# 🗺️ Roadmap — NexusFlow

Ordem por dependência técnica, não por prioridade de negócio isolada. Cada fase assume a anterior estável.

> Detalhamento de engenharia (arquivos/crates concretos, critério de "pronto" por marco): ver [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md).

## ⚠️ Pendências ativas (não esquecer)

Consolidado dos itens que ficaram faltando/incompletos ao longo das fases abaixo — checar aqui antes de assumir que algo já está pronto.

1. **Fase 12 — Enterprise connectors**: nada implementado ainda. Repo separado, mecanismo de license key (JWT), definir primeiro conector pago.
2. ~~**Marco 13 do `IMPLEMENTATION_PLAN.md` — CDC nativo sem Kafka/Debezium**~~ — resolvido: Fase 18 (`postgres-cdc`/`mongodb-cdc`/`mysql-cdc`), sinal de adoção confirmado.
3. **`nexus-ai`: features `cuda`/`metal` registram o execution provider ONNX Runtime correto (`ort::ep::CUDA`/`ort::ep::CoreML`), mas não validadas em hardware real** (sandbox é Linux sem GPU) — só confirmado que compilam e que o EP é registrado antes do load da sessão; runtime faz fallback silencioso pra CPU se o driver/hardware não estiver presente. `api` (embeddings via HTTP externa, endpoint compatível com OpenAI) implementada e testada (mock via `wiremock`) — sem chamada real contra OpenAI/Azure/etc neste sandbox. O perfil `cuda` do Docker já tem a infra de runtime pronta (base image + `--gpus all`).
4. **Alertas: Slack, MS Teams, PagerDuty, Email e Webhook genérico — todos os 5 canais de `CLAUDE.md §6` implementados** (ver `nexus-server/src/alerts.rs`).
5. **Windows (`.msi`/winget) e macOS (Homebrew/`.dmg`): specs escritos em `packaging/`, nunca validados em máquina real** (sandbox de dev é Linux). Falta também um build script dos drivers ADBC pra Windows (`.dll`) e macOS (`.dylib`) — os scripts atuais (`scripts/build-adbc-*.sh`) só geram `.so`.
6. **`.rpm` validado com `rpmbuild` real** — `scripts/package-rpm.sh` buildou `nexusflow-0.1.0-1.x86_64.rpm` de ponta a ponta; corrigido de brinde um `Requires:` incompleto (faltava `unixODBC`/`cyrus-sasl-lib`, equivalentes RPM do `unixodbc`/`libsasl2-2` que o `.deb` já lista — não pegos pelo scanner automático do rpmbuild porque são dlopen'd, não linkados direto no ELF).
7. **Estatísticas de hardware (CPU/RAM) implementadas** — `sysinfo` via `nexus-server::hardware_stats`, frame `{"hardware_stats": {...}}` intercalado no WebSocket de progresso a cada 2s (mesmo canal do `ProgressEvent`, discriminado pela chave). Sem GPU — `sysinfo` não expõe utilização de GPU (é vendor-specific, NVML pra NVIDIA etc.) e nada no código depende disso ainda.
8. **Imagens Docker: publicadas no GHCR a cada tag de release, multi-arch (amd64+arm64)** (`docker-publish` job em `.github/workflows/release.yml`, `ghcr.io/<owner>/<repo>` com tags semver via `GITHUB_TOKEN` — sem credencial externa). arm64 builda via QEMU emulado (`docker/setup-qemu-action`), não runner ARM nativo como o job `build` (tarballs) usa — cargo compila mais lento sob emulação, timeout do job em 180min pra acomodar; não validado ainda com uma tag `v*` real (só revisão de config).
9. **Admin (gestão de usuários) tem tela no Canvas** — `UsersPanel.tsx` cobre criar/promover/excluir contra as rotas já existentes (`GET/POST /users`, `GET/DELETE /users/{username}`, `PUT /users/{username}/role`). Nav item só aparece pra role Admin (decodificado do JWT client-side, sem verificar assinatura) — enforcement real continua 100% no servidor (`auth.rs`).
10. Ver também a seção **Débitos conhecidos** no fim deste arquivo (secrets sem KMS, RBAC sem escopo por recurso, versões de dependência pinadas, advisories RustSec aceitos).
11. ~~**Estágio `embedding` do `PipelineSpec` sem UI no Canvas**~~ — resolvido: node dedicado `kind: 'embedding'` no Canvas (mesmo padrão do node `dbt`, painel próprio em `NodeInspector.tsx` já que `EmbeddingModelSpec`/`ChunkingSpec` são unions com tag que o `SchemaForm` genérico não resolve). `lib/dag.ts`'s `PipelineSpec` agora declara `embedding`; `toPipelineSpec`/`fromPipelineSpec` fazem o round-trip completo (Onnx↔Api, fixed_window↔recursive_character) sem perder config ao editar/salvar.
12. **Fase 16 (Preview + dbt ETL) é backend-only** — `GET /pipelines/{id}/preview` não tem botão/tabela no Canvas ainda (só curl/Postman); o node dbt tem handle de saída (Fase 18 adicionou, consistência visual), mas painel de config pra `dbt.output` — hoje só configurável via API/JSON direto. Ambos deliberadamente adiados até validar se o formato backend-only já resolve o suficiente.
13. **Fase 18 (CDC nativo) sem toggle no Canvas** — `postgres-cdc`/`mongodb-cdc`/`mysql-cdc` aparecem no catálogo de conectores automaticamente (`GET /connectors` é dinâmico), mas hoje usar CDC significa escolher esse conector em vez do batch normal na lista — não tem um switch Batch/CDC dentro do mesmo node.

## Fase 0 — Fundação (workspace) ✅
- [x] `Cargo.toml` workspace + crates vazios: `nexus-core`, `nexus-ai`, `nexus-server`, e `crates/nexus-connectors/` já como workspace de sub-crates (não crate único) — ver `CLAUDE.md §3` e `ARCHITECTURE.md §3`
- [x] Traits base (`Source`, `Sink`, `Transform`) em `nexus-core`
- [x] `ConnectorRegistry` em `nexus-core` (registro de conectores, consumido por `nexus-server`)
- [x] `RecordBatchBuilder` genérico (adapter fallback)
- [x] `src/` como bootstrap fino (só sobe `nexus-server`, zero lógica de orquestração própria)
- [x] CI básico (fmt, clippy, test) por crate
- [x] Documentar decisão de escopo single-node (`ARCHITECTURE.md §6`) — não é item de código, é alinhamento antes de codar scheduler

## Fase 1 — MVP Fast-Path ✅
- [x] `nexus-connector-postgres` (ADBC real, source e sink), próprio crate — `nexus-connector-sqlite` acabou entrando na Fase 2 junto do transform
- [x] DAG parser: JSON estrito (2 nodes: source → sink, sem transform ainda)
- [x] Canal `mpsc` por partição (config de tamanho por pipeline, não fixo em 100) — ver `ARCHITECTURE.md §4`
- [x] Checkpoint por partição (`CheckpointCursor{partition_id, last_updated_at, offset}`) persistido em SQLite
- [x] Contrato de idempotência documentado e testado no `Sink` de referência (upsert, não insert puro) — ver `ARCHITECTURE.md §5`

## Fase 2 — Transformação leve (DataFusion) ✅
- [x] Node de transform via SQL em memória (`datafusion`)
- [x] Suporte a múltiplos sources → um transform → um sink no DAG (fan-in de N sources, fan-out pra M sinks) — mais `nexus-connector-sqlite` como segundo conector, provando o `ConnectorRegistry`

## Fase 3 — Conectores híbridos ✅
- [x] `nexus-connector-rest` genérico via `reqwest` + `RecordBatchBuilder`
- [x] `nexus-connector-mongodb` (bson → Arrow)
- [x] `nexus-connector-odbc` bridging (legado, feature `legacy`)
- [x] `nexus-connector-kafka` (base pra CDC da Fase 4, feature `consumer`)
- [x] `nexus-connector-csv` — source+sink de texto delimitado (CSV/TSV/TXT com separador configurável), `uri` local ou `s3://`/`gs://`/`az://` via `object_store` (feature `csv`)
- [x] Conector sink `webhook` (dentro de `nexus-connector-rest`, feature `rest`) — API/webhook genérico de saída, method configurável (POST/PUT/PATCH/DELETE), `body_mode` array ou per-row, sem consciência de CDC (API arbitrária não tem semântica acordada pra `__opcode`)

## Fase 4 — CDC (escopo faseado, ver `ARCHITECTURE.md §7`) ✅
- [x] CDC via Debezium + Kafka: consumir eventos já decodificados (JSON/Avro) através de `nexus-connector-kafka`, converter pra `RecordBatch` com opcode (I/U/D) via `RecordBatchBuilder` — continua suportado como alternativa (ver Fase 18)
- [x] Resume automático a partir do checkpoint por partição em falha
- [x] CDC nativo (Postgres/MongoDB/MySQL, sem Debezium/Kafka) — deixou de ser condicional, ver Fase 18

## Fase 5 — AI Lakehouse (`nexus-ai`) ✅ (GPU não validada em hardware real)
- [x] Chunking (fixed-size, recursive, semantic)
- [x] Embeddings via `ort` (feature `cpu`) — `cuda`/`metal` registram o execution provider certo (compilam, não validados em GPU/Apple Silicon real) — `api` (endpoint HTTP compatível com OpenAI, feature independente de `cpu`) implementada e testada com mock
- [x] Node `embedding` no Canvas (fora do plano original, ver pendência #11 acima) — troca de backend (Onnx local ↔ API externa) e de estratégia de chunking direto na UI, sem precisar editar JSON à mão
- [x] Sinks vetoriais: pgvector → Qdrant → LanceDB → Milvus → Pinecone → ChromaDB (nessa ordem, do mais simples de operar ao mais complexo)

## Fase 6 — Data Lake formats ✅
- [x] Sink Parquet puro
- [x] Delta Lake (`deltalake`)
- [x] Iceberg (`iceberg-rust`)
- [x] AI-Lake (`nexus-connector-ailake`, formato próprio Parquet+HNSW — não estava no escopo original desta fase, adicionado depois)

## Fase 7 — `nexus-server` (API + Auth) ✅
- [x] Axum REST: CRUD de pipelines, execução, status
- [x] JWT + RBAC (`Read`/`Execute`/`Write`/`Admin`)
- [x] Segredos AES-256-GCM em repouso
- [x] WebSocket: progresso de execução em tempo real

## Fase 8 — Frontend (React Flow) ✅
- [x] Canvas node-based: criar/editar DAG, source of truth em JSON
- [x] Painel de execução em tempo real (MB/s, linhas/s, logs)
- [x] Tela de credenciais (sem exibir segredo em plain text)
- [x] Admin: gestão de usuários (criar/promover/excluir), visível só pra role Admin

## Fase 9 — Observabilidade & Alertas ✅
- [x] `tracing` estruturado (JSON) + OpenTelemetry (traces via OTLP + métricas Prometheus em `/metrics`)
- [x] Alertas assíncronos: Slack (Block Kit), MS Teams (Adaptive Card), PagerDuty (Events API v2), Email (SMTP STARTTLS), Webhook genérico (JSON puro)
- [x] Estatísticas de hardware (CPU/RAM via `sysinfo`) intercaladas no WebSocket de progresso a cada 2s — sem GPU (vendor-specific, nada depende disso ainda)

## Fase 10 — dbt (ELT opcional) ✅
- [x] Subprocesso assíncrono invocando `dbt run`/`build`/`test` pós-carga (feature `dbt`), com resultado de lineage/qualidade no histórico de execução

## Fase 11 — Distribuição multiplataforma ✅ (Windows/macOS não validados)
- [x] Single binary com frontend embutido (`rust-embed`, feature `embed-ui`)
- [x] Empacotamento: AppImage/deb/rpm (Linux, todos testados) — `.msi`/winget (Windows) e Homebrew/dmg (macOS) têm specs em `packaging/` mas não foram validados em máquina real
- [x] Imagem Docker com perfil `cuda` selecionável via `--build-arg RUNTIME_IMAGE` (base image + `--gpus all` prontos; aceleração real pendente da Fase 5's `cuda` feature), publicada no GHCR a cada tag de release, multi-arch (amd64 nativo + arm64 via QEMU) — ver item 8 das Pendências ativas acima sobre o trade-off de build sob emulação
- [x] Script de instalação `curl | sh` (`scripts/install.sh`) + `.github/workflows/release.yml`

## Fase 12 — Enterprise connectors (paralelo, repo separado)
- [ ] Definir primeiro conector pago (candidato: Snowflake/Databricks avançado)
- [ ] Mecanismo de license key (JWT) validado em `nexus-server`
- [ ] Ver [`LICENSING.md`](./LICENSING.md)

## Fase 13 — Todos os conectores linkados no binário (fora do plano original)
- [x] Os 14 conectores que só existiam no workspace aninhado `crates/nexus-connectors` agora também são feature opcional em `nexus-server` (`connectors-all`), aparecendo de verdade no catálogo `GET /connectors` — antes só postgres/sqlite estavam linkados no binário servido pra UI.

## Fase 14 — Formulário de config por schema real (fora do plano original)
- [x] Cada Config struct de conector deriva `schemars::JsonSchema`; `GET /connectors` expõe esse schema (`config_schema`). Canvas renderiza um formulário real (`SchemaForm.tsx`, recursivo: texto/número/boolean/enum/array-de-objeto) em vez de pedir JSON escrito à mão — descrições vêm dos doc comments do Rust. Ver `ARCHITECTURE.md §3`.

## Fase 15 — Agendamento automático + gestão completa de pipelines no Canvas (fora do plano original)
- [x] `PipelineSpec.schedule` (cron 5 ou 6 campos) + scheduler em background no `nexus-server` (poll de 30s), reusando o mesmo caminho de execução do run manual (histórico/dbt/alertas idênticos). Validado end-to-end com servidor real rodando: disparo automático sem nenhuma chamada manual a `/run`. Ver `ARCHITECTURE.md §12`.
- [x] Canvas ganha **Save** (criar/atualizar), **Edit** (recarrega config completa de um pipeline salvo, incl. segredos de conector, via `GET /pipelines/{id}/spec` — role `Write`) e mantém **Delete** — antes só dava pra montar/rodar um pipeline no Canvas sem nunca conseguir persisti-lo.
- [x] Aba "Status": lista todos os pipelines salvos com flag verde/amarelo/vermelho/cinza (sucesso/em execução/falha/nunca rodou), baseado em `last_run_status`/`last_run_at` novos em `PipelineSummary`.

## Fase 16 — Preview de dados + dbt como ETL real (fora do plano original)
- [x] `GET /pipelines/{id}/preview?node={resolved_name}&limit={n}` — primeiras N linhas (default 50, teto 500) de qualquer node source/sink de um pipeline persistido, reusando `build_source`; conector sink-only devolve 400 com mensagem clara. Backend-only por ora, sem botão no Canvas. Ver `ARCHITECTURE.md §13`.
- [x] dbt deixa de ser só ELT: `DbtConfig.output` + `PipelineSpec.post_dbt_sinks` fecham o ciclo `Source → carga bruta → dbt transforma → lê resultado de volta → Sink final` num `run` só, sem precisar de um segundo pipeline manual. Testado com Postgres real (testcontainers) + `dbt-fusion` CLI real end-to-end (`crates/nexus-server/tests/dbt_etl_pipeline.rs`). Canvas: node dbt ganhou handle de saída (consistência visual); painel de config do destino (`dbt.output`) ainda não implementado — configuração via API/JSON só.

## Fase 18 — CDC nativo: Postgres, MongoDB e MySQL, sem Debezium/Kafka (fora do plano original)

O Marco 13 do `IMPLEMENTATION_PLAN.md` deixava CDC nativo condicional — só entraria se o overhead de operar Debezium+Kafka virasse bloqueador real de adoção confirmado. Esse sinal chegou (hardware mais simples não aguenta 3 JVMs rodando). Ver `ARCHITECTURE.md §7`.

- [x] `postgres-cdc` (feature `cdc` do `nexus-connector-postgres`, crate `pg_walstream`) — lê direto do protocolo de replicação lógica do Postgres (`pgoutput`). O replication slot é criado automaticamente no primeiro connect; a publicação (`CREATE PUBLICATION ... FOR TABLE ...`) precisa existir de antemão (não criada automaticamente, mesmo pré-requisito operacional que o Debezium já exige). Resume via o próprio slot — Postgres guarda o ponto server-side, sem precisar de checkpoint externo. Testado com Postgres real via testcontainers.
- [x] `mongodb-cdc` (mesmo crate `nexus-connector-mongodb`, sem dependência nova) — Change Streams nativo do driver oficial (`Collection::watch()`). Requer MongoDB como replica set (mesmo single-node serve). Testado com MongoDB real via testcontainers — inclusive um caso real onde `full_document: updateLookup` não retorna o documento atualizado (fallback pra `document_key`, linha não é descartada).
- [x] `mysql-cdc` (novo crate `nexus-connector-mysql`, dependência `mysql_cdc`) — lê o binlog direto, CDC-only (sem modo batch, mesmo padrão do Kafka). Colunas mapeadas **posicionalmente** (protocolo binlog não carrega nome de coluna por padrão), diferente de Postgres/MongoDB que casam por nome. Requer `binlog_format=ROW`/`binlog_row_image=FULL` e um usuário com `REPLICATION SLAVE`/`REPLICATION CLIENT`. Testado com MySQL real via testcontainers.
- [x] `nexus-connector-kafka` continua existindo — deixa de ser o único caminho de CDC, mas segue disponível como fonte genérica de Kafka e como caminho alternativo via Debezium pra quem já opera essa infra.
- [ ] Canvas: os 3 novos conectores aparecem no catálogo dinâmico (`GET /connectors`) sem mudança de frontend, mas ainda não tem um toggle Batch/CDC no mesmo node — hoje é escolher um conector diferente na lista.

---

**Critério de "MVP pronto"**: Fases 0–3 + 7 (parcial: auth básica) + 8 (canvas mínimo) funcionando end-to-end — mover dados de Postgres pra Postgres via canvas visual, com checkpoint por partição, retry e escrita idempotente. **Atingido e superado** — Fases 0–11 e 13–16 completas, só falta Fase 12 (enterprise, repo separado) e os itens condicionais/parciais marcados acima.

## Débitos conhecidos (aceitos pro MVP, resolver antes de vender enterprise)

- **Secrets via env var, sem KMS/rotação** — ok pra self-host single-tenant; precisa migrar pra KMS (AWS/GCP/Vault) antes do primeiro cliente enterprise (`ARCHITECTURE.md §10`).
- **RBAC sem escopo por recurso** — 4 papéis globais chega pro MVP; SaaS multi-tenant vai exigir permissão por pipeline/credencial.
- ~~**Ciclo de vida do modelo ONNX indefinido**~~ — decidido 2026-07-30: HF Hub em runtime + cache local (`ARCHITECTURE.md §8`).
- **Execução single-node** — decisão deliberada de escopo, não limitação a esconder do usuário (`ARCHITECTURE.md §6`). Documentar isso claramente também no README quando o produto for anunciado publicamente.
- **5 advisories RustSec aceitos (ver `.github/workflows/ci.yml`'s `cargo-audit` job)**: `RUSTSEC-2023-0071` (rsa, via `jsonwebtoken`'s RS256 — sem correção disponível upstream), `RUSTSEC-2026-0194`/`-0195` (quick-xml, via `object_store`/`datafusion` 54.1.0 — mesmo pin de arrow 58.x abaixo), `RUSTSEC-2025-0009`/`RUSTSEC-2024-0336` (ring/rustls, via `milvus-sdk-rust`'s tonic 0.8.3 — sem release mais nova do SDK). Reavaliar cada um quando a dependência que os carrega soltar uma versão nova.
- **`arrow-array`/`arrow-schema` fixados em `58.4.0` e `adbc_core`/`adbc_driver_manager`/`adbc_ffi` em `0.23.0` (não a última, `0.24.0`) em todo o workspace** — `datafusion` 54.1.0 (última versão publicada) ainda depende de arrow 58.x, enquanto adbc 0.24.0 já exige arrow ≥59. Sem overlap entre as duas, então fixamos tudo em 58.4.0/0.23.0 pra ter um `RecordBatch` só no grafo de dependências. Reavaliar quando o datafusion soltar uma versão em cima de arrow 59+.
