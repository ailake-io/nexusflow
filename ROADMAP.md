# 🗺️ Roadmap — NexusFlow

Ordem por dependência técnica, não por prioridade de negócio isolada. Cada fase assume a anterior estável.

> Detalhamento de engenharia (arquivos/crates concretos, critério de "pronto" por marco): ver [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md).

## ⚠️ Pendências ativas (não esquecer)

Consolidado dos itens que ficaram faltando/incompletos ao longo das fases abaixo — checar aqui antes de assumir que algo já está pronto.

1. **Fase 12 — Enterprise connectors**: nada implementado ainda. Repo separado, mecanismo de license key (JWT), definir primeiro conector pago.
2. **Marco 13 do `IMPLEMENTATION_PLAN.md` — CDC nativo sem Kafka/Debezium**: condicional, não agendado. Só entra se o overhead de operar Debezium+Kafka virar bloqueador real de adoção confirmado (não é especulativo). Parser de WAL nativo do Postgres, resume por LSN em vez de offset Kafka.
3. **`nexus-ai`: só a feature `cpu` existe** — `cuda`/`metal`/`api` (aceleração de embeddings) não implementados. O perfil `cuda` do Docker já tem a infra de runtime pronta (base image + `--gpus all`), mas não acelera nada até isso ser feito.
4. **Alertas: só Slack implementado** — MS Teams, PagerDuty, Email e Webhook genérico ainda faltam (ver `CLAUDE.md §6`).
5. **Windows (`.msi`/winget) e macOS (Homebrew/`.dmg`): specs escritos em `packaging/`, nunca validados em máquina real** (sandbox de dev é Linux). Falta também um build script dos drivers ADBC pra Windows (`.dll`) e macOS (`.dylib`) — os scripts atuais (`scripts/build-adbc-*.sh`) só geram `.so`.
6. **`.rpm` nunca testado** — `scripts/package-rpm.sh` está escrito mas o sandbox não tem `rpmbuild` instalado pra validar.
7. **Estatísticas de hardware (CPU/RAM/GPU) não implementadas** — o WebSocket de progresso (`ARCHITECTURE.md §12`) só transmite `batches_written`/`rows_written`/`bytes_written` por partição. `CLAUDE.md §6` menciona "estatísticas de hardware" na UI real-time; isso ainda não existe (nenhum uso de `sysinfo` ou equivalente em `nexus-server`).
8. **Imagens Docker: build+smoke-test no CI (`docker-image` job), mas nunca publicadas** — `.github/workflows/release.yml` só produz tarballs Linux/macOS; não há passo de `docker push` pra nenhum registry (GHCR ou outro) num tag de release. "Prontas" na Fase 11 abaixo quer dizer "buildam e passam no smoke test", não "publicadas pra usuário final puxar".
9. **Admin (gestão de usuários) só existe no backend** — rotas `GET/POST /users`, `GET/DELETE /users/{username}`, `PUT /users/{username}/role` existem em `nexus-server` (ver `crates/nexus-server/src/lib.rs`), mas o Canvas (Fase 8) não tem nenhuma tela pra elas — só dá pra gerenciar usuários via API direta.
10. Ver também a seção **Débitos conhecidos** no fim deste arquivo (secrets sem KMS, RBAC sem escopo por recurso, versões de dependência pinadas, advisories RustSec aceitos).

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

## Fase 4 — CDC (escopo faseado, ver `ARCHITECTURE.md §7`) ✅ (parcial — nativo é condicional)
- [x] CDC via Debezium + Kafka: consumir eventos já decodificados (JSON/Avro) através de `nexus-connector-kafka`, converter pra `RecordBatch` com opcode (I/U/D) via `RecordBatchBuilder`
- [x] Resume automático a partir do checkpoint por partição em falha
- [ ] (Pós-MVP, sob demanda) Parser nativo de WAL Postgres / binlog MySQL — condicional, ver Marco 13 do `IMPLEMENTATION_PLAN.md`; só entra se overhead de operar Debezium+Kafka virar bloqueador real de adoção

## Fase 5 — AI Lakehouse (`nexus-ai`) ✅ (GPU/API acceleration pendente)
- [x] Chunking (fixed-size, recursive, semantic)
- [x] Embeddings via `ort` (feature `cpu`) — `cuda`/`metal`/`api` ainda não implementados (`crates/nexus-ai/Cargo.toml` só define `cpu`)
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

## Fase 9 — Observabilidade & Alertas ✅ (parcial — só Slack)
- [x] `tracing` estruturado (JSON) + OpenTelemetry (traces via OTLP + métricas Prometheus em `/metrics`)
- [x] Alertas assíncronos: Slack (Block Kit) — MS Teams/PagerDuty/Email/Webhook ainda não implementados

## Fase 10 — dbt (ELT opcional) ✅
- [x] Subprocesso assíncrono invocando `dbt run`/`build`/`test` pós-carga (feature `dbt`), com resultado de lineage/qualidade no histórico de execução

## Fase 11 — Distribuição multiplataforma ✅ (Windows/macOS não validados)
- [x] Single binary com frontend embutido (`rust-embed`, feature `embed-ui`)
- [x] Empacotamento: AppImage/deb (Linux, testado) — rpm (spec pronto, sem `rpmbuild` local) — `.msi`/winget (Windows) e Homebrew/dmg (macOS) têm specs em `packaging/` mas não foram validados em máquina real
- [x] Imagem Docker com perfil `cuda` selecionável via `--build-arg RUNTIME_IMAGE` (base image + `--gpus all` prontos; aceleração real pendente da Fase 5's `cuda` feature) — **não multi-arch ainda**: `Dockerfile`/CI não passam `platforms:` pro build, então só builda pra arquitetura do runner (amd64); ver item 8 das Pendências ativas acima sobre a imagem também nunca ser publicada num registry
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

---

**Critério de "MVP pronto"**: Fases 0–3 + 7 (parcial: auth básica) + 8 (canvas mínimo) funcionando end-to-end — mover dados de Postgres pra Postgres via canvas visual, com checkpoint por partição, retry e escrita idempotente. **Atingido e superado** — Fases 0–11, 13 e 14 completas, só falta Fase 12 (enterprise, repo separado) e os itens condicionais/parciais marcados acima.

## Débitos conhecidos (aceitos pro MVP, resolver antes de vender enterprise)

- **Secrets via env var, sem KMS/rotação** — ok pra self-host single-tenant; precisa migrar pra KMS (AWS/GCP/Vault) antes do primeiro cliente enterprise (`ARCHITECTURE.md §10`).
- **RBAC sem escopo por recurso** — 4 papéis globais chega pro MVP; SaaS multi-tenant vai exigir permissão por pipeline/credencial.
- ~~**Ciclo de vida do modelo ONNX indefinido**~~ — decidido 2026-07-30: HF Hub em runtime + cache local (`ARCHITECTURE.md §8`).
- **Execução single-node** — decisão deliberada de escopo, não limitação a esconder do usuário (`ARCHITECTURE.md §6`). Documentar isso claramente também no README quando o produto for anunciado publicamente.
- **5 advisories RustSec aceitos (ver `.github/workflows/ci.yml`'s `cargo-audit` job)**: `RUSTSEC-2023-0071` (rsa, via `jsonwebtoken`'s RS256 — sem correção disponível upstream), `RUSTSEC-2026-0194`/`-0195` (quick-xml, via `object_store`/`datafusion` 54.1.0 — mesmo pin de arrow 58.x abaixo), `RUSTSEC-2025-0009`/`RUSTSEC-2024-0336` (ring/rustls, via `milvus-sdk-rust`'s tonic 0.8.3 — sem release mais nova do SDK). Reavaliar cada um quando a dependência que os carrega soltar uma versão nova.
- **`arrow-array`/`arrow-schema` fixados em `58.4.0` e `adbc_core`/`adbc_driver_manager`/`adbc_ffi` em `0.23.0` (não a última, `0.24.0`) em todo o workspace** — `datafusion` 54.1.0 (última versão publicada) ainda depende de arrow 58.x, enquanto adbc 0.24.0 já exige arrow ≥59. Sem overlap entre as duas, então fixamos tudo em 58.4.0/0.23.0 pra ter um `RecordBatch` só no grafo de dependências. Reavaliar quando o datafusion soltar uma versão em cima de arrow 59+.
