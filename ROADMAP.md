# 🗺️ Roadmap — NexusFlow

Ordem por dependência técnica, não por prioridade de negócio isolada. Cada fase assume a anterior estável.

> Detalhamento de engenharia (arquivos/crates concretos, critério de "pronto" por marco): ver [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md).

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

## Fase 3 — Conectores híbridos
- [ ] `nexus-connector-rest` genérico via `reqwest` + `RecordBatchBuilder`
- [ ] `nexus-connector-mongodb` (bson → Arrow)
- [ ] `nexus-connector-odbc` bridging (legado)
- [ ] `nexus-connector-kafka` (base pra CDC da Fase 4)

## Fase 4 — CDC (escopo faseado, ver `ARCHITECTURE.md §7`)
- [ ] CDC via Debezium + Kafka: consumir eventos já decodificados (JSON/Avro) através de `nexus-connector-kafka`, converter pra `RecordBatch` com opcode (I/U/D) via `RecordBatchBuilder`
- [ ] Resume automático a partir do checkpoint por partição em falha
- [ ] (Pós-MVP, sob demanda) Parser nativo de WAL Postgres / binlog MySQL — só se overhead de operar Debezium+Kafka virar bloqueador real de adoção

## Fase 5 — AI Lakehouse (`nexus-ai`)
- [ ] Chunking (fixed-size, recursive, semantic)
- [ ] Embeddings via `ort` (feature `cpu` primeiro; `cuda`/`metal`/`api` depois)
- [ ] Sinks vetoriais: pgvector → Qdrant → LanceDB → Milvus → Pinecone → ChromaDB (nessa ordem, do mais simples de operar ao mais complexo)

## Fase 6 — Data Lake formats
- [ ] Sink Parquet puro
- [ ] Delta Lake (`deltalake`)
- [ ] Iceberg (`iceberg-rust`)

## Fase 7 — `nexus-server` (API + Auth)
- [ ] Axum REST: CRUD de pipelines, execução, status
- [ ] JWT + RBAC (`Read`/`Execute`/`Write`/`Admin`)
- [ ] Segredos AES-256-GCM em repouso
- [ ] WebSocket: progresso de execução em tempo real

## Fase 8 — Frontend (React Flow)
- [ ] Canvas node-based: criar/editar DAG, source of truth em JSON
- [ ] Painel de execução em tempo real (MB/s, linhas/s, logs)
- [ ] Tela de credenciais (sem exibir segredo em plain text)

## Fase 9 — Observabilidade & Alertas
- [ ] `tracing` estruturado (JSON) + OpenTelemetry
- [ ] Alertas assíncronos: Slack, MS Teams, PagerDuty, Email, Webhook

## Fase 10 — dbt (ELT opcional)
- [ ] Subprocesso assíncrono invocando `dbt run`/`dbt build` pós-carga

## Fase 11 — Distribuição multiplataforma
- [ ] Single binary com frontend embutido (`rust-embed`)
- [ ] Empacotamento: `.msi`/winget (Windows), AppImage/deb/rpm (Linux), Homebrew/dmg (macOS)
- [ ] Imagens Docker multi-arch com suporte `--gpus all`

## Fase 12 — Enterprise connectors (paralelo, repo separado)
- [ ] Definir primeiro conector pago (candidato: Snowflake/Databricks avançado)
- [ ] Mecanismo de license key (JWT) validado em `nexus-server`
- [ ] Ver [`LICENSING.md`](./LICENSING.md)

---

**Critério de "MVP pronto"**: Fases 0–3 + 7 (parcial: auth básica) + 8 (canvas mínimo) funcionando end-to-end — mover dados de Postgres pra Postgres via canvas visual, com checkpoint por partição, retry e escrita idempotente.

## Débitos conhecidos (aceitos pro MVP, resolver antes de vender enterprise)

- **Secrets via env var, sem KMS/rotação** — ok pra self-host single-tenant; precisa migrar pra KMS (AWS/GCP/Vault) antes do primeiro cliente enterprise (`ARCHITECTURE.md §10`).
- **RBAC sem escopo por recurso** — 4 papéis globais chega pro MVP; SaaS multi-tenant vai exigir permissão por pipeline/credencial.
- ~~**Ciclo de vida do modelo ONNX indefinido**~~ — decidido 2026-07-30: HF Hub em runtime + cache local (`ARCHITECTURE.md §8`).
- **Execução single-node** — decisão deliberada de escopo, não limitação a esconder do usuário (`ARCHITECTURE.md §6`). Documentar isso claramente também no README quando o produto for anunciado publicamente.
- **`arrow-array`/`arrow-schema` fixados em `58.4.0` e `adbc_core`/`adbc_driver_manager`/`adbc_ffi` em `0.23.0` (não a última, `0.24.0`) em todo o workspace** — `datafusion` 54.1.0 (última versão publicada) ainda depende de arrow 58.x, enquanto adbc 0.24.0 já exige arrow ≥59. Sem overlap entre as duas, então fixamos tudo em 58.4.0/0.23.0 pra ter um `RecordBatch` só no grafo de dependências. Reavaliar quando o datafusion soltar uma versão em cima de arrow 59+.
