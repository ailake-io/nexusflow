# Plano de Implementação — NexusFlow

Detalhamento de engenharia por trás do `ROADMAP.md`: em que ordem construir, quais crates/arquivos concretamente, e qual o critério de "pronto" de cada marco. `ROADMAP.md` continua sendo a lista de fases em alto nível; este documento é o "como" de cada uma.

Parte da arquitetura já revisada em `ARCHITECTURE.md` v2: crate-por-conector, backpressure particionado, checkpoint idempotente, escopo single-node, CDC faseado via Debezium.

---

## Marco 0 — Workspace e contratos base
**Objetivo:** esqueleto Cargo compilando, com os traits centrais definidos e testáveis, sem nenhum conector real ainda.

- `Cargo.toml` workspace na raiz, membros: `crates/nexus-core`, `crates/nexus-ai`, `crates/nexus-server`, `crates/nexus-connectors/*` (workspace aninhado, vazio por enquanto), `src/` (bin).
- `nexus-core`:
  - `error.rs` — `NexusError` (`thiserror`), variantes: `Connector`, `Schema`, `Serialization`, `Checkpoint`.
  - `traits.rs` — `Source`, `Sink`, `Transform` (assinatura definida em `ARCHITECTURE.md §2`), mais `ConnectorCapability` enum (`AdbcNative`/`ArrowFlight`/`Bridged`).
  - `checkpoint.rs` — struct `CheckpointCursor { partition_id, last_updated_at: Option<DateTime<Utc>>, offset: Option<i64>, opcode: Option<Opcode> }`.
  - `registry.rs` — `ConnectorRegistry` (registro em runtime; usar `inventory` crate pra auto-registro via macro, evita lista hardcoded).
  - `record_batch_builder.rs` — `RecordBatchBuilder` genérico (linhas heterogêneas → `RecordBatch`), com testes usando dados mock (`serde_json::Value` → batch).
- `src/main.rs` — bootstrap mínimo: carrega config (env/arquivo), chama `nexus_server::run()`. Nenhuma lógica além disso (contrato do `ARCHITECTURE.md §1`).
- `nexus-server` (esqueleto): `lib.rs` com `pub async fn run()` que só sobe um Axum app vazio com `/health`.
- CI: workflow (GitHub Actions) rodando `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --workspace` em cada push/PR pra `develop`.

**Critério de pronto:** `cargo build --workspace` e `cargo test --workspace` verdes; `curl localhost:8080/health` responde 200.

---

## Marco 1 — MVP Fast-Path (Postgres → Postgres)
**Objetivo:** primeiro pipeline real, end-to-end, provando o modelo de particionamento/checkpoint/idempotência.

- `crates/nexus-connectors/nexus-connector-postgres/`: implementa `Source` e `Sink` via `adbc_driver_manager` + driver Postgres ADBC.
  - Particionamento: leitura por range de chave primária (`WHERE id BETWEEN ? AND ?`), configurável por número de partições no node.
  - Sink: `INSERT ... ON CONFLICT (pk) DO UPDATE` (upsert — contrato de idempotência do `ARCHITECTURE.md §5`).
  - Registra-se no `ConnectorRegistry` via macro do `nexus-core`.
- `nexus-core::dag` — parser de DAG em JSON estrito, MVP suporta só grafo linear `source → sink` (sem transform ainda). Validação de schema JSON com `serde` + `jsonschema` (ou validação manual simples).
- `nexus-core::pipeline` — engine de execução: por partição, spawna `tokio::task` com canal `mpsc::channel(N)` (N configurável no JSON do pipeline, default documentado, não fixo em 100 no código), lê do `Source`, escreve no `Sink`, emite `CheckpointCursor` a cada commit.
- `nexus-server`: persistência de checkpoint em SQLite via `sqlx` (tabela `checkpoints(pipeline_id, partition_id, last_updated_at, offset, updated_at)`), endpoint REST `POST /pipelines/{id}/run` que dispara execução.
- Teste de integração: usar `testcontainers-rs` (Postgres em container) — pipeline real rodando em CI, não só mock. Teste de resume: matar o processo no meio, reiniciar, confirmar que retoma do cursor certo e não duplica linha (valida idempotência).

**Critério de pronto:** mover 100k linhas de uma tabela Postgres pra outra via `POST /pipelines/{id}/run`, matar e retomar no meio sem duplicar dado, checkpoint por partição persistido.

---

## Marco 2 — Transform leve (DataFusion) + segundo conector
- Node de transform via SQL em memória (`datafusion::prelude::SessionContext`, registra `RecordBatch` como tabela in-memory, roda query, retorna `RecordBatch`).
- DAG parser: suportar grafo com N sources → 1 transform → N sinks (fan-in/fan-out do `ARCHITECTURE.md §4`), cada aresta com canal próprio.
- Segundo conector: `nexus-connector-sqlite` (ADBC) — prova que o `ConnectorRegistry` funciona com 2+ conectores plugados sem mudar `nexus-server`.

**Critério de pronto:** pipeline Postgres → transform SQL (filtro/agregação simples) → SQLite, com fan-in de 2 sources testado.

---

## Marco 3 — Conectores híbridos (bridging)
- `nexus-connector-rest`: genérico via `reqwest`, usa `RecordBatchBuilder` pra converter JSON de resposta em `RecordBatch`. Config de node inclui paginação (cursor-based e offset-based).
- `nexus-connector-mongodb`: `bson::Document` → `RecordBatch` via `RecordBatchBuilder`.
- `nexus-connector-kafka` (via `rdkafka`, feature-gated): consumer básico, mensagem → `RecordBatch`. Serve de base pro CDC do Marco 4.
- `nexus-connector-odbc` (via `odbc-api`, feature-gated): bridging genérico pra bancos legados.

**Critério de pronto:** cada conector com teste unitário mockável (sem depender de serviço real rodando, regra `CLAUDE.md §8.6`) + 1 teste de integração com `testcontainers`/mock server onde aplicável (Mongo e Kafka via testcontainers; REST via `wiremock`).

---

## Marco 4 — CDC via Debezium+Kafka (não parser nativo)
- Documentar/prover `docker-compose` de referência (Debezium connector + Kafka) em `docs/cdc-reference/` — não é infra de produção, é ambiente de teste/exemplo.
- `nexus-connector-kafka` ganha modo "Debezium envelope" — decodifica JSON/Avro do Debezium, extrai `op` (`c`/`u`/`d`/`r`) e mapeia pro `Opcode` interno (`I`/`U`/`D`), gera `RecordBatch` com coluna de opcode.
- Teste de integração: `testcontainers` subindo Postgres + Debezium + Kafka, gerar INSERT/UPDATE/DELETE na fonte, validar que chegam como eventos com opcode certo no `RecordBatch`.
- Parser nativo de WAL/binlog **fica fora deste marco** (só entra se virar prioridade de negócio confirmada — ver débito em `ROADMAP.md`).

**Critério de pronto:** pipeline CDC end-to-end Postgres→Kafka(Debezium)→sink, com opcode correto e resume por partição.

---

## Marco 5 — AI Lakehouse (`nexus-ai`)
- ~~**Pré-requisito de decisão** (bloqueia início do código)~~ — resolvido 2026-07-30: HF Hub em runtime + cache local, versionado por repo+revision fixados na config (ver `ARCHITECTURE.md §8`).
- `nexus-ai::chunking` — 3 estratégias (fixed-size window, recursive character, semantic), funções puras `fn chunk(text: &str, cfg: ChunkConfig) -> Vec<String>`, testáveis sem I/O.
- `nexus-ai::embedding` — wrapper `ort` feature-gated (`cpu` primeiro; `cuda`/`metal`/`api` depois, cada um behind `[features]`), batch de inferência, anexa coluna `embedding: FixedSizeList<Float32>` ao `RecordBatch`.
- Sinks vetoriais, nesta ordem (mais simples de operar → mais complexo, conforme `ROADMAP.md` Fase 5): `nexus-connector-pgvector` → `nexus-connector-qdrant` → `nexus-connector-lancedb` → `nexus-connector-milvus` → `nexus-connector-pinecone` → `nexus-connector-chromadb`.

**Critério de pronto:** pipeline texto→chunk→embedding(CPU)→pgvector rodando end-to-end com teste de integração (`testcontainers` Postgres+pgvector).

---

## Marco 6 — Data Lake formats
- Sink Parquet puro (via `parquet` crate direto).
- `nexus-connector-deltalake` (via `deltalake` crate).
- `nexus-connector-iceberg` (via `iceberg-rust`).

**Critério de pronto:** mesmo pipeline de origem (ex. Postgres) escrevendo em Parquet, Delta e Iceberg, cada um com teste de leitura de volta pra validar schema/dado.

---

## Marco 7 — `nexus-server` completo (Auth/RBAC/WebSocket)
- JWT (`jsonwebtoken`) + middleware Axum de RBAC (`Read`/`Execute`/`Write`/`Admin`), checado antes do handler (`ARCHITECTURE.md §10`).
- Segredos: AES-256-GCM (`aes-gcm` crate) antes de persistir credencial de conector; chave via env var no MVP (débito de KMS documentado, não resolvido aqui).
- CRUD de pipelines via REST (`/pipelines`, `/pipelines/{id}/runs`).
- WebSocket: canal de progresso (linhas/s, MB/s, log stream) por execução — publica em `tokio::sync::broadcast`, handler Axum faz upgrade pra WS e repassa.
- Alertas assíncronos (`tokio::spawn`, não bloqueia pipeline): Slack primeiro (Block Kit), depois Teams/PagerDuty/Email/Webhook — nessa ordem de prioridade de implementação.

**Critério de pronto:** login JWT, criar pipeline via API, rodar, acompanhar progresso via WebSocket, receber alerta Slack em caso de falha.

---

## Marco 8 — Frontend (React Flow)
- Canvas: nodes = conectores do `ConnectorRegistry` expostos via `GET /connectors` (catálogo dinâmico, não hardcoded no frontend).
- Source of truth do DAG = JSON estrito (mesmo schema validado no backend no Marco 1) — frontend serializa/desserializa, nunca inventa campo que o backend não entende.
- Painel de execução: consome o WebSocket do Marco 7 (MB/s, linhas/s, logs).
- Tela de credenciais: nunca renderiza segredo em plain text (mascarado, só mostra "•••• editado em Y").

**Critério de pronto:** criar pipeline Postgres→Postgres 100% pela UI, rodar, ver progresso em tempo real, sem tocar em JSON manualmente.

---

## Marco 9 — Observabilidade
- `tracing` com formato JSON (`tracing-subscriber` + `tracing-opentelemetry`), export OTel.
- Métricas (linhas/s, MB/s) — decidir formato concreto: expor via OTel metrics + endpoint Prometheus `/metrics`, e o mesmo dado alimenta o WebSocket do Marco 7 (uma única fonte de verdade pro número, não duas contagens divergentes).

**Critério de pronto:** dashboard Grafana básico lendo `/metrics`, números batendo com o que a UI mostra.

---

## Marco 10 — dbt (ELT opcional)
- Subprocesso assíncrono (`tokio::process::Command`) invocando `dbt run`/`dbt build` pós-carga, feature-gated (dbt é opcional, `CLAUDE.md §4.4`).
- Capturar `manifest.json`/`run_results.json` do dbt pra alimentar observabilidade/lineage básico.

**Critério de pronto:** pipeline com "modo ELT" ativado dispara `dbt build` após carga bruta, resultado aparece nos logs da execução.

---

## Marco 11 — Distribuição multiplataforma
- `rust-embed` incorporando build do frontend no binário.
- Empacotamento: AppImage/deb/rpm (Linux) primeiro (ambiente de dev), depois `.msi`/winget (Windows) e Homebrew/dmg (macOS).
- Docker multi-arch com `--gpus all` (perfil `cuda`).

**Critério de pronto:** `docker run` sobe binário único servindo UI em `localhost:8080`, sem dependência externa além do container.

---

## Marco 12 — Primeiro conector enterprise (paralelo, repo separado)
- Repo privado novo (ex. `ailake-io/nexusflow-connectors-enterprise`), fora do escopo deste repo.
- Mecanismo de license key (JWT assinado) validado em `nexus-server` antes de expor node enterprise no catálogo (`ARCHITECTURE.md §11`).
- Primeiro candidato: decidir com dado de mercado, não assumir aqui.

**Critério de pronto:** feature flag `enterprise` compilando um binário que só expõe o conector premium com license key válida.

---

## Marco 13 — CDC nativo (WAL/binlog, condicional — sob demanda)

**Não agendado.** Só entra em execução se o overhead operacional de manter Debezium+Kafka como dependência do Marco 4 virar bloqueador real de adoção confirmado — não é trabalho especulativo, é resposta a sinal de mercado (ver ARCHITECTURE.md §7, débito registrado desde o Marco 4).

- Parser binário do protocolo de replicação lógica do Postgres (WAL, formato `pgoutput` ou `wal2json` direto via `libpq` streaming replication) — sem passar por Kafka/Debezium. Primeiro candidato: Postgres, por já ser o fast-path (Marco 1).
- Elimina a dependência de infra externa (Kafka+Debezium) só pra CDC — trade-off é reimplementar dentro do nexusflow um subsistema do tamanho do Debezium: gestão de replication slot, resume por LSN, decodificação binária do WAL, mapeamento de tipos Postgres → Arrow sem o schema registry que o Debezium já resolve.
- `nexus-connector-postgres` ganha um modo de leitura CDC nativo, opcode (`I`/`U`/`D`) como coluna extra do `RecordBatch` — mesma convenção do Marco 4 (`ARCHITECTURE.md §5`), pra manter os dois modos (Debezium e nativo) intercambiáveis do ponto de vista do resto do pipeline.
- MySQL (binlog) e outros bancos só entram depois, cada um é um parser de protocolo binário próprio — não reaproveita nada do parser Postgres além da convenção de opcode/checkpoint.

**Critério de pronto:** pipeline CDC nativo Postgres, sem Kafka/Debezium na frente, com resume por LSN (não por offset de partição Kafka) e opcode correto — validado com teste de integração contra Postgres real gerando INSERT/UPDATE/DELETE.

---

## Ordem de execução e paralelização

Sequencial obrigatório: Marco 0 → 1 → (2 e 3 podem ser paralelos entre si) → 4. Marco 5, 6 podem rodar em paralelo com 7 (times/momentos diferentes, sem dependência forte — todos consomem só os traits do Marco 0-1). Marco 8 depende de 7 (precisa da API/WS prontos). Marco 9 pode começar cedo (instrumentar desde o Marco 1) mas "critério de pronto" formal só fecha depois do Marco 7. Marco 10 e 11 são independentes, podem entrar a qualquer momento após Marco 1. Marco 12 é paralelo ao resto, em repo separado. Marco 13 não tem posição na sequência — é condicional, só entra se confirmado por sinal de adoção (ver Marco 13).

## Verificação

Cada marco tem `cargo test --workspace` + pelo menos 1 teste de integração via `testcontainers-rs` cobrindo o "critério de pronto" descrito. CI (GitHub Actions, criado no Marco 0) roda tudo isso a cada push pra `develop`. Nenhum marco é considerado fechado sem o teste de integração correspondente passando em CI, não só localmente.
