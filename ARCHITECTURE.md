# 🏗️ Arquitetura Técnica — NexusFlow

Complementa `CLAUDE.md §4`. Aqui o detalhe de design por trás de cada componente do core.

## 1. Camadas do sistema

`src/` (binário principal) só faz bootstrap: carrega config e sobe `nexus-server`. Toda orquestração (scheduler, checkpoint, RBAC) mora em `nexus-server` — não existe uma segunda implementação de scheduler no binário principal.

```text
┌─────────────────────────────────────────────────────────┐
│  Frontend (React Flow) — DAG em JSON estrito             │
└───────────────────────┬───────────────────────────────────┘
                        │ REST/WebSocket (Axum)
┌───────────────────────▼───────────────────────────────────┐
│  nexus-server — Auth/RBAC, Scheduler, Checkpoint store    │
│  (SQLite/Postgres via sqlx), Alertas, Observabilidade      │
└───────────────────────┬───────────────────────────────────┘
                        │ spawn de pipelines (tokio::task)
┌───────────────────────▼───────────────────────────────────┐
│  nexus-core — DAG parser, traits Source/Sink/Transform     │
│  RecordBatch (Arrow) como unidade de dado universal        │
└──────┬────────────────┬──────────────────┬─────────────────┘
       │                │                  │
┌──────▼──────┐  ┌───────▼───────┐  ┌───────▼────────┐
│ nexus-      │  │ nexus-ai      │  │ nexus-connectors│
│ connectors  │  │ (chunking +   │  │ (destinos:      │
│ (fontes)    │  │ embeddings)   │  │ DB/Lake/Vector) │
└─────────────┘  └───────────────┘  └─────────────────┘
```

## 2. Traits centrais (`nexus-core`)

Contrato mínimo que todo conector deve implementar (mockável, sem dependência de banco real nos testes unitários — regra `CLAUDE.md §8.6`):

```rust
#[async_trait]
pub trait Source {
    async fn read_batches(&mut self) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError>;
    fn schema(&self) -> SchemaRef;
}

#[async_trait]
pub trait Sink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError>;
    async fn commit_checkpoint(&mut self, cursor: CheckpointCursor) -> Result<(), NexusError>;
}

#[async_trait]
pub trait Transform {
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, NexusError>;
}
```

`RecordBatchBuilder` é o adapter genérico usado por qualquer conector híbrido (sem ADBC nativo) para produzir `RecordBatch` a partir de linhas heterogêneas (JSON de API REST, `bson::Document` do MongoDB, etc).

## 3. Roteador de conectores

Decisão de fast-path vs. híbrido acontece em tempo de configuração do node (não em runtime dinâmico): cada conector se registra com uma `ConnectorCapability` (`AdbcNative`, `ArrowFlight`, `Bridged`). O roteador só escolhe a estratégia de leitura/escrita; o pipeline downstream trata tudo como `RecordBatch` — nenhuma lógica de negócio depende de qual caminho foi usado.

**Cada conector é um crate próprio** (`nexus-connector-postgres`, `nexus-connector-mongodb`, ...) sob `crates/nexus-connectors/` (ver `CLAUDE.md §3`), não módulos dentro de um crate monolítico. Motivo: compile time, tamanho de binário e a fronteira OSS/enterprise (`LICENSING.md`) exigem que cada conector possa ser incluído/excluído do build independentemente via feature flag — um crate único com todos os SDKs (mongodb + rdkafka + odbc-api + N vector DB SDKs) inviabiliza isso.

Registro: `nexus-core` expõe um `ConnectorRegistry` (trait object registry, ex. via `inventory` ou registro explícito no bootstrap) — cada crate de conector se registra chamando uma macro/fn de registro no seu próprio `lib.rs`. `nexus-server` consulta o registry pra popular o catálogo de nodes do canvas; não existe lista hardcoded de conectores em `nexus-server`.

## 4. Streaming, paralelismo e backpressure

Modelo ingênuo de 1 canal `mpsc` entre 1 source e 1 sink não escala além do caso trivial (não cobre múltiplas partições, múltiplos workers, nem sink com rate-limit variável). Modelo real:

- **Unidade de paralelismo é a partição**, não o pipeline inteiro. Um `Source` que suporta leitura particionada (ex.: por range de chave primária, por tópico/partição Kafka, por shard) expõe N streams independentes; cada partição tem seu próprio canal `mpsc::channel(N)` Source→Sink e seu próprio `CheckpointCursor`.
- Tamanho do canal por partição é configurável por pipeline (não fixo em 100 hardcoded) — sink lento (ex.: API com rate-limit) drena mais devagar, canal enche, Source daquela partição bloqueia em `.send()`. Backpressure continua sendo "capacidade do canal", só que por partição em vez de global.
- Fan-out (1 source → N sinks) e fan-in (N sources → 1 sink) são grafos válidos no DAG; cada aresta do DAG é um canal independente com seu próprio backpressure — não existe canal compartilhado entre arestas diferentes.
- Pipeline sem suporte a particionamento (ex.: API REST simples) roda como caso degenerado: 1 partição só, mesmo comportamento do modelo antigo.

## 5. Checkpointing e idempotência

- Checkpoint é **por partição**, não por pipeline: `CheckpointCursor { partition_id, last_updated_at, offset, opcode }`. `nexus-server` persiste um registro por `(pipeline_id, partition_id)`.
- Retry/resume lê o último cursor de cada partição independentemente — permite retomar só a partição que falhou, sem reprocessar as demais.
- **Garantia é at-least-once, não exactly-once.** Isso precisa ser dito explicitamente pro autor de cada `Sink`: retry após falha de commit pode reenviar um batch já parcialmente aplicado. Todo `Sink` deve implementar escrita idempotente (upsert por chave primária, ou dedup por `(partition_id, offset)` na tabela destino) — não é opcional, é contrato da trait `Sink`.
- CDC: eventos carregam opcode (`I`/`U`/`D`) como coluna extra do `RecordBatch`, nunca como side-channel separado — mantém uniformidade de tipo em todo o pipeline.

## 6. Escopo de execução: single-node (decisão explícita)

NexusFlow MVP e OSS rodam **single-node**: um processo `nexus-server` executa todos os pipelines daquele deployment, paralelismo é *dentro* do processo (tokio tasks por partição, não processos/máquinas distintas). Isso é escopo deliberado, não limitação esquecida — mesmo modelo do Airbyte OSS self-hosted.

Motivo: DataFusion (usado nas transformações SQL) é single-node por padrão; ir pra execução distribuída exigiria Ballista (ou equivalente) e reabre toda a arquitetura de scheduling/checkpoint acima. Não faz parte do escopo atual.

Caminho de escala, se necessário no futuro: escalar horizontalmente por **pipeline** (rodar pipelines diferentes em processos/máquinas diferentes, cada um single-node), não por *paralelizar um pipeline entre máquinas*. Isso evita reescrever o modelo de checkpoint/backpressure do §4-5.

## 7. CDC — escopo faseado

Construir parser nativo de WAL (Postgres) ou binlog (MySQL) do zero é um subsistema do tamanho do Debezium, não uma feature de pipeline. Escopo em duas etapas (ver `ROADMAP.md` Fase 4):

1. **MVP de CDC**: consumir eventos já decodificados via Debezium + Kafka — isto é, o conector `Bridged` de Kafka (§ já previsto) recebe eventos CDC no formato Debezium (JSON/Avro) e converte pra `RecordBatch` com opcode, via `RecordBatchBuilder`. Nenhum código novo de parsing de protocolo binário de replicação.
2. **CDC nativo** (pós-MVP, sob demanda): parser direto de WAL/binlog, só se o overhead operacional de manter Debezium+Kafka como dependência for um bloqueador real de adoção.

## 8. Pipeline de embeddings (`nexus-ai`)

Fluxo: `RecordBatch` (texto bruto) → chunking (fixed-size / recursive / semantic) → batch de inferência ONNX (`ort`, feature-gated por `cpu`/`cuda`/`metal`/`api`) → coluna `embedding: FixedSizeList<Float32>` anexada ao `RecordBatch` original → Sink vetorial (LanceDB/Qdrant/Milvus/Pinecone/ChromaDB/pgvector).

Chunking e embedding são etapas **puras** (sem I/O) para ficarem testáveis sem GPU/rede — só o Sink final faz I/O externo.

**Ciclo de vida do modelo ONNX (decidido, Marco 5, 2026-07-30)**: download do Hugging Face Hub em runtime (crate `hf-hub`), cacheado localmente em `~/.cache/nexusflow/models/` (ou equivalente XDG por SO). Primeira execução com um modelo precisa de rede; execuções seguintes usam o cache. Versionamento: repo HF + revision (commit/tag) fixados na config do node — nunca "latest" implícito, pra reprodutibilidade. Sem empacotamento no binário (mantém o binário leve, custo é exigir rede na primeira execução com cada modelo novo).

## 9. Erros

- Erros locais de cada crate: `thiserror` (enum tipado, ex. `NexusConnectorError`).
- Orquestração de alto nível (CLI, scheduler): `anyhow`.
- Axum: struct `ApiError` implementando `IntoResponse`, mapeando variantes de erro para status HTTP (400/401/403/404/409/500) — nunca vazar erro interno cru pro cliente.

## 10. Segurança

- Segredos de conexão (URIs, tokens) criptografados com **AES-256-GCM** antes de persistir em `sqlx` (Postgres/SQLite). Chave de criptografia vem de variável de ambiente/secret manager, nunca hardcoded.
- RBAC checado em middleware Axum, antes do handler — não dentro de cada handler individualmente.
- **Débito conhecido**: chave via env var não atende requisito enterprise de KMS (AWS/GCP/Vault) + rotação de chave. Aceitável pro MVP self-host; precisa entrar no roadmap antes do primeiro cliente enterprise (ver `LICENSING.md`).
- **Débito conhecido**: RBAC atual é 4 papéis globais (`Read`/`Execute`/`Write`/`Admin`), sem escopo por recurso (pipeline específico, credencial específica). Suficiente pra single-tenant self-host; bloqueador se o produto for SaaS multi-tenant no futuro.

## 11. Distribuição de conectores enterprise

Ver [`LICENSING.md`](./LICENSING.md) §2. Tecnicamente: `nexus-server` carrega conectores enterprise como plugin via feature flag `enterprise` compilado num binário separado, validando license key (JWT) antes de expor o node no catálogo do canvas.
