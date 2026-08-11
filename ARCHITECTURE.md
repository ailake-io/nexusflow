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
pub trait Transform: Send + Sync {
    async fn apply(
        &self,
        inputs: Vec<(String, SchemaRef, Vec<RecordBatch>)>,
    ) -> Result<Vec<RecordBatch>, NexusError>;
}
```

`RecordBatchBuilder` é o adapter genérico usado por qualquer conector híbrido (sem ADBC nativo) para produzir `RecordBatch` a partir de linhas heterogêneas (JSON de API REST, `bson::Document` do MongoDB, etc).

## 3. Roteador de conectores

Decisão de fast-path vs. híbrido acontece em tempo de configuração do node (não em runtime dinâmico): cada conector se registra com uma `ConnectorCapability` (`AdbcNative`, `ArrowFlight`, `Bridged`). O roteador só escolhe a estratégia de leitura/escrita; o pipeline downstream trata tudo como `RecordBatch` — nenhuma lógica de negócio depende de qual caminho foi usado.

**Cada conector é um crate próprio** (`nexus-connector-postgres`, `nexus-connector-mongodb`, ...) sob `crates/nexus-connectors/` (ver `CLAUDE.md §3`), não módulos dentro de um crate monolítico. Motivo: compile time, tamanho de binário e a fronteira OSS/enterprise (`LICENSING.md`) exigem que cada conector possa ser incluído/excluído do build independentemente via feature flag — um crate único com todos os SDKs (mongodb + rdkafka + odbc-api + N vector DB SDKs) inviabiliza isso.

Registro: `nexus-core` expõe um `ConnectorRegistry` (trait object registry, via `inventory`) — cada crate de conector chama `submit_connector!(nome, capability, ConfigType)` no seu próprio `lib.rs`, passando também o tipo do seu Config struct. `nexus-server` consulta o registry pra popular o catálogo de nodes do canvas; não existe lista hardcoded de conectores em `nexus-server`. O `ConfigType` passado pro macro precisa derivar `schemars::JsonSchema` — `GET /connectors` expõe esse schema (`config_schema`, um `fn() -> serde_json::Value` computado sob demanda, já que fn pointer não é `Serialize`) e o canvas usa isso pra renderizar um formulário real por conector (`SchemaForm.tsx`) em vez de um textarea de JSON cru. Doc comments nos campos do Config struct viram `description` no schema, aparecendo como dica no formulário — não são só documentação de código, são UX real.

**Pegadinha de workspace do Cargo:** cada conector vive no workspace aninhado `crates/nexus-connectors` (`[workspace] members = [...]`), mas isso NÃO isola seus testes do workspace raiz. Assim que `nexus-server` ganha uma dependência de path pra um conector (via feature própria), `cargo metadata --no-deps` rodado da raiz passa a listar aquele conector como membro do workspace raiz também — uma dependência de path sem `package.workspace` explícito não fica presa ao workspace aninhado quando algo no workspace raiz depende dela. Na prática: `cargo test --workspace` na raiz roda até os testes de integração pesados/com container de CADA conector linkado (ex. os testes de CDC nativo via testcontainers de Postgres/MongoDB/MySQL), duplicando o que os jobs `connectors`/`connectors-heavy` do CI já cobrem isoladamente — e competindo pelos mesmos recursos da máquina self-hosted. Por isso o CI usa `-p nexus-core -p nexus-ai -p nexus-server` em vez de `--workspace` nos jobs `clippy`/`test` da raiz (ver `.github/workflows/ci.yml`) — os conectores continuam compilando normalmente como dependência, só os testes próprios de cada crate de conector ficam exclusivamente nos jobs `connectors`/`connectors-heavy`.

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

> **Atualização (§14)**: "single-node" acima é sobre a execução de **um** pipeline (nunca paralelizada entre máquinas). Rodar **múltiplas réplicas** do processo `nexus-server` inteiro (cada uma executando pipelines diferentes, ou a mesma réplica líder do scheduler) já é seguro *storage-wise* com o backend Postgres dos metadados (§14) — manifests de referência (k8s e Docker Swarm) pra isso já existem em `packaging/kubernetes/`/`packaging/swarm/`, ver detalhe no fim do §14.

## 7. CDC — nativo (Postgres/MongoDB/MySQL), sem Debezium/Kafka

O plano original faseava CDC em duas etapas: MVP via Debezium+Kafka, e um "CDC nativo" pós-MVP só se o overhead operacional de rodar Debezium+Kafka+Zookeeper (3 JVMs) virasse bloqueador real de adoção confirmado (`IMPLEMENTATION_PLAN.md` Marco 13). Esse sinal chegou — rodar essa stack é pesado demais pra hardware mais simples — então o CDC nativo saiu de "condicional, não agendado" pra implementado (Fase 18 do `ROADMAP.md`), e o suporte a Debezium foi removido em seguida (não ficou nenhum usuário dependendo dele).

**Único caminho de CDC hoje: nativo**, sem Debezium/Kafka na frente:
- **Postgres** (`postgres-cdc`, feature `cdc` do `nexus-connector-postgres`, crate `pg_walstream`): lê direto do protocolo de replicação lógica (`pgoutput`). O replication slot é criado automaticamente no primeiro connect; **a publicação não é** — `CREATE PUBLICATION <nome> FOR TABLE <tabela>` precisa existir de antemão (mesmo pré-requisito operacional que o Debezium já exige, só não automatizado aqui). Resume: reconectar no mesmo slot já retoma do ponto certo — Postgres guarda isso server-side. Pré-requisito: `wal_level = logical`.
  - Nota de implementação: nem `postgres-protocol` nem `tokio-postgres` (crates.io, mainline) têm suporte a replicação lógica hoje — essa capacidade só existe num fork privado. `pg_walstream` é o crate publicado que resolve isso (wire protocol + decodificação `pgoutput` completos, pure Rust via `rustls-tls`, sem libpq).
- **MongoDB** (`mongodb-cdc`, mesmo crate `nexus-connector-mongodb`, sem dependência nova): Change Streams nativo do driver oficial (`Collection::watch()`). Pré-requisito: rodar como replica set (mesmo single-node serve). `full_document: updateLookup` busca o documento atual numa query separada, no momento da decodificação — isso pode legitimamente vir `null` (observado até sem corrida com delete); o conector cai pra `document_key` nesse caso (linha chega com a identidade e o opcode certos, só sem os valores dessa mudança específica) em vez de descartar a linha.
- **MySQL** (`mysql-cdc`, novo crate `nexus-connector-mysql`, dependência `mysql_cdc`): lê o binlog diretamente, sem conector batch associado (é CDC-only, mesmo padrão do Kafka). Pré-requisito: `binlog_format=ROW`, `binlog_row_image=FULL`, usuário com `REPLICATION SLAVE`/`REPLICATION CLIENT` (privilégios globais — a conexão de replicação não deve declarar um banco padrão, ou falha com "access denied to database"). Diferença importante: colunas são casadas **posicionalmente** com `fields` do config, não por nome — o protocolo binlog não carrega nome de coluna por padrão (só com `binlog_row_metadata=FULL`, MySQL 8.0.1+, desligado por padrão), diferente do Postgres/MongoDB.

Os 3 produzem `RecordBatch` com coluna `__opcode` (`I`/`U`/`D`) na mesma convenção do resto do sistema (§5) — `nexus_core::split_by_opcode` já é 100% agnóstico à origem, nenhum sink precisou mudar.

**Debezium+Kafka foi removido** (código do envelope `Debezium` em `nexus-connector-kafka`, teste de integração de 3 JVMs, `docs/cdc-reference/`) — não é mais um caminho suportado. `nexus-connector-kafka` continua existindo só como fonte genérica de Kafka (mensagens JSON arbitrárias, sem semântica de CDC).

**Canvas**: os 3 conectores aparecem automaticamente no catálogo (`GET /connectors` é dinâmico via `ConnectorRegistry`), e o `NodeInspector` tem um toggle Batch/CDC dentro do mesmo node pra Postgres/MongoDB — troca `data.connector` e limpa o config (os dois modos não compartilham forma de config). Detecta a variante `-cdc` dinamicamente contra o catálogo real, não hardcoded. MySQL não tem toggle (`mysql-cdc` não tem batch equivalente).

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

## 12. Agendamento automático (scheduler) e gestão de pipelines

- `PipelineSpec.schedule` (opcional) guarda uma expressão cron — 5 campos Unix (`min hora dia mês dia-da-semana`) ou 6 campos Quartz (com segundos); `nexus_core::parse_cron_expression` normaliza a forma de 5 campos prependando `"0 "`. Sem `schedule`, o pipeline só roda via `POST /pipelines/{id}/run` manual, comportamento idêntico a antes dessa feature existir.
- `nexus-server::scheduler` roda como um `tokio::spawn` separado, iniciado só em `run()` (nunca em `build_app`, pra não competir com asserts dos testes de integração). Faz *poll* a cada 30s: lista todos os pipelines, filtra os que têm `schedule`, calcula o próximo disparo a partir de uma âncora (`started_at` do último run, ou `created_at` se nunca rodou) e dispara via o mesmo `execute_pipeline` usado pelo endpoint manual — histórico, alertas e dbt se comportam de forma idêntica entre run manual e agendado.
- Proteção contra sobreposição: se o run mais recente do pipeline ainda não tem `finished_at`, o tick pula esse pipeline (não empilha disparos num pipeline lento).
- `schedule` persiste dentro do `spec_ciphertext` já existente (mesma criptografia AES-256-GCM do resto do spec) — não precisou de coluna nova nem migração.
- Gestão completa a partir do Canvas: **Save** (`POST`/`PUT /pipelines/{id}`, cria ou atualiza), **Edit** (recarrega o spec completo — configs de conector inclusas — via `GET /pipelines/{id}/spec`, uma rota nova protegida por role `Write`, não `Read`) e **Delete** (`DELETE /pipelines/{id}`, já existente). A rota `/spec` é a única exceção deliberada ao contrato do §10 de nunca devolver config de conector puro pela API (`GET /pipelines`/`GET /pipelines/{id}` continuam mascarados) — é simétrica a criar/editar: só quem já tem permissão de digitar o segredo recebe ele de volta.
- `PipelineSummary` expõe `last_run_status`/`last_run_at` (via `LEFT JOIN` com a run mais recente de `pipeline_runs`), consumido pela aba "Status" do frontend — um flag por pipeline (verde=sucesso, amarelo=em execução, vermelho=falha, cinza=nunca rodou).

## 13. Preview de dados e dbt como ETL real

- **Preview**: `GET /pipelines/{id}/preview?node={resolved_name}&limit={n}` (role `Execute`, mesma exigida por `/run` — abre conexão real contra o conector) lê as primeiras `limit` linhas (default 50, teto 500) de um node source/sink persistido, reusando `connectors::build_source` — conectores sink-only (milvus/qdrant/lancedb/pgvector/pinecone/chromadb/webhook) devolvem 400 com mensagem clara via o mesmo catch-all de `build_source`, sem código extra por conector. Conversão `RecordBatch` → JSON via `arrow_json::writer::ArrayWriter`, que aguenta qualquer schema arbitrário (ex. coluna de embedding `FixedSizeList<Float32>`). Só cobre pipelines persistidos (sem preview de spec ad-hoc); backend-only por ora, sem botão no Canvas.
- **dbt como ETL** (extensão da Fase 10/Marco 10, que era só ELT): `DbtConfig.output: Option<NodeSpec>` descreve onde ler de volta o resultado que o dbt acabou de transformar (mesmo warehouse que os `sinks` normais carregaram); `PipelineSpec.post_dbt_sinks: Vec<NodeSpec>` são os destinos finais desse resultado. Fluxo: extrai → carrega bruto em `sinks` (staging) → `dbt run/build/test` transforma no warehouse → `runner::run_post_dbt_stage` lê `dbt.output` de volta (via `build_source` + `PipelineEngine::drain_sources`) e grava em `post_dbt_sinks` (via `build_sink` + `PipelineEngine::fan_out_write`), tudo num `POST /pipelines/{id}/run` só. `post_dbt_sinks` só é válido com `dbt.output` setado (`PipelineSpec::validate`); ambos passam pelo mesmo scanner de SSRF/path-traversal dos `sources`/`sinks` normais (`validate_security_with`). Checkpoints desse estágio usam prefixo `post_dbt_` pra não colidir com os nomes resolvidos (`sink0`, `sink1`, ...) do estágio de carga bruta. Sem `dbt.output`, o comportamento é idêntico ao ELT original (dbt como passo terminal). Canvas (edição visual do node dbt com handle de saída) ainda não implementado — configuração via API/JSON só.

## 14. Backend Postgres pros metadados, leader election e migração

- **`nexus_server::db::MetadataPool`**: enum `Sqlite(SqlitePool) | Postgres(PgPool)` — não `sqlx::Any` (precisamos do `PgPool` nativo pro advisory lock abaixo, e nenhum dos 3 stores usa macro `query!`/`query_as!`, só `sqlx::query`/`query_as` dinâmico, então não há tipagem em tempo de compilação a perder). `MetadataPool::connect(url)` detecta o backend pelo scheme (`sqlite://` vs `postgres://`/`postgresql://`) — `NEXUS_CHECKPOINT_DB`/`NEXUS_AUTH_DB`/`NEXUS_PIPELINES_DB` continuam `String` cru, sem mudança de forma em `ServerConfig`. Cada um dos 3 stores (`auth_store`, `pipeline_store`, `checkpoint_store`) escreve toda query com `?` (estilo SQLite) e usa `db::rewrite_placeholders` pra virar `$1, $2, ...` em runtime quando o backend é Postgres — sqlx 0.9 exige `sqlx::AssertSqlSafe(...)` em volta de qualquer SQL não-`&'static str` (guarda anti-injeção nova da versão), então cada método tem um `match &self.pool { Sqlite(p) => ..., Postgres(p) => ... }` que só duplica a chamada `.execute(p)`/`.fetch_one(p)`, não a lógica.
- **Diferenças reais de dialeto tratadas** (não hipotéticas — cada uma quebrou um teste até ser corrigida): `last_insert_rowid()` (SQLite) vira `INSERT ... RETURNING id` + `query_scalar` (Postgres) em `PipelineStore::start_run`; `offset` é palavra reservada no Postgres (`checkpoint_store`'s DDL/upsert citam `"offset"`); `datetime('now')` (SQLite) vira `to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')` (Postgres) em todo `DEFAULT`/`UPDATE` — deliberadamente formatado igual ao SQLite (`"YYYY-MM-DD HH:MM:SS"`), não `TIMESTAMPTZ`, porque `scheduler.rs`'s `parse_stored_datetime` (usado pra calcular a próxima disparada de um cron) faz parse assumindo esse formato exato, independente do backend.
- **Leader election** (`scheduler.rs`): com >1 réplica compartilhando o mesmo Postgres, cada uma dispararia os mesmos pipelines agendados — `pg_try_advisory_lock` (chave fixa `SCHEDULER_LOCK_KEY`) resolve isso sem infra nova (sem etcd/Redis). Uma conexão dedicada (não emprestada do pool geral) é mantida viva entre ticks — o lock é por sessão, então só persiste enquanto essa conexão existe; a réplica cair (crash, rede) fecha a conexão e libera o lock automaticamente pra outra réplica assumir no próximo tick. No-op em SQLite (`MetadataPool::as_postgres()` retorna `None`) — sem cenário de múltiplas réplicas seguro pra SQLite de qualquer forma.
- **Migração** (`nexus_server::migrate`, binário `migrate-metadata`): copia as 3 tabelas de metadados de SQLite pra Postgres preservando IDs originais (`audit_log.id`, `pipeline_runs.id`) via insert explícito + `setval(pg_get_serial_sequence(...))` no final, pra não colidir com o próximo insert real. `spec_ciphertext` é copiado byte a byte (não re-criptografado) — o servidor de destino precisa da mesma `NEXUS_ENCRYPTION_KEY`. Idempotente via `ON CONFLICT ... DO NOTHING` em cada insert.
- **Deployment**: manifests de referência em `packaging/kubernetes/` (Deployment 2 réplicas, Service, PVC opcional pro cache de embeddings, HPA, ConfigMap/Secret) e stack file de Docker Swarm em `packaging/swarm/` — não são Helm chart nem testados num cluster gerenciado real, validados offline (`kubeconform`/`docker compose config`). Ver o `README.md` de cada um pros trade-offs (HPA escalando pra baixo mata runs em voo daquele pod — recuperável via checkpoint, não é perda de dado; Swarm não tem convenção de secret-via-arquivo, usa env var).

## 15. Logs de execução por run

Complementa o `ProgressEvent` numérico (rows/bytes por partição, §4) com uma
narração textual do que o run está fazendo — motivado por um caso que o
WebSocket puro nunca cobriu: um run disparado pelo scheduler (§12) não tem
ninguém com o socket aberto pra assistir, e `ProgressHub::finish` remove o
canal assim que o run termina, sem replay.

- **`nexus_server::progress::RunLogEvent`** (`{ ts, level: Info|Warn|Error,
  message }`) é emitido via `RunLogger` (também em `progress.rs`), que faz
  as duas coisas na emissão — não numa depois: broadcast pro canal ao vivo
  (best-effort, pode não ter subscriber) *e* persistência em
  `RunLogStore` (`run_log_store.rs`, mesmo padrão dual-dialeto do
  `MetadataPool` que `checkpoint_store.rs` já usa, tabela nova
  `pipeline_run_logs`). A persistência acontece na emissão, não no loop que
  repassa pro WebSocket (`forward_progress`) — esse loop roda uma vez por
  conexão, então persistir ali duplicaria a linha por subscriber conectado.
- **Canais**: `ProgressHub` (antes só `ProgressSender`) agora guarda um par
  `(ProgressSender, LogSender)` por `run_id` — dois `broadcast::channel`
  independentes, não um enum unificado. Decisão deliberada: unificar exigiria
  mudar o tipo público `nexus_core::ProgressEvent`/`ProgressSender`, usado
  em ~10 testes de `nexus-core::pipeline` e no hot path de execução; manter
  os dois canais separados deixa `nexus-core` (que não deve fazer I/O nem
  saber de logging — regra do crate) completamente intocado.
- **Wire format**: o frame de log leva um `"type": "log"` explícito
  (`forward_progress` adiciona essa chave manualmente só nesse frame) pra o
  frontend discriminar sem heurística — os frames de `ProgressEvent`/
  `hardware_stats` continuam sem tag, formato inalterado (evita quebrar
  qualquer consumidor existente do WebSocket).
- **Onde as linhas são emitidas**: `execute_pipeline_run` (lib.rs) loga uma
  mensagem de início estruturada (pipeline id, run id, modo e conectores),
  cada etapa do dbt e o resumo final (linhas/partições ou erro sanitizado —
  mesmo `error::sanitize_error` que já protege `pipeline_runs.error`/alertas,
  texto idêntico). `runner.rs` loga contagem de partições/sources/sinks,
  falhas de connect por partição/source/sink (`log_on_err`) e, via
  `log_progress()`, marcos percentuais a cada 10% (10%, 20%, ..., 100%) à
  medida que partições/sinks reportam `done` — todos persistidos no
  `RunLogStore` e repassados ao WebSocket ao vivo.
- **`GET /pipelines/{id}/runs/{run_id}/logs`** (role `Read`, mesmo nível de
  `GET .../runs`) devolve o histórico completo persistido — funciona pra um
  run em andamento, terminado, ou disparado pelo scheduler sem ninguém
  olhando. Sem paginação (volume esperado é dezenas de linhas por run, não
  milhares); sem retenção/cleanup ainda, mesmo estado de `pipeline_runs`/
  `checkpoints`.
- **Canvas**: `ExecutionPanel` ganhou um modo "terminal" (toggle no header,
  `LogTerminal.tsx` compartilhado) alimentado pelos frames `type: "log"` do
  mesmo WebSocket que já mantinha aberto pro progresso — sem socket novo.
  `RunHistoryPanel` reusa o mesmo `LogTerminal` por run, mas alimentado por
  `GET .../logs` (`useRunLogs`, fetch sob demanda) em vez do WebSocket — é
  o caminho que cobre um run já terminado ou agendado.

## 16. CDC nativo pra Delta Lake, Iceberg e AI-Lake (lakehouse)

Extensão do §7 pros formatos de data lake que já tinham conector batch —
Delta Lake e Iceberg têm changelog/manifest versionado nativo, e AI-Lake
(`nexus-connector-ailake`) é Iceberg-compatible por baixo (`ailake-catalog`,
implementação própria, não usa o crate `iceberg`). **Diferença
arquitetural importante em relação ao §7**: os 3 são inerentemente
**poll/batch**, não streaming — cada `read_batches` lê "o que mudou desde a
versão/snapshot X" e termina, sem conexão viva. Encaixa direto no modelo de
pipeline agendado (cron) já existente, sem infra nova. Resume entre
execuções segue o mesmo precedente do `start_offsets` do Kafka: campo
estático no config (`starting_version`/`starting_snapshot_id`), não
auto-avançado por checkpoint entre runs — omitido, lê a tabela inteira
desde a criação (seguro por causa do upsert idempotente do sink de
destino, só desperdiça trabalho numa tabela grande).

- **Delta Lake** (`deltalake-cdc`, feature `cdc` do `nexus-connector-deltalake`,
  sem dependência nova — `datafusion` já ligado pro sink): usa o **Change
  Data Feed nativo** (`DeltaTable::scan_cdf()`), coluna `_change_type`
  (`insert`/`update_postimage`/`delete`, `update_preimage` descartado)
  mapeada direto pro `__opcode`. Pré-requisito: `delta.enableChangeDataFeed
  = 'true'` na tabela (property, via `TBLPROPERTIES` ou `ALTER TABLE`).
  **Pegadinha real**: DataFusion não garante que a ordem dos batches
  retornados bata com a ordem de commit — sem ordenar explicitamente por
  `_commit_version` antes de processar, um delete pode aparecer antes do
  insert que ele deveria sobrescrever. `read_batches` ordena a
  `DataFrame` por essa coluna antes de coletar.
- **Iceberg** (`iceberg-cdc`, feature `cdc` do `nexus-connector-iceberg`,
  sem dependência nova): `iceberg` 0.10.0 **não tem scan incremental
  nativo** — construído à mão andando `TableMetadata::snapshots()` (via
  `parent_snapshot_id`), lendo `ManifestList`/`Manifest` (`iceberg::spec`,
  tudo API pública) e filtrando `ManifestEntry`s com `status() ==
  Added` + `content_type() == Data`. **Insert-only**: `IcebergSink` só
  comita `fast_append` (o `Transaction` API do `iceberg` 0.10.0 não tem
  ação de row-delta/equality-delete commitável ainda — ver `sink.rs`), então
  não existe `Overwrite`/`Delete` escrito por este sistema pra detectar.
  **Pegadinha real**: o manifest-list de um snapshot relista TODOS os
  manifests ainda vivos, não só os que ele introduziu — sem filtrar por
  `ManifestFile::added_snapshot_id == snapshot.snapshot_id()`, as mesmas
  linhas `Added` de snapshots antigos são reprocessadas em cada snapshot
  posterior que ainda os referencia, duplicando linhas.
- **AI-Lake** (`ailake-cdc`, feature `cdc` do `nexus-connector-ailake`, sem
  dependência nova): mais simples que o Iceberg porque `CatalogProvider`
  (`ailake-catalog`) já expõe `list_files`/`list_equality_deletes` com um
  parâmetro **"as of snapshot"** — basta diferenciar a lista "as of
  `starting_snapshot_id`" contra a lista "as of atual" (por `path`) pra
  achar exatamente o que mudou, sem andar manifest por manifest como no
  Iceberg puro. `AilakeSink::delete` comita equality-deletes reais, então
  emite `D` de verdade. Desde `ailake-catalog`/`ailake-query` 0.1.11
  (equality delete escopado por sequence number — só mascara arquivo com
  sequence number estritamente menor que o do próprio delete, spec
  Iceberg), o CDC usa `DataFileEntry::sequence_number`/
  `EqualityDeleteFile::sequence_number` pra decidir se uma linha marcada
  numa mesma janela como inserida E deletada está de fato mascarada agora
  (delete com sequence maior) ou sobrevive (delete com sequence menor —
  ex.: o próprio `upsert()` comita seu delete um passo antes do append).
  **`U` ainda não é inferido**: distinguir uma atualização real (chave já
  existia antes da janela) de um insert genuíno exigiria checar liveness
  da chave no snapshot de partida, não só diferençar o que mudou — as duas
  seguem marcadas como `I` por ora.
- **`AilakeSink::upsert` (batch sem `__opcode`) tem semântica de upsert real
  desde a correção do bug de escopo do equality-delete**: antes de cada
  append, comita um `delete_where` pra chave do batch (dois commits
  sequenciais — delete primeiro, depois o append). Como o delete tem
  sequence number menor que o append que vem depois, nunca mascara a
  própria linha nova (mesmo mecanismo delete-then-insert que o
  `DeltaSink` já usa); qualquer linha anterior com a mesma chave (de um
  commit ainda mais antigo) fica mascarada normalmente. Seguro mesmo pra
  chave nunca vista — um equality-delete pra valor inexistente é inócuo.

Os 3 produzem `RecordBatch` com `__opcode` na mesma convenção do §5/§7 —
mesmo `nexus_core::split_by_opcode` agnóstico à origem.
