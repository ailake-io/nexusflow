# Guia de uso do NexusFlow — instalação a conector por conector

Referência completa e prática: da instalação até a configuração exata de cada um dos 17 conectores, transformações (SQL, embeddings, dbt) e recursos de execução (preview, agendamento). Para o passo a passo mínimo de "primeiro pipeline", ver [`GETTING_STARTED.md`](./GETTING_STARTED.md); para arquitetura interna, [`ARCHITECTURE.md`](../ARCHITECTURE.md).

## Índice

1. [Instalação](#1-instalação)
2. [Autenticação e papéis (RBAC)](#2-autenticação-e-papéis-rbac)
3. [Anatomia de um pipeline](#3-anatomia-de-um-pipeline)
4. [Conectores — referência completa](#4-conectores--referência-completa)
5. [Transformação SQL (DataFusion)](#5-transformação-sql-datafusion)
6. [Embeddings (chunking + vetorização)](#6-embeddings-chunking--vetorização)
7. [dbt — ELT e ETL real](#7-dbt--elt-e-etl-real)
8. [Preview de dados](#8-preview-de-dados)
9. [Agendamento automático](#9-agendamento-automático)
10. [Observabilidade](#10-observabilidade)

---

## 1. Instalação

Ver [`GETTING_STARTED.md` §1](./GETTING_STARTED.md#1-instalação) para as opções completas (Docker, script `curl | sh`, pacotes `.deb`/AppImage/rpm, build from source). Resumo rápido — todas sobem o mesmo binário, um único processo servindo API REST + WebSocket + UI web em `http://localhost:8080`:

```bash
docker run -d --name nexusflow -p 8080:8080 \
  -e NEXUS_JWT_SECRET="$(openssl rand -hex 32)" \
  -e NEXUS_ENCRYPTION_KEY="$(openssl rand -hex 32)" \
  -e NEXUS_ADMIN_USERNAME=admin \
  -e NEXUS_ADMIN_PASSWORD="troque-isto" \
  nexusflow
```

Variáveis de ambiente completas: ver [`GETTING_STARTED.md` §3](./GETTING_STARTED.md#3-variáveis-de-ambiente). As duas obrigatórias são `NEXUS_JWT_SECRET` e `NEXUS_ENCRYPTION_KEY` — sem elas o processo não sobe.

**Conectores linkados no binário**: binários pré-buildados (release, `.deb`, AppImage, rpm, imagem Docker `:full`) já vêm com os 17 conectores ligados. Buildando a partir do source, cada conector é uma feature Cargo opcional (`cargo build --features embed-ui,connectors-all` liga todos de uma vez) — ver [`GETTING_STARTED.md` §2](./GETTING_STARTED.md#2-habilitando-conectores). O catálogo em `GET /connectors` sempre reflete exatamente o que foi compilado; a UI nunca mostra um conector que não está no binário.

---

## 2. Autenticação e papéis (RBAC)

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/auth/login \
  -H 'content-type: application/json' \
  -d '{"username":"admin","password":"troque-isto"}' | jq -r .token)
```

Todo request subsequente leva `-H "authorization: Bearer $TOKEN"`. Quatro papéis hierárquicos (`Read < Execute < Write < Admin`):

| Papel | Pode fazer |
|---|---|
| `Read` | Ver catálogo de conectores, listar pipelines, ver histórico de execuções |
| `Execute` | Tudo de `Read` + rodar pipeline (`POST /run`) e usar preview (`GET /preview`) |
| `Write` | Tudo de `Execute` + criar/editar/deletar pipeline, recarregar spec completo (`GET /spec`, inclui segredos) |
| `Admin` | Tudo de `Write` + gestão de usuários (`GET/POST/DELETE /users`, `PUT /users/{u}/role`) |

Gestão de usuários (só `Admin`):
```bash
curl -s -X POST http://localhost:8080/users -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"username":"analista","password":"...","role":"execute"}'
```

---

## 3. Anatomia de um pipeline

Um `PipelineSpec` (JSON) tem esta forma:

```jsonc
{
  "pipeline_id": "meu-pipeline",       // obrigatório, único
  "sources": [ /* NodeSpec[] */ ],     // obrigatório, não vazio
  "transform": { "sql": "..." },       // opcional
  "sinks": [ /* NodeSpec[] */ ],       // obrigatório, não vazio
  "embedding": { /* ... */ },          // opcional — ver §6
  "channel_capacity": 100,             // opcional, default 100
  "partitions": 1,                     // opcional, default 1
  "dbt": { /* ... */ },                // opcional — ver §7
  "post_dbt_sinks": [ /* NodeSpec[] */ ], // opcional, default [] — só válido com dbt.output setado
  "schedule": "0 */6 * * *"            // opcional — cron, ver §9
}
```

Cada `NodeSpec` (fonte, destino ou saída do dbt) tem o formato:

```jsonc
{
  "name": "minha_tabela",   // opcional — nome de referência na SQL do transform; default "source{N}"/"sink{N}"
  "connector": "postgres",  // obrigatório — string exata da tabela de conectores abaixo
  "config": { /* ... */ }   // config específica do conector — ver §4
}
```

**Duas formas de DAG válidas:**

- **Sem `transform`**: estritamente linear, exatamente **1 source e 1 sink**, execução particionada por chave primária (retomável por partição). Hoje só `postgres → postgres` é suportado nesse caminho.
- **Com `transform`**: `N sources → 1 transform SQL → M sinks` (fan-in/fan-out), qualquer combinação de conectores, execução não particionada (lê tudo, aplica SQL, escreve em todos os sinks). **Use transform sempre que o source e o sink forem de tipos diferentes.**

Exemplo cross-connector mínimo (postgres → sqlite, exige transform mesmo sendo um "passthrough"):

```json
{
  "pipeline_id": "pg-para-sqlite",
  "sources": [{"connector": "postgres", "config": {"uri": "postgresql://user:pass@host/db", "table": "events", "primary_key": "id"}}],
  "transform": {"sql": "SELECT * FROM source0"},
  "sinks": [{"connector": "sqlite", "config": {"uri": "/tmp/out.db", "table": "events", "primary_key": "id"}}]
}
```

Criar/atualizar/rodar via API:

```bash
curl -s -X POST http://localhost:8080/pipelines -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' -d @spec.json
curl -s -X PUT  http://localhost:8080/pipelines/meu-pipeline -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' -d @spec.json
curl -s -X POST http://localhost:8080/pipelines/meu-pipeline/run -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{"pipeline_id":"meu-pipeline"}'
# 202 Accepted + {"run_id": N} — acompanhe em GET /pipelines/{id}/runs ou no WebSocket /pipelines/{id}/runs/{run_id}/progress
```

---

## 4. Conectores — referência completa

Todos os campos abaixo são os nomes **exatos** dos structs de config em Rust (`serde` sem `rename`, exceto onde indicado). Campos sem `default` são obrigatórios. Convenção comum: quase todo conector de rede tem `timeout_seconds` (default `30`); conectores "bridging" sem schema próprio (mongodb, kafka, rest, odbc, csv) exigem `fields: [{"name", "data_type", "nullable"}]` explícito, com `data_type` em `int64|float64|boolean|utf8`.

### 4.1 Fast-path ADBC (nativo, binário, sem overhead de serialização)

#### `postgres` — source + sink
```json
{"connector": "postgres", "config": {
  "uri": "postgresql://user:pass@host:5432/db",
  "table": "events",
  "primary_key": "id",
  "timeout_seconds": 30
}}
```
Requer `ADBC_DRIVER_POSTGRESQL_PATH` apontando pro `.so` (build com `scripts/build-adbc-postgresql-driver.sh`). `primary_key` deve ser coluna indexada/ordenável (int/UUID/timestamp) — usada tanto pra particionar leitura quanto pra upsert no sink.

#### `sqlite` — source + sink
```json
{"connector": "sqlite", "config": {
  "uri": "/caminho/para/arquivo.db",
  "table": "events",
  "primary_key": "id",
  "timeout_seconds": 30
}}
```
Requer `ADBC_DRIVER_SQLITE_PATH`. `uri` aceita `:memory:`. Tabela criada automaticamente no sink se não existir.

### 4.2 NoSQL e filas (bridging — convertidos para Arrow via schema explícito)

#### `mongodb` — source + sink
```json
{"connector": "mongodb", "config": {
  "uri": "mongodb://user:pass@host:27017",
  "database": "meudb",
  "collection": "events",
  "primary_key": "_id",
  "fields": [
    {"name": "_id", "data_type": "utf8"},
    {"name": "amount", "data_type": "float64", "nullable": true},
    {"name": "address.city", "data_type": "utf8", "nullable": true}
  ],
  "batch_size": 1000,
  "timeout_seconds": 30
}}
```
`fields[].name` aceita dot-notation pra campos aninhados (ex. `"address.city"`). MongoDB não tem schema fixo — por isso o schema é sempre explícito aqui.

#### `kafka` — **source apenas** (feature `kafka` + `nexus-connector-kafka/consumer`, dependência nativa `librdkafka`)
```json
{"connector": "kafka", "config": {
  "bootstrap_servers": "broker1:9092,broker2:9092",
  "topic": "events",
  "group_id": "nexusflow-consumer",
  "fields": [{"name": "id", "data_type": "int64"}, {"name": "payload", "data_type": "utf8"}],
  "batch_size": 500,
  "poll_timeout_ms": 2000,
  "max_messages": 100000,
  "envelope": "raw",
  "start_offsets": {}
}}
```
`envelope`: `"raw"` (default, payload é a própria row em JSON) ou `"debezium"` (evento de CDC — o `topic` vira o tópico do Kafka Connect, ex. `"{server}.{schema}.{table}"`, não o nome da tabela; a coluna `__opcode` é adicionada automaticamente com `I`/`U`/`D`). Offsets são commitados manualmente ao final de cada leitura, alinhado com o checkpoint do pipeline (`enable.auto.commit` desligado).

### 4.3 APIs REST/SaaS (bridging genérico)

#### `rest` — **source apenas**
```json
{"connector": "rest", "config": {
  "base_url": "https://api.example.com",
  "path": "/v1/items",
  "headers": {"Authorization": "Bearer TOKEN"},
  "fields": [{"name": "id", "data_type": "int64"}, {"name": "name", "data_type": "utf8"}],
  "rows_path": "data.items",
  "pagination": {"type": "offset", "offset_param": "offset", "limit_param": "limit", "limit": 100},
  "max_pages": 1000,
  "timeout_seconds": 30,
  "retries": 3,
  "retry_backoff_seconds": 1,
  "requests_per_second": 0
}}
```
`rows_path` (opcional): caminho dot-separated até o array de linhas dentro do corpo da resposta (`null`/omitido = o corpo inteiro é o array). `pagination.type`: `"none"` (default, 1 request), `"offset"` (para quando a página voltar com menos que `limit` linhas) ou `"cursor"` (`{"type":"cursor","cursor_param":"cursor","next_cursor_path":"meta.next_cursor"}` — para quando o próximo cursor vier ausente/`null`).

#### `webhook` — **sink apenas** (mesmo crate do `rest`, nome de conector diferente porque a forma da config é outra)
```json
{"connector": "webhook", "config": {
  "url": "https://api.example.com/v1/events",
  "method": "POST",
  "headers": {"Authorization": "Bearer TOKEN"},
  "body_mode": "per_row",
  "timeout_seconds": 30,
  "retries": 3,
  "retry_backoff_seconds": 1,
  "requests_per_second": 10
}}
```
`method`: `POST`/`PUT`/`PATCH`/`DELETE`. `body_mode`: `"array"` (default, 1 request por batch com todas as linhas) ou `"per_row"` (1 request por linha — `requests_per_second` limita a taxa nesse modo).

### 4.4 Legado (ODBC/JDBC)

#### `odbc` — source + sink (feature `odbc` + `nexus-connector-odbc/legacy`, unixODBC vendorizado)
```json
{"connector": "odbc", "config": {
  "connection_string": "Driver={PostgreSQL Unicode};Server=host;Port=5432;Database=db;Uid=user;Pwd=pass;",
  "table": "events",
  "primary_key": "id",
  "fields": [{"name": "id", "data_type": "int64"}, {"name": "name", "data_type": "utf8"}],
  "batch_size": 1000,
  "timeout_seconds": 30
}}
```
`fields` é explícito porque introspecção de schema não é confiável entre drivers ODBC diferentes.

### 4.5 Bancos vetoriais / AI Lakehouse (todos **sink apenas**, exceto `lancedb`/`ailake` que também são source — ver §4.6)

Todos compartilham o mesmo formato: `primary_key`, `embedding_column` (coluna `FixedSizeList<Float32>`), `dimension`. A coleção/tabela/índice de destino **deve já existir** com a dimensão correta configurada externamente — o sink só escreve.

#### `milvus`
```json
{"connector": "milvus", "config": {
  "url": "http://localhost:19530", "collection": "docs",
  "primary_key": "id", "embedding_column": "embedding", "dimension": 384, "timeout_seconds": 30
}}
```
`primary_key` deve ser coluna `Int64` na coleção (VarChar PK não suportado).

#### `qdrant`
```json
{"connector": "qdrant", "config": {
  "url": "http://localhost:6334", "collection": "docs",
  "primary_key": "id", "embedding_column": "embedding", "dimension": 384, "timeout_seconds": 30
}}
```
Porta é a gRPC (6334, não a REST 6333). `primary_key` deve ser inteiro sem sinal ou UUID (point ID do Qdrant) — string arbitrária não é suportada.

#### `pgvector`
```json
{"connector": "pgvector", "config": {
  "uri": "postgresql://user:pass@host/db", "table": "docs",
  "primary_key": "id", "embedding_column": "embedding", "dimension": 384, "timeout_seconds": 30
}}
```
Requer `CREATE EXTENSION vector` já rodado no banco e a tabela já existir com coluna `vector(384)` (dimensão precisa bater com `dimension`).

#### `pinecone`
```json
{"connector": "pinecone", "config": {
  "host": "https://meu-indice-xxxx.svc.us-east1-aws.pinecone.io",
  "api_key": "...", "primary_key": "id", "embedding_column": "embedding", "dimension": 384,
  "namespace": "producao", "timeout_seconds": 30
}}
```
`host` vem do `describe_index` do índice — **não** é construído a partir de `index`+`environment` (esquema antigo, depreciado). `namespace` é opcional (omitido = namespace default sem nome). Serviço gerenciado — sem opção self-hosted.

#### `chromadb`
```json
{"connector": "chromadb", "config": {
  "host": "http://localhost:8000", "tenant": "default_tenant", "database": "default_database",
  "collection": "docs", "primary_key": "id", "embedding_column": "embedding", "dimension": 384, "timeout_seconds": 30
}}
```
Fala com a API REST v2 do Chroma (`/api/v1` está deprecado). `tenant`/`database` têm default e normalmente não precisam ser setados.

### 4.6 Data Lake formats (source + sink, embarcados — sem servidor externo)

#### `deltalake`
```json
{"connector": "deltalake", "config": {
  "table_uri": "/dados/delta/events", "primary_key": "id", "timeout_seconds": 30
}}
```
`table_uri` aceita path local ou `file://`; criado automaticamente no primeiro write.

#### `iceberg`
```json
{"connector": "iceberg", "config": {
  "catalog_uri": "sqlite:///dados/iceberg/catalog.db?mode=rwc",
  "warehouse_location": "file:///dados/iceberg/warehouse",
  "namespace": "meu_namespace", "table": "events",
  "format_version": "v2", "timeout_seconds": 30
}}
```
Totalmente embarcado (catálogo SQLite + warehouse local, sem metastore externo). `format_version` (`"v2"` default ou `"v3"`) só afeta tabela nova. **Limitação conhecida**: sink é *append-only* na versão atual do `iceberg-rust` — reexecuções duplicam linhas até o crate ganhar suporte a *equality-delete*; deletes de CDC são rejeitados com erro explícito.

#### `parquet`
```json
{"connector": "parquet", "config": {
  "path": "/dados/events.parquet", "primary_key": "id"
}}
```
Config mais simples de todas (sem `timeout_seconds` — é I/O de arquivo local, não rede). Upsert/delete são feitos via read-filter-rewrite (arquivo temporário + rename atômico) — correto, mas custo `O(tamanho do arquivo)` por batch.

#### `ailake` — formato Parquet + índice HNSW nativo para vetores
```json
{"connector": "ailake", "config": {
  "warehouse": "/dados/ailake", "namespace": "meu_namespace", "table": "docs",
  "primary_key": "id", "embedding_column": "embedding", "dimension": 384, "timeout_seconds": 30
}}
```
Único data-lake format que também é destino vetorial nativo (coluna de embedding é indexada com HNSW automaticamente) — por isso é source **e** sink, ao contrário dos outros 6 conectores vetoriais. Atenção: `dimension` aqui é `u32`, mas isso não muda a sintaxe JSON.

### 4.7 Arquivos delimitados + armazenamento em nuvem

#### `csv` — source + sink, local ou nuvem
```json
{"connector": "csv", "config": {
  "uri": "/dados/events.csv",
  "delimiter": ",",
  "has_header": true,
  "fields": [{"name": "id", "data_type": "int64"}, {"name": "name", "data_type": "utf8", "nullable": true}],
  "primary_key": "id",
  "batch_size": 1000,
  "timeout_seconds": 30
}}
```
- `delimiter`: qualquer caractere único — `,` (CSV, default), `\t` (TSV), `;`/`|` (TXT customizado).
- `primary_key`: opcional na source, mas **obrigatório de fato no sink** (usado pra upsert/delete).
- `uri` aceita path local **ou** URL de nuvem — `s3://bucket/key`, `gs://bucket/key`, `az://container/key`:

```json
{"connector": "csv", "config": {
  "uri": "s3://meu-bucket/events.csv",
  "fields": [{"name": "id", "data_type": "int64"}, {"name": "name", "data_type": "utf8"}],
  "primary_key": "id",
  "storage_options": {
    "aws_access_key_id": "...", "aws_secret_access_key": "...", "aws_region": "us-east-1"
  }
}}
```
Chaves de `storage_options` por provedor:
| Nuvem | Chaves |
|---|---|
| `s3://` | `aws_access_key_id`, `aws_secret_access_key`, `aws_region` |
| `gs://` | `google_service_account` ou `google_service_account_key` |
| `az://` | `azure_storage_account_name`, `azure_storage_account_key` |

`storage_options` é ignorado para paths locais.

### 4.8 Tabela-resumo

| Conector | Source | Sink | Notas |
|---|:---:|:---:|---|
| `postgres` | ✅ | ✅ | ADBC nativo |
| `sqlite` | ✅ | ✅ | ADBC nativo |
| `mongodb` | ✅ | ✅ | schema explícito |
| `kafka` | ✅ | — | só leitura, CDC via Debezium |
| `rest` | ✅ | — | genérico, paginação offset/cursor |
| `webhook` | — | ✅ | mesmo crate do `rest` |
| `odbc` | ✅ | ✅ | legado, driver nativo |
| `milvus` | — | ✅ | vetorial |
| `qdrant` | — | ✅ | vetorial |
| `lancedb` | — | ✅ | vetorial, embarcado |
| `pgvector` | — | ✅ | vetorial, sobre Postgres |
| `pinecone` | — | ✅ | vetorial, serviço gerenciado |
| `chromadb` | — | ✅ | vetorial |
| `deltalake` | ✅ | ✅ | data lake |
| `iceberg` | ✅ | ✅ | data lake, append-only |
| `parquet` | ✅ | ✅ | data lake, arquivo único |
| `ailake` | ✅ | ✅ | data lake + vetorial (HNSW) |
| `csv` | ✅ | ✅ | local ou S3/GCS/Azure |

> `lancedb` não tem config listada acima por brevidade — segue o mesmo padrão vetorial de `pgvector`/`milvus`: `{"uri": "/dados/vectors", "table": "docs", "primary_key": "id", "embedding_column": "embedding", "dimension": 384, "timeout_seconds": 30}`, path local embarcado (sem servidor), criado automaticamente no primeiro write.

---

## 5. Transformação SQL (DataFusion)

Um node `transform` roda **uma** query SQL sobre todas as `sources`, usando o `name` (ou `source{N}` default) de cada uma como nome de tabela:

```json
{
  "sources": [
    {"name": "eventos", "connector": "postgres", "config": {...}},
    {"name": "regioes", "connector": "postgres", "config": {...}}
  ],
  "transform": {"sql": "SELECT e.*, r.nome_regiao FROM eventos e JOIN regioes r ON e.regiao_id = r.id"},
  "sinks": [{"connector": "sqlite", "config": {...}}]
}
```

O resultado é escrito em **todos** os `sinks` (fan-out) — mesma tabela final pra todos, colunas vindas do schema de saída da query. Motor: Apache DataFusion, em memória, sem particionamento (lê tudo, transforma, escreve).

---

## 6. Embeddings (chunking + vetorização)

Node `embedding` opcional, roda **antes** do transform (ou antes dos sinks, se não houver transform), expande cada linha da source em chunks e anexa uma coluna `FixedSizeList<Float32>` com os vetores. Dois backends de modelo e duas estratégias de chunking, combináveis livremente:

```jsonc
"embedding": {
  "source_column": "corpo",      // coluna de texto a embeddar
  "output_column": "embedding",  // nome da nova coluna de vetor
  "dimension": 384,              // deve bater com a saída do modelo
  "model": { /* Onnx ou Api, ver abaixo */ },
  "chunking": { /* FixedWindow ou RecursiveCharacter, ver abaixo */ }
}
```

**Backend Onnx** (local, CPU — feature `embeddings`):
```json
"model": {
  "backend": "onnx",
  "repo": "sentence-transformers/all-MiniLM-L6-v2",
  "filename": "model.onnx",
  "tokenizer_filename": "tokenizer.json",
  "max_length": 128
}
```
Baixa o modelo do Hugging Face Hub em runtime (cache local). CUDA/Metal ainda não validados em hardware real — hoje roda em CPU mesmo com essas features ligadas (fallback silencioso).

**Backend Api** (HTTP externa compatível com OpenAI — feature `embeddings-api`):
```json
"model": {
  "backend": "api",
  "base_url": "https://api.openai.com/v1",
  "model": "text-embedding-3-small",
  "api_key_env": "OPENAI_API_KEY"
}
```
`api_key_env` é o **nome** da variável de ambiente com a chave (nunca a chave em si — segredos não vivem no JSON persistido do pipeline). `base_url` sem `/embeddings` no final.

**Chunking — `fixed_window`:**
```json
"chunking": {"strategy": "fixed_window", "chunk_size": 256, "overlap": 32}
```
**Chunking — `recursive_character`:**
```json
"chunking": {"strategy": "recursive_character", "chunk_size": 256, "overlap": 32, "separators": ["\n\n", "\n", " "]}
```

---

## 7. dbt — ELT e ETL real

Precisa do build com feature `dbt` e do CLI `dbt` (dbt-fusion) no `PATH` do processo — não instalado automaticamente.

**Modo ELT** (clássico): depois que os `sinks` terminam de carregar os dados brutos, roda `dbt run`/`build`/`test` no warehouse de destino:
```json
"dbt": {"project_dir": "meu_projeto_dbt", "command": "run", "select": null}
```
`project_dir` é relativo (não pode começar com `/` nem conter `..`) e resultado (models/tests, lineage) aparece no histórico da execução.

**Modo ETL real** (extensão — `dbt.output` + `post_dbt_sinks` no nível do `PipelineSpec`): o pipeline lê de volta o resultado transformado pelo dbt e grava num destino final, tudo no mesmo `run`:

```json
{
  "pipeline_id": "etl-com-dbt",
  "sources": [{"connector": "postgres", "config": {"uri": "...", "table": "raw_events", "primary_key": "id"}}],
  "sinks": [{"connector": "postgres", "config": {"uri": "...", "table": "staging_events", "primary_key": "id"}}],
  "dbt": {
    "project_dir": "meu_projeto_dbt",
    "command": "run",
    "output": {"connector": "postgres", "config": {"uri": "...", "table": "transformed_events", "primary_key": "id"}}
  },
  "post_dbt_sinks": [
    {"connector": "postgres", "config": {"uri": "...", "table": "final_events", "primary_key": "id"}}
  ]
}
```

Fluxo: `Source → carga bruta em staging_events → dbt transforma (model transformed_events) → dbt.output lê o model de volta → post_dbt_sinks grava em final_events`. `post_dbt_sinks` só é aceito se `dbt.output` estiver setado. Sem Canvas dedicado ainda — configuração via API/JSON.

---

## 8. Preview de dados

Antes de rodar o pipeline inteiro, dá pra espiar as primeiras linhas que um node source/sink vai ler (pipeline precisa já estar salvo — `POST`/`PUT /pipelines`):

```bash
curl -s "http://localhost:8080/pipelines/meu-pipeline/preview?node=source0&limit=20" \
  -H "authorization: Bearer $TOKEN"
```

`node` é o nome resolvido (`name` explícito do `NodeSpec`, ou `source{N}`/`sink{N}` default). `limit` é opcional (default 50, teto 500). Exige papel `Execute` (abre conexão real contra o conector, igual rodar o pipeline). Conectores sink-only (os 6 vetoriais + `webhook`) retornam erro 400 claro, já que não têm implementação de leitura.

---

## 9. Agendamento automático

Campo `schedule` no `PipelineSpec`, cron de 5 campos (Unix, `min hora dia mês dia-da-semana`) ou 6 campos (Quartz, com segundos):

```json
{"pipeline_id": "meu-pipeline", "sources": [...], "sinks": [...], "schedule": "0 */6 * * *"}
```

O scheduler faz *poll* a cada 30s e dispara via o mesmo caminho de execução do `POST /run` manual — histórico, alertas e dbt se comportam de forma idêntica entre run manual e agendado. Sem `schedule`, o pipeline só roda quando chamado explicitamente.

---

## 10. Observabilidade

- `GET /health` — liveness, sem auth.
- `GET /metrics` — Prometheus, sem auth (segurança via segmentação de rede).
- `GET /pipelines/{id}/runs` — histórico de execuções, inclui as disparadas pelo scheduler.
- WebSocket `/pipelines/{id}/runs/{run_id}/progress` — batches/linhas/bytes escritos por partição, em tempo real, mais um frame `{"hardware_stats": {...}}` intercalado a cada 2s com CPU/memória do processo.
- Alertas em falha de pipeline: Slack, MS Teams, PagerDuty, Email e webhook genérico — configurados via variáveis de ambiente (`NEXUS_SLACK_WEBHOOK_URL` etc., ver [`GETTING_STARTED.md` §3](./GETTING_STARTED.md#3-variáveis-de-ambiente)).
- Logs estruturados em JSON no stdout; `NEXUS_OTLP_ENDPOINT` exporta traces pra um coletor OTel.
