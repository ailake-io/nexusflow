# Plano de melhoria de performance e correção dos conectores

## Resumo executivo

Foram executados testes de carga de **100 mil linhas** em todos os conectores OSS que conseguimos levantar localmente sem conta de terceiro. A grande maioria passou. Os principais gargalos encontrados foram:

1. **ChromaDB** — funcionava, mas com carga sequencial de chunks (46 s para 100 k).
2. **ClickHouse** — falhava porque a imagem Docker não continha o driver ADBC do ClickHouse.
3. **Kafka/MQTT** — os testes iniciais falharam por configuração do ambiente de teste, não por bug do conector.

As correções para ChromaDB e ClickHouse já estão em andamento (commits no branch `fix/connectors-and-build-improvements`).

---

## Resultados dos testes 100 k (OSS, local)

| Direção | Conector | Status | Tempo aprox. | Observação |
|---|---|---|---|---|
| CSV → Postgres | postgres | ✅ sucesso | < 1 s | Já otimizado com batch buffer |
| CSV → MySQL | mysql | ✅ sucesso | < 1 s |  |
| CSV → MongoDB | mongodb | ✅ sucesso | ~5 s |  |
| Parquet → Postgres | postgres | ✅ sucesso | < 1 s |  |
| SQLite → Postgres | postgres | ✅ sucesso | < 1 s |  |
| Parquet → SQLite | sqlite | ✅ sucesso | ~4 s |  |
| Postgres → CSV | csv | ✅ sucesso | < 1 s |  |
| MySQL → CSV | csv | ✅ sucesso | ~5 s |  |
| MongoDB → CSV | csv | ✅ sucesso | ~5 s |  |
| Parquet → Qdrant | qdrant | ✅ sucesso | ~1,5 s | Upsert em batch único |
| Parquet → ChromaDB | chromadb | ✅ sucesso | ~60 s | 98 batches; ganho real exige aumentar chunk/batch size |
| Parquet → PGVector | pgvector | ✅ sucesso | ~2,5 s | COPY/batch |
| REST → CSV | rest | ✅ sucesso | ~1 s |  |
| CSV → Webhook | webhook | ✅ sucesso | ~1 s |  |
| Kafka → CSV | kafka | ✅ sucesso | ~9 s | Depois de recriar broker com RF=1 |
| MQTT → CSV | mqtt | ✅ sucesso | ~21 s | Publicação durante o run |
| CSV → ClickHouse | clickhouse | ✅ sucesso | < 1 s | Driver ADBC incluído; qualified table corrigido |
| ClickHouse → CSV | clickhouse | ✅ sucesso | ~2 s | Driver ADBC incluído; qualified table corrigido |
| CSV → Delta Lake | deltalake | ✅ sucesso | — | Já com CDC/batch buffer |
| CSV → Iceberg | iceberg | ✅ sucesso | — | Já com CDC/batch buffer |
| Parquet → AI-Lake | ailake | ✅ sucesso | — | Já com CDC/batch buffer |

---

## Melhorias já implementadas

### 1. ChromaDB — paralelização de chunks

- Arquivos alterados:
  - `crates/nexus-connectors/nexus-connector-chromadb/Cargo.toml`
  - `crates/nexus-connectors/nexus-connector-chromadb/src/config.rs`
  - `crates/nexus-connectors/nexus-connector-chromadb/src/sink.rs`
  - `crates/nexus-connectors/nexus-connector-chromadb/tests/chromadb_integration.rs`
- O que mudou:
  - Adicionada config `max_concurrent_requests` (default 8).
  - Chunks de upsert/delete agora são submetidos com `buffer_unordered`.
  - CDC split entre upserts e deletes preservado.
- Resultado observado: a paralelização interna de chunks manteve o tempo próximo do original (~44 s) porque cada batch do pipeline já tem ~1.020 registros (apenas ~2 chunks de 1.000). O ganho real virá de:
  - Aumentar `CHROMA_CHUNK_SIZE` e/ou acumular múltiplos batches antes de enviar.
  - Ajustar o batch size do source ou o pipeline engine para reduzir o número de batches.

### 2. ClickHouse — inclusão do driver ADBC na imagem Docker + correção de autenticação

- Arquivos alterados:
  - `scripts/build-adbc-clickhouse-driver.sh` (novo)
  - `Dockerfile`
  - `crates/nexus-connectors/nexus-connector-clickhouse/src/driver.rs`
  - `crates/nexus-connectors/nexus-connector-clickhouse/src/config.rs`
  - `crates/nexus-connectors/nexus-connector-clickhouse/src/source.rs`
  - `crates/nexus-connectors/nexus-connector-clickhouse/src/sink.rs`
  - `scratchpad/nexusflow-compose/docker-compose.yml`
- O que mudou:
  - Nova stage `clickhouse-adbc` no Dockerfile compilando `libadbc_clickhouse.so`.
  - Driver copiado para `/usr/lib/nexusflow/`.
  - Env `ADBC_DRIVER_CLICKHOUSE_PATH` apontando para a biblioteca no compose.
  - URI sem database no path (o driver não aceita path component).
  - Username/password passados como opções ADBC separadas em vez de userinfo na URI.
- Resultado esperado: CSV ↔ ClickHouse passa a funcionar.
- Resultado observado: CSV → ClickHouse 100 k em < 1 s; ClickHouse → CSV 100 k em ~2 s.

### 3. ClickHouse — qualified table

- Arquivos alterados:
  - `crates/nexus-connectors/nexus-connector-clickhouse/src/sink.rs`
  - `crates/nexus-connectors/nexus-connector-clickhouse/src/source.rs`
- O que mudou:
  - Todas as queries passam a usar `"database"."tabela"` qualificado.
  - O driver ADBC do ClickHouse não suporta `current_schema`/default database via opções de conexão, então sem isso as queries caíam no database `default`.

---

## Teste de regressão 1 M CSV → Postgres

| Direção | Conector | Status | Tempo | Observação |
|---|---|---|---|---|
| CSV → Postgres | postgres | ✅ sucesso | ~3 s | 1 000 000 linhas, 20 batches; sem regressão dos 71 min |

---

## Plano de melhorias restantes (OSS)

### Curto prazo (já pode ser feito)

| # | Conector | Problema / Oportunidade | Ação proposta |
|---|---|---|---|
| 1 | **ODBC** | Depende de driver nativo configurado no host; unixODBC é pesado | Documentar pré-requisitos e validar com SQLite/Postgres ODBC locais |
| 2 | **Milvus** | Usa gRPC; pode ter limitação de tamanho de batch | Testar batch grande e, se necessário, chunking paralelo similar ao ChromaDB |
| 3 | **LanceDB** | Embeddings local pesado; sem teste de carga | Validar com dataset 100 k usando conector local |
| 4 | **Kafka** | O teste requer broker com `offsets.topic.replication.factor=1` | Adicionar exemplo de `docker run` com RF=1 na documentação |
| 5 | **MQTT** | `__mqtt_topic` é adicionada ao schema e o sink CSV falha se omitida | Melhorar documentação do conector e considerar strip da coluna em sinks não-CDC |

### Médio prazo

| # | Conector | Ação proposta |
|---|---|---|
| 6 | **Postgres/MySQL/MongoDB** | Manter batching atual; adicionar testes de 1 M para garantir que o tempo escala linearmente |
| 7 | **CSV / Parquet / SQLite** | Gargalo geralmente em I/O de disco; manter, adicionar benchmark |
| 8 | **REST / Webhook** | Adicionar retry/backoff e pool de conexões compartilhado |

---

## Plano de melhorias para conectores Enterprise

Os conectores do repositório `nexus-connectors-enterprise` foram inspecionados por código. Abaixo estão as oportunidades de performance agrupadas por família.

### Vector / Search sinks

| Conector | Avaliação | Oportunidade de melhoria |
|---|---|---|
| **Weaviate** | Upsert em batch único (`/v1/batch/objects`) — OK | Delete é **um `DELETE` por ID**. Para CDC com muitos deletes, implementar batch delete com `where`/id-in |
| **Elasticsearch / OpenSearch** | Bulk API em batch único — OK | Nenhuma ação imediata |
| **Azure AI Search** | Index Documents em batch único — OK | Nenhuma ação imediata |
| **Vertex Vector Search** | `upsertDatapoints`/`removeDatapoints` em batch único — OK | Nenhuma ação imediata |

### Data warehouses

| Conector | Avaliação | Oportunidade de melhoria |
|---|---|---|
| **Snowflake** | MERGE com bind de batch inteiro via ADBC | Para cargas plain-insert grandes, adicionar fast-path usando `adbc.ingest.target_table` (bulk ingest nativo do driver) |
| **BigQuery** | A ser inspecionado em ambiente real | Verificar uso de streaming insert vs batch load jobs |
| **Redshift** | Usa ADBC; provavelmente similar a Postgres | Validar batch COPY via ADBC |
| **Databricks** | ADBC/ODBC provável | Testar bulk insert nativo |
| **Starburst** | ADBC/ODBC provável | Testar bulk insert nativo |

### Bancos enterprise

| Conector | Avaliação | Oportunidade de melhoria |
|---|---|---|
| **MSSQL** | ADBC? | Verificar se usa batch bind ou row-by-row |
| **Oracle** | ADBC? | Verificar bulk bind vs row-by-row |
| **HANA** | ADBC/ODBC | Verificar batch insert |

### SaaS / APIs

| Conector | Avaliação | Oportunidade de melhoria |
|---|---|---|
| **Salesforce** | Usa Bulk API 2.0 — adequado | Nenhuma ação imediata |
| **Stripe** | Paginação sequencial por API (limit 100) | Inerente à API; considerar paralelismo de páginas dentro dos rate limits |
| **Shopify** | Similar a Stripe | Paginação sequencial; considerar paralelismo controlado |
| **GA4 / YouTube Analytics** | APIs de analytics com cota baixa | Não há grande otimização possível além de cache e respeito a cotas |
| **Google Ads / Meta / LinkedIn / TikTok / X Ads** | APIs com rate limits agressivos | Foco em retry exponencial, cache de tokens e respeito a cotas |
| **Excel** | File sink | Não aplicável |

---

## Requisitos para testar cada conector Enterprise

| Família | Conector | Precisa de conta/infra? | O que é necessário para testar | Alternativa local/mock disponível? |
|---|---|---|---|---|
| Vector / Search | **Weaviate** | Não (self-hosted) | Container Docker `semitechnologies/weaviate` + collection criada | ✅ Sim — roda local |
| Vector / Search | **Elasticsearch** | Não (self-hosted) | Container Docker `elasticsearch:8.x` + índice criado | ✅ Sim — roda local |
| Vector / Search | **Azure AI Search** | Sim | Subscription Azure + serviço Azure AI Search + api key | ❌ Não |
| Vector / Search | **Vertex Vector Search** | Sim | Projeto GCP + service account + índice criado | ❌ Não |
| Data warehouse | **Snowflake** | Sim | Trial Snowflake (30 dias) + warehouse/database/schema + auth password ou key-pair | ❌ Não |
| Data warehouse | **BigQuery** | Sim | Projeto GCP + service account JSON + dataset/table | ✅ Sim — `ghcr.io/goccy/bigquery-emulator` ou `gcr.io/cloud-devrel-public-resources/bigquery-emulator` |
| Data warehouse | **Redshift** | Sim | AWS account + cluster Redshift + usuário/senha | ✅ Sim — LocalStack Pro (suporta Redshift via `localstack/localstack`) |
| Data warehouse | **Databricks** | Sim | Workspace Databricks + personal access token + cluster SQL warehouse | ❌ Não |
| Data warehouse | **Starburst** | Sim | Galaxy Starburst ou cluster Trino/Presto + usuário/senha | ✅ Sim — container `trinodb/trino` local (API compatível) |
| Bancos enterprise | **MSSQL** | Não (container) | Container Docker `mcr.microsoft.com/mssql/server` + banco + tabela | ✅ Sim — roda local |
| Bancos enterprise | **Oracle** | Não (container) | Container Docker `gvenzl/oracle-free` ou `gvenzl/oracle-xe` + tabela | ✅ Sim — roda local |
| Bancos enterprise | **HANA** | Sim | Imagem SAP HANA Express (requer registro SAP) ou instância cloud | ⚠️ Imagem Docker disponível com registro SAP |
| SaaS / APIs | **Salesforce** | Sim | Developer Edition (grátis) + connected app + certificado JWT | ⚠️ Conta grátis disponível |
| SaaS / APIs | **Stripe** | Sim | Conta Stripe + secret key (modo teste gratuito) | ❌ Não — mas modo teste é gratuito |
| SaaS / APIs | **Shopify** | Sim | Loja de desenvolvedor Shopify (grátis) + access token | ⚠️ Conta dev grátis disponível |
| SaaS / APIs | **GA4** | Sim | Propriedade GA4 + service account Google Cloud | ❌ Não |
| SaaS / APIs | **Google Ads** | Sim | Conta Google Ads + developer token + refresh token OAuth | ❌ Não |
| SaaS / APIs | **Meta Ads** | Sim | Conta Business Meta + access token Graph API | ❌ Não |
| SaaS / APIs | **LinkedIn Ads** | Sim | Conta LinkedIn Campaign Manager + access token | ❌ Não |
| SaaS / APIs | **TikTok Ads** | Sim | Conta TikTok for Business + access token + advertiser ID | ❌ Não |
| SaaS / APIs | **X Ads** | Sim | Conta X Developer + app OAuth + access token | ❌ Não |
| SaaS / APIs | **YouTube Analytics** | Sim | Canal YouTube + projeto GCP OAuth | ❌ Não |
| Streaming | **Kinesis** | Sim | AWS account + stream Kinesis + credenciais IAM | ✅ Sim — LocalStack `localstack/localstack` com Kinesis + credenciais dummy |
| Streaming | **Pulsar** | Não (self-hosted) | Container Docker `apachepulsar/pulsar` standalone + topic | ✅ Sim — roda local |
| File | **Excel** | Não | Arquivo `.xlsx` local ou em storage S3/GCS/Azure compatível | ✅ Sim — arquivo local |

### Conectores que já podem ser testados localmente sem conta

- **Weaviate** — container Docker.
- **Elasticsearch** — container Docker.
- **MSSQL** — container Docker.
- **Oracle** — container Docker (`gvenzl/oracle-free`).
- **Pulsar** — container Docker standalone.
- **Excel** — arquivo local.
- **BigQuery** — emulator Docker (`ghcr.io/goccy/bigquery-emulator`).
- **Redshift** — LocalStack Pro com serviço Redshift.
- **Kinesis** — LocalStack com serviço Kinesis.
- **Starburst** — Trino local (`trinodb/trino`).

### Conectores que precisam de trial/conta (prioridade para testes)

1. **Snowflake** — trial de 30 dias, ideal para validar bulk ingest ADBC.
2. **BigQuery** — projeto GCP com billing (ou créditos gratuitos).
3. **Stripe** — modo teste gratuito, ideal para validar paralelismo de páginas.
4. **Salesforce** — Developer Edition gratuita.
5. **Shopify** — loja de desenvolvedor gratuita.
6. **AWS (Redshift, Kinesis)** — requer conta AWS (possível usar créditos free tier).
7. **Databricks** — trial/community edition.
8. **SAP HANA** — registro SAP para baixar imagem Express.
9. **Google/Meta/LinkedIn/TikTok/X Ads + GA4/YouTube** — requerem contas nas respectivas plataformas + projetos OAuth.
10. **Azure AI Search** — subscription Azure.
11. **Vertex Vector Search** — projeto GCP.

---

## Resultados dos testes Enterprise locais

Testes executados com o binário `nexusflow-enterprise --features connectors-all` (repo `nexus-connectors-enterprise`) após instalar uma licença de teste JWT.

| Direção | Conector | Infra | Status | Tempo aprox. | Linhas | Observação |
|---|---|---|---|---|---|---|
| Parquet → Elasticsearch | elasticsearch | Container local | ✅ sucesso | ~18 s | 100 000 | Bulk API; precisou ajustar watermark de disco |
| Parquet → Weaviate | weaviate | Container local | ✅ sucesso | ~58 s | 100 000 | Batch upsert; batch delete implementado; precisou ajustar threshold de disco e usar UUID como primary key |

Observações:
- **Weaviate** exige que o campo `primary_key` seja um UUID válido. Conectores que recebem inteiros precisam de uma estratégia de geração de UUID (documentar ou implementar no connector).
- **Elasticsearch** e **Weaviate** entraram em modo read-only devido ao watermark de disco do host (disco acima de 90%). Em ambiente de teste local é necessário desabilitar/elevar os thresholds.

---

## Próximos passos recomendados

1. **Validar o build Docker atual** com as correções de ChromaDB + ClickHouse.
2. **Recuperar o container `nexusflow-app`** com a nova imagem.
3. **Re-executar os testes 100 k** para ChromaDB e ClickHouse e medir o novo tempo.
4. **Executar teste de 1 milhão de linhas** CSV → Postgres para confirmar que o tempo está linear (não 71 min).
5. **Testar localmente sem conta** MSSQL, Oracle, Pulsar, Excel, BigQuery emulator, LocalStack (Kinesis/Redshift) e Trino (Starburst). — ✅ feito, ver §"Resultados da sessão seguinte" abaixo.
6. **Criar contas trial** para Snowflake, Stripe, Salesforce, Shopify e contas de ads (Google/Meta/LinkedIn/TikTok/X) para benchmarks reais.
7. **Abrir issues/tasks** para cada item do plano Enterprise acima.

---

## Resultados da sessão seguinte (2026-08-28) — item 5 acima

### Achado real: bug de registro duplicado, não bug de config

`nexus-connector-kinesis` e `nexus-connector-pulsar` tinham sido
adicionados por engano diretamente em `crates/nexus-connectors/`
(commit `b5260aa`, 2026-08-27) — violando a regra deste próprio repo
("conectores enterprise nunca entram em `crates/nexus-connectors/`",
`CLAUDE.md` §3) e duplicando crates que já existiam, corretamente, no
repo privado `nexus-connectors-enterprise`. `nexus-server`'s
`build_source`/`validate_source_config` tinham `match` arms fixos pra
`"kinesis"`/`"pulsar"` que **sempre** venciam a cópia pública sobre o
registro `SourceBuilder` do enterprise (`other => registry fallback`,
nunca alcançado pra esses dois nomes) — todo fix aplicado no lado
enterprise (timeout do pulsar, endpoint do kinesis) era código morto,
nunca exercitado em runtime. Removido em `ecdb632`/`ba311eb` — a cópia
pública nunca deveria ter existido.

### Bug real corrigido: hang do Pulsar

Com o registro duplicado fora do caminho, o `pulsar` do enterprise
ainda travava pra sempre (`tokio::time::timeout` nunca disparava contra
um broker real — nem no `connect()`, nem no loop de leitura). Causa:
`tokio::time::timeout(dur, fut)` só funciona de forma confiável quando
`fut` cede o controle de volta pro runtime nos próprios pontos de
`.await` — o cliente `pulsar` (v6.9.0) aparentemente não fazia isso
contra esse broker. Fix: isolar as chamadas ao broker (`connect`,
`consumer build`, loop de leitura) numa task `tokio::spawn` própria e
correr o `timeout` contra o `JoinHandle` dela, não contra a future
direto — uma task travada só prende sua própria worker thread; a task
que espera o `JoinHandle` continua respondendo ao timer normalmente.
Confirmado contra broker real: pipeline termina em ~15-20s (antes
travava indefinidamente) e lê dado corretamente.

### Resultados por conector

| Conector | Infra | Status | Observação |
|---|---|---|---|
| MSSQL | container local | ✅ sucesso | source+sink; auto-create-table implementado e testado (tabela dropada antes do teste, criada sozinha) |
| Oracle | container local (`gvenzl/oracle-free`) | ✅ sucesso | sink; auto-create-table implementado e testado; precisou instalar `libaio1t64` (symlink pra `libaio.so.1`, nome antigo que o Instant Client espera no Ubuntu 24.04) e `unixodbc` (`libodbcinst.so.2`) na imagem |
| Synapse | mesmo driver/protocolo do MSSQL, testado contra MSSQL local | ✅ wiring confirmado | não é teste do serviço Azure real; precisou de slug `synapse` próprio na license de teste (não herda de `mssql`) |
| Pulsar | container local | ✅ sucesso (após fix) | ver "Bug real corrigido" acima |
| Starburst | Trino local (`trinodb/trino`, catálogo `memory`) | ✅ sucesso | precisava de `ssl: false` (imagem de teste sem TLS) e da license de teste incluir o slug `starburst` (conector novo, adicionado depois da license anterior) |
| Kinesis | kinesalite local | ✅ sucesso | confirmado com stream limpo (tentativa anterior falhou por registro malformado do próprio `aws-cli` de teste, não bug do conector) |
| Weaviate, Elasticsearch | containers locais | ✅ já confirmados em sessão anterior | ver seção acima |
| BigQuery | emulador `ghcr.io/goccy/bigquery-emulator` | ❌ bloqueado | **achado novo**: o driver ADBC (Go, `apache/arrow-adbc`) ignora a env `BIGQUERY_EMULATOR_HOST` — toda chamada bate direto em `bigquery.googleapis.com` real, mesmo setando essa env no processo. O emulador local **não é** alternativa viável pra este conector hoje (correção ao que este doc dizia antes). Só dá pra testar com conta GCP real. |
| Redshift | Postgres local (mesmo driver ADBC, wire-compatible) | ⚠️ parcial | driver conecta, mas exige SSL sem opção de desligar no schema — Postgres de teste sem TLS não fecha o teste. Dá pra fechar configurando SSL no Postgres local (não feito, desproporcional só pra validar wiring). |
| HANA | — | ❌ fora de escopo | driver ODBC (`HDBODBC`) exige registro/login SAP pra baixar — bloqueio do fabricante, não do NexusFlow. |

### Feature nova: auto-create-table (Oracle, MSSQL/Synapse)

Só `postgres`/`mysql` (OSS) tinham `CREATE TABLE IF NOT EXISTS`
automático. Oracle e MSSQL exigiam criar a tabela na mão antes de
testar — implementado seguindo o mesmo padrão do `postgres` (schema do
primeiro batch escrito, DDL lazy no primeiro `write_batch`). MSSQL usa
`IF OBJECT_ID(...) IS NULL CREATE TABLE ...` (idempotente numa
instrução só); Oracle não tem `CREATE TABLE IF NOT EXISTS` antes da
23c, então tenta o DDL e engole `ORA-00955` ("name is already used by
an existing object").
