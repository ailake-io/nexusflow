# Plano de performance para conectores Enterprise

> Aplica as mesmas lições das otimizações OSS recentes (Postgres COPY, CSV/Parquet append-only, SQLite/ClickHouse/ODBC batch, MongoDB concurrency, Pinecone/ChromaDB chunking, Delta/Iceberg/AI-Lake append-only) aos conectores pagos do repositório `nexus-connectors-enterprise`.
>
> Princípio orientador: **nunca quebrar CDC**. Toda otimização de "append-only" ou "batch" deve ser uma trilha separada para batches sem `__opcode`; batches CDC continuam usando o caminho delete/upsert/update correto.

## 1. Data warehouses / bancos analíticos enterprise

| Conector | Otimização principal | Detalhe técnico |
|---|---|---|
| Snowflake | `PUT` + `COPY INTO` para cargas iniciais; `MERGE` só para CDC | Stage local/S3 via Snowflake SQL, depois `COPY INTO <table>`. Append-only: `COPY INTO` direto. CDC: `MERGE` com predicado por primary key. |
| BigQuery | `storage.write_api` com `AppendRows` em stream default; `MERGE` para CDC | Usar BigQuery Storage Write API (streaming insert) em vez de REST `tabledata.insertAll`. Batch em 1.000-10.000 linhas. CDC: `MERGE` via query job. |
| Redshift | `COPY` de S3 para cargas iniciais; multi-row `INSERT`/`DELETE` para CDC | Redshift é otimizado para COPY. Para non-CDC append-only, gerar arquivos Parquet/CSV no S3 e `COPY`. CDC: DELETE + INSERT em transação. |
| Databricks | `COPY INTO` ou Delta Lake `write` otimizado | Usar Delta Lake nativo (já há experiência OSS). Append-only: escrever direto. CDC: `MERGE INTO`. |
| Oracle | Direct-Path `INSERT /*+ APPEND */` + `MERGE` | `INSERT /*+ APPEND */` em modo nologging para cargas iniciais. CDC: `MERGE` com bind variables em batch. |
| SAP HANA | `INSERT` batch + `UPSERT`/autocommit off | Driver ODBC/JDBC com batch de binds. Append-only: desligar dedup. CDC: `UPSERT`/`DELETE`. |
| SQL Server / Azure Synapse | `BULK INSERT`/`bcp` para cargas iniciais; `MERGE` para CDC | SQL Server adora bulk copy. Gerar CSV/Parquet no filesystem e chamar `BULK INSERT`. Synapse: `COPY INTO`. CDC: `MERGE`. |
| Teradata | FastLoad/TPT para cargas iniciais; `MERGE`/`DELETE`+`INSERT` para CDC | Usar utilitários nativos quando possível. Batch inserts. |
| IBM Db2 | `LOAD` utility para cargas iniciais; `MERGE` para CDC | `LOAD FROM ... OF DEL` para append-only. CDC: SQL `MERGE`. |
| Vertica | `COPY` de STDIN/arquivo + `MERGE` | Vertica tem COPY muito rápido. Usar para non-CDC. |

### Checklist comum para warehouses
- [ ] Adicionar `append_only: bool` na config quando o sink fizer dedup/delete-before-append.
- [ ] Implementar caminho de bulk-load nativo (COPY, PUT+COPY, BULK INSERT, etc.) para non-CDC.
- [ ] Manter `MERGE`/`DELETE`+`INSERT` para batches CDC (`__opcode`).
- [ ] Usar prepared statements com binds em batch (evitar uma query por linha).
- [ ] Transações explícitas (`BEGIN`/`COMMIT`) para cada batch CDC.

## 2. SaaS / CRM / ERP

| Conector | Otimização principal |
|---|---|
| Salesforce | Bulk API 2.0 para inserts/upserts; REST só para CDC pequeno. Usar `ingestJob` com CSV/JSONL. CDC: Platform Events / Bulk API query + delete lógico. |
| SAP (BAPI/IDoc/S/4HANA) | Batch de chamadas RFC/BAPI em uma sessão; IDoc em pacotes. |
| HubSpot | Batch API (`/crm/v3/objects/{type}/batch/create`, `batch/update`, `batch/archive`). Chunk de 100. |
| Workday | Usar bulk-load APIs quando disponíveis; chunk de requests. |
| NetSuite | SuiteTalk RESTlets/Suitload batch; chunk de 200. |
| Dynamics 365 | OData `$batch`; chunk de 100. |
| ServiceNow | REST API batch; chunk de 200. |
| Zendesk | Bulk endpoints (`/api/v2/users/create_many`, etc.). |

### Checklist comum para SaaS
- [ ] Identificar endpoint batch nativo do SaaS.
- [ ] Chunk de acordo com o limite do endpoint (geralmente 100-500).
- [ ] Concorrência controlada (max 8-32 requisições paralelas).
- [ ] Retry com backoff exponencial em 429/5xx.
- [ ] Append-only: usar `create_many`; CDC: usar `update_many`/`delete_many` com opcode.

## 3. Marketing / Ads / Analytics / E-commerce

| Conector | Otimização principal |
|---|---|
| GA4 / Google Ads / YouTube Analytics | Google Ads API batch; GA4 usa Data API com paginação. Para sink (raro), batch em 1000. |
| Meta Ads (Facebook/Instagram) | Batch API (`/batch`) com 50 requests por call. |
| LinkedIn Ads | Batch endpoints; chunk de 100. |
| Stripe | Stripe suporta `Idempotency-Key` e batch limitado; usar `stripe::Client` com concurrency. |
| Shopify | Bulk Operations GraphQL para leitura; REST batch para escrita. |
| TikTok Ads | Batch API com chunk de 100. |

### Checklist
- [ ] Usar GraphQL Bulk Operations para sources de leitura massiva.
- [ ] Para sinks, agrupar por endpoint batch.
- [ ] Não fazer uma requisição por linha.

## 4. Arquivos de escritório / produtividade

| Conector | Otimização principal |
|---|---|
| Excel (.xlsx) | Escrita incremental de worksheets (já usa `calamine`/`rust_xlsx_writer`). Append-only: adicionar rows ao workbook existente sem reescrever tudo. CDC: reescrever worksheet filtrando por PK. |
| Google Sheets | `batchUpdate` com `appendCells`/`updateCells` em ranges; chunk de 50.000 células. |
| SharePoint / OneDrive | Microsoft Graph batch; upload em chunks para arquivos grandes. |

### Checklist
- [ ] Adicionar `append_only` para Excel/Sheets.
- [ ] Escrever em chunks de células, não célula por célula.

## 5. Vetorial / busca enterprise

| Conector | Otimização principal |
|---|---|
| Elasticsearch / OpenSearch | `_bulk` API com NDJSON; chunk de 5-10 MB / 1000 docs. |
| Weaviate | `objects/batch` com chunk de 100-500 vetores. |
| Vertex AI Vector Search | `indexEndpoint.upsertDatapoints` em batch; chunk de 1000. |
| Azure AI Search | `indexDocuments` batch; chunk de 1000 docs. |

### Checklist
- [ ] Replicar padrão Pinecone/ChromaDB: chunk fixo (1000).
- [ ] Usar `_bulk`/batch endpoints nativos.
- [ ] Append-only: upsert/insert em batch. CDC: delete + upsert separados.

## 6. Streaming enterprise

| Conector | Otimização principal |
|---|---|
| Amazon Kinesis | `PutRecords` (múltiplos records por call) em vez de `PutRecord`. Batch até 500 records / 5 MB. |
| Apache Pulsar | Produzir em batch com `batch_size` e `batch_timeout`. |

### Checklist
- [ ] Usar APIs batch de produção.
- [ ] Async com concurrency limitada.

## 7. CDC avançado

| Conector | Otimização principal |
|---|---|
| Oracle CDC (LogMiner) | Continuar usando LogMiner. Otimizar source: prefetch + batch de commits; sink: `MERGE` batch. |
| SQL Server CDC (CT/fn_cdc) | Continuar usando `sys.fn_cdc_get_all_changes_*`. Otimizar source: leitura em batches maiores; sink: `MERGE` batch. |
| Db2 CDC | Similar: journal/log read em batch; sink batch. |

### Princípio
- CDC **não** deve usar caminhos append-only.
- Otimizações de CDC devem focar no **source** (leitura em batch) e no **sink** (aplicação de mudanças em batch via `MERGE`/`DELETE`+`INSERT`).

## 8. Protocolos industriais

| Conector | Otimização principal |
|---|---|
| OPC-UA | Batch de writes/subscrições; buffer de leituras. Para sink, escrever múltiplos nodes em uma sessão. |

## Ordem de implementação sugerida

1. **Snowflake, BigQuery, Redshift, Databricks** — impacto máximo em RFPs enterprise; bulk-load é onde o ganho de performance é maior.
2. **Elasticsearch/OpenSearch, Weaviate** — replicação trivial do chunking Pinecone/ChromaDB.
3. **Salesforce, Shopify, HubSpot** — APIs batch bem documentadas; alto volume.
4. **SQL Server/Synapse, Oracle, SAP HANA** — aplicar padrão SQL batch + `append_only`.
5. **Kinesis, Pulsar** — batch de produção.
6. **Excel, Google Sheets** — append-only incremental.
7. **Demais SaaS e protocolos** — sob demanda de cliente.

## Métricas de sucesso

- Carga de 1M linhas em Snowflake/BigQuery/Redshift: < 30 segundos (vs. horas com insert row-by-row).
- Sinks vetoriais enterprise: chunk de 1000; sem 413/429 por payload grande.
- Todos os sinks enterprise com opção `append_only` quando aplicável.
- Testes de CDC enterprise continuam passando (sem regressão de delete/update).
