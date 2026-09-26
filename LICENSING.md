# 📜 Modelo de Licenciamento — NexusFlow

NexusFlow segue modelo **open-core**: núcleo aberto, conectores premium fechados.

## 1. Community Edition (OSS)

Licença: **Apache License 2.0** (ver `LICENSE`).

Inclui:
- `nexus-core` — traits, modelos Arrow, DAG parser
- `nexus-server` — API Axum, Auth/RBAC, Scheduler, WebSockets
- `nexus-ai` — pipeline de embeddings (chunking, ONNX/ort, destinos vetoriais mainstream)
- `nexus-connectors` (46 crates de conector OSS — Fase 32, 2026-09-25, migrou 22 do enterprise pra cá, ver `ROADMAP.md`): fast-path ADBC (Postgres, SQLite, DuckDB, ClickHouse sink append-only); bridging (REST genérico, MongoDB, Kafka source+sink, Redis Streams, NATS core, RabbitMQ, ODBC, MySQL batch, CSV, webhook, MQTT); data lake (Delta Lake, Iceberg, Parquet, AI Lake); vetoriais/busca (LanceDB, Qdrant, Milvus, pgvector, Pinecone, ChromaDB, Weaviate, Vertex AI Vector Search, Azure AI Search, Elasticsearch); CDC nativo (Postgres WAL, MongoDB Change Streams, MySQL binlog); SQL/data warehouse (BigQuery, Databricks, SAP HANA, MSSQL + CDC, Oracle + CDC, Redshift, Snowflake, Starburst, Teradata, Vertica); streaming (Kinesis, Pulsar); arquivo/storage (Excel, PDF OCR, Google Drive, Google Sheets, Dropbox, SharePoint) — regra: **infraestrutura de dado é OSS**, só SaaS de negócio (ads/marketing, CRM/ERP/suporte/RH, pagamento) fica enterprise
- Frontend (React Flow canvas)

Por que Apache-2.0 e não MIT: cláusula de patente protege contribuidores e usuários enterprise — mesmo racional do Arrow, DataFusion e Tokio (stack que o NexusFlow já depende).

## 2. Enterprise Connectors (pago)

Vivem em **repositório/crate privado separado** (`nexus-connectors-enterprise`), NUNCA publicado no repo OSS nem no crates.io público.

**Catálogo atual (16 conectores, todo o resto é OSS desde a Fase 32)** — regra: SaaS de negócio, não infraestrutura de dado:
- Ads/analytics de marketing: GA4, Google Ads, LinkedIn Ads, Meta Ads, TikTok Ads, X Ads, YouTube Analytics
- CRM/ERP/suporte/RH: Salesforce, HubSpot, Zendesk, Shopify, Dynamics 365, NetSuite, ServiceNow, Workday
- Pagamento: Stripe

(mais `nexus-infra-terraform` — módulo do Canvas de Infra/Terraform, não é conector de dado, fica enterprise por decisão separada)

Distribuição: **binário próprio**, compilado a partir do repo privado (`nexus-connectors-enterprise/bin`), que depende de `nexus-core`/`nexus-server` como git dependency pinada por rev — não é uma feature flag ligada no binário OSS nem um plugin carregado dinamicamente em runtime. O binário enterprise sempre lista todo o catálogo (OSS + enterprise) no `GET /connectors`; o que trava por conector é a **license key** validada em runtime (JWT assinado, checagem de expiração/seat count via `check_connector_license`) — sem license cobrindo, o conector aparece mas não salva/roda (ver `docs/ENTERPRISE_LICENSING.md`).

## 3. Regra prática pro assistente (Claude)

- Ao gerar conector novo, perguntar (ou inferir do contexto) se é candidato OSS ou enterprise **antes** de commitar no repo público.
- Nunca colar código de conector enterprise dentro de `nexus-connectors` OSS.
- Nunca misturar headers de licença Apache-2.0 com código proprietário no mesmo arquivo.
- `Cargo.toml` do workspace público não deve referenciar path de crates privados — dependência enterprise é plugin carregado em runtime ou crate git privado, nunca path relativo versionado junto do OSS.

## 4. Contribuições (CLA)

Contribuições externas ao core OSS ficam sob Apache-2.0 automaticamente (seção 5 da licença Apache cobre isso — nenhum CLA extra necessário no MVP). Reavaliar se necessário CLA formal quando houver contribuições corporativas significativas.
