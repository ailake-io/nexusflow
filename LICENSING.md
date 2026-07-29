# 📜 Modelo de Licenciamento — NexusFlow

NexusFlow segue modelo **open-core**: núcleo aberto, conectores premium fechados.

## 1. Community Edition (OSS)

Licença: **Apache License 2.0** (ver `LICENSE`).

Inclui:
- `nexus-core` — traits, modelos Arrow, DAG parser
- `nexus-server` — API Axum, Auth/RBAC, Scheduler, WebSockets
- `nexus-ai` — pipeline de embeddings (chunking, ONNX/ort, destinos vetoriais mainstream)
- `nexus-connectors` (subset OSS) — conectores fast-path (Postgres, MySQL, DuckDB, SQLite, ClickHouse ADBC) e híbridos comuns (REST genérico, MongoDB, Kafka)
- Frontend (React Flow canvas)

Por que Apache-2.0 e não MIT: cláusula de patente protege contribuidores e usuários enterprise — mesmo racional do Arrow, DataFusion e Tokio (stack que o NexusFlow já depende).

## 2. Enterprise Connectors (pago)

Vivem em **repositório/crate privado separado** (ex: `nexus-connectors-enterprise`), NUNCA publicado no repo OSS nem no crates.io público.

Candidatos a conector pago (decidir caso a caso conforme demanda de mercado):
- Snowflake / BigQuery / Databricks avançado (features enterprise, não o básico ADBC)
- SaaS/CRM conectores (Salesforce, HubSpot, SAP)
- Vector DBs enterprise (Pinecone managed, Milvus cluster mode)
- CDC avançado (Oracle GoldenGate-style, SQL Server CDC enterprise)

Distribuição: binário compilado com feature flag `enterprise`, carregado via `nexus-server` mediante **license key** validada em runtime (JWT assinado pela NexusFlow, checagem de expiração/seat count).

## 3. Regra prática pro assistente (Claude)

- Ao gerar conector novo, perguntar (ou inferir do contexto) se é candidato OSS ou enterprise **antes** de commitar no repo público.
- Nunca colar código de conector enterprise dentro de `nexus-connectors` OSS.
- Nunca misturar headers de licença Apache-2.0 com código proprietário no mesmo arquivo.
- `Cargo.toml` do workspace público não deve referenciar path de crates privados — dependência enterprise é plugin carregado em runtime ou crate git privado, nunca path relativo versionado junto do OSS.

## 4. Contribuições (CLA)

Contribuições externas ao core OSS ficam sob Apache-2.0 automaticamente (seção 5 da licença Apache cobre isso — nenhum CLA extra necessário no MVP). Reavaliar se necessário CLA formal quando houver contribuições corporativas significativas.
