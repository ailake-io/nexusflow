# 🚀 Projeto: NexusFlow (Universal Rust Data & Vector Framework)

## 📌 1. Visão Geral
O **NexusFlow** é uma plataforma universal de movimentação, transformação, vetorização e orquestração de dados (ETL/ELT/Streaming) de altíssima performance escrita em **Rust**.

O framework é construído sobre o ecossistema **Apache Arrow** (para processamento e transferência em memória *zero-copy*) e adota uma **Arquitetura de Conectores Híbrida**:
1. **Nativa/Fast-Path (ADBC & Arrow Flight SQL):** Para bancos de dados relacionais e analíticos modernos (transmissão binária sem overhead de serialização).
2. **Híbrida/Fallback (Bridging Connectors):** Para sistemas legados (ODBC/JDBC), APIs REST/SaaS, NoSQL (MongoDB) e Filas de Mensagens (Kafka), convertendo qualquer fonte diretamente para `RecordBatch` Arrow em memória.

O NexusFlow atua como um **AI Lakehouse Builder**: extrai dados de qualquer fonte, aplica *chunking*, gera *embeddings* vetoriais acelerados por hardware (CPU/GPU) e realiza a carga nativa em **Bancos Vetoriais**, **Open Table Formats**, **Filesystems** e **Databases**.

O sistema conta com uma interface gráfica visual baseada em nós (*Node-based Canvas* com React Flow), controle de acesso RBAC, suporte opcional a dbt, alertas multi-canal, observabilidade estruturada e distribuição multiplataforma (Windows, Linux e macOS).

---

## 🛠️ 2. Tech Stack

### 🦀 Backend (Core Engine em Rust)
* **Linguagem:** Rust (Edition 2021)
* **Formato Universal em Memória:** Apache Arrow (`arrow`, `arrow-array`, `arrow-flight`)
* **Conectividade Fast-Path (ADBC/Flight):** `adbc_core`, `adbc_driver_manager`, `arrow-flight` (gRPC)
* **Conectividade Híbrida (Fallback/Bridging):** `odbc-api`, `mongodb`, `rdkafka`, `reqwest`
* **Conectividade Data Lake:** `deltalake`, `iceberg-rust`, `parquet`
* **Conectividade Vetorial (AI):** SDKs (`qdrant-client`, `milvus-sdk-rust`, `lancedb`, `pgvector`, Pinecone)
* **Engine de Transformação & AI:** `datafusion` (SQL em memória), `ort` (ONNX Runtime via CPU/CUDA/Metal)
* **Async Runtime & Observabilidade:** `tokio` (com `mpsc`), `tracing`, `opentelemetry`
* **API Server & Metadados:** `axum` (REST & WebSockets), `sqlx` (Postgres/SQLite), `jsonwebtoken`, `aes-gcm`

### 🎨 Frontend (UI)
* **Framework:** React / Next.js (TypeScript) ou Vite
* **Canvas Visual:** React Flow (Drag-and-drop de nós)
* **Estilização e Componentes:** Tailwind CSS + Shadcn/ui
* **Comunicação em Tempo Real:** WebSockets (para métricas MB/s, progresso e logs)

---

## 📂 3. Estrutura de Diretórios (Monorepo Workspace)

O projeto adota a estrutura de *Cargo Workspace* para modularidade:

```text
nexusflow/
├── Cargo.toml                     # Configuração do Workspace
├── crates/
│   ├── nexus-core/                # Traits base (Source/Sink/Transform), modelos Arrow, DAG parser, registry de conectores
│   ├── nexus-connectors/          # Workspace de sub-crates, um crate por conector (NÃO monolítico):
│   │   ├── nexus-connector-postgres/
│   │   ├── nexus-connector-mysql/
│   │   ├── nexus-connector-duckdb/
│   │   ├── nexus-connector-mongodb/
│   │   ├── nexus-connector-rest/      # Bridging genérico REST/SaaS
│   │   └── ...                        # 1 crate novo por conector, feature-flag controla o que entra no binário final
│   ├── nexus-ai/                   # Pipeline de embeddings (ort, ONNX, chunking) — funções puras, sem I/O externo
│   └── nexus-server/               # API Axum, Auth/RBAC, Scheduler, Checkpoint store, WebSockets
├── frontend/                      # UI em React/Vite (React Flow)
├── docs/                          # Documentação da arquitetura e APIs
└── src/                           # Binário principal: SOMENTE bootstrap fino que sobe nexus-server (nenhuma lógica de orquestração duplicada aqui)
```

> Conectores enterprise (pagos) NUNCA entram em `crates/nexus-connectors/`. Vivem em repo/crate privado separado, carregado como plugin em runtime — ver `LICENSING.md`.

---

## 🏗️ 4. Arquitetura do Sistema e Conectores Híbridos

### 4.1. Roteador de Conectores (Matrix de Conectividade)
                      +-----------------------------------+
                      |    ROUTER DE CONECTORES (RUST)    |
                      +-----------------------------------+
                                        |
    +-----------------------------------+-----------------------------------+
    |                                   |                                   |
    v                                   v                                   v
[ FAST-PATH (ADBC) ]            [ FAST-PATH (ARROW FLIGHT) ]        [ HÍBRIDO (BRIDGING) ]
- Postgres, DuckDB, MySQL       - ClickHouse, Dremio,               - APIs REST / SaaS
- Snowflake, BigQuery           - Spark Flight SQL,                 - NoSQL (MongoDB)
- ClickHouse ADBC, SQLite       - Databricks                        - Filas / ODBC Legados

* **Entrada (Source):** Lê via ADBC binário. Fontes sem ADBC são convertidas via `RecordBatchBuilder`.
* **Saída (Destination):** Descarrega via ADBC ou converte batch Arrow para queries parametrizadas (batch insert).
* **Suporte a CDC (Change Data Capture):** Leitura de logs de transação (WAL no Postgres, Binlog no MySQL) convertidos em eventos Arrow contendo opcodes (`I`, `U`, `D`) para cargas incrementais em tempo real.

> **Estado real (ver `ROADMAP.md`)**: a matriz acima é a visão de produto, não o estado atual. Hoje só **Postgres** e **SQLite** existem como conectores ADBC fast-path (`crates/nexus-connectors/nexus-connector-postgres`/`-sqlite`); MySQL, DuckDB, Snowflake, BigQuery e ClickHouse ADBC **não têm crate implementado**. Nenhum conector Arrow Flight SQL existe ainda — ClickHouse/Dremio/Spark Flight SQL/Databricks são inteiramente aspiracionais. CDC hoje é só via Debezium+Kafka (`nexus-connector-kafka`, camada Híbrida) — o parser nativo de WAL/binlog descrito acima é condicional e não agendado (`ARCHITECTURE.md §7`, `ROADMAP.md` Marco 13).

### 4.2. Engine de Streaming, Backpressure e Checkpointing
O núcleo opera via canais assíncronos (`mpsc::channel`).
* **Backpressure:** Limitando o canal `mpsc(100)`, evita-se estouro de memória RAM (OOM) se a leitura for mais rápida que a escrita.
* **Gestão de Estado (Checkpoints):** Para cargas incrementais e CDC, o Worker de Destino, a cada *commit* bem-sucedido no banco alvo, emite uma mensagem de estado (ex: `last_updated_at` = '2023-10-01') que o `nexus-server` persiste no SQLite/Postgres. Se o pipeline falhar, ele retoma exatamente do último cursor validado.

### 4.3. Pipeline de Embeddings e AI Lakehouse (Node AI)
Módulo intermediário que converte colunas de texto em vetores float32 e anexa a coluna `embedding` ao `RecordBatch` Arrow.

 [ Input Arrow RecordBatch ] 
 |  id: 101
 |  texto: "A transação foi aprovada..." 
 +----------------------------------------+
                     |
                     v
 [ EMBEDDING WORKER (Rust + ONNX / CUDA) ]
                     |
                     v
 [ Output Arrow RecordBatch com Vetor ]
 |  id: 101
 |  texto: "A transação foi aprovada..."
 |  embedding: [0.012, -0.431, 0.881, ...]
 +----------------------------------------+
                     |
                     v
 [ AI LAKE / VECTOR DATABASE ]

* **Chunking:** Fixed-size Window, Recursive Character e Semantic.
* **Aceleração (Features):** `cpu` (SIMD), `cuda` (NVIDIA), `metal` (Apple), `api` (LLM Externa).
* **Destinos:** LanceDB, Qdrant, Milvus, Pinecone, ChromaDB e pgvector.

### 4.4. Transformações Opcionais e ELT (dbt)
* **Modo Padrão (Sem dbt):** Movimentação com transformação leve SQL em memória via DataFusion.
* **Modo Padrão ELT (Com dbt Opcional):** Após a carga dos dados brutos no destino, o backend Rust invoca via subprocesso assíncrono o `dbt-core` (`dbt run/build`) para transformações no Data Warehouse.

---

## 🔒 5. Segurança, Autenticação e RBAC Granular

* **Autenticação e RBAC:** Controle de acesso por JWT com níveis: `Read` (visualizar logs), `Execute` (rodar jobs), `Write` (editar Canvas) e `Admin` (gerenciar acessos).
* **Segredos:** Credenciais e URIs salvas no banco são criptografadas em repouso via **AES-256-GCM**. Nenhuma senha é exposta em plain text na UI.

---

## 🔔 6. Observabilidade, Telemetria e Alertas

* **Logs Estruturados:** Utilização da crate `tracing` formatando logs em JSON para captura via OpenTelemetry, permitindo rastrear gargalos em tasks assíncronas.
* **Alertas Assíncronos (`tokio::spawn`):** Envio de notificações sem bloquear o fluxo principal para: Slack (Block Kit), MS Teams, PagerDuty (críticos), Email (SMTP) e Webhooks.
* **UI Real-Time:** WebSockets transmitindo estatísticas de hardware, MB/s, linhas/segundo e logs do dbt diretamente para a UI.

---

## 📦 7. Deploy e Distribuição Multiplataforma

O NexusFlow é cross-platform com compilação estática sempre que possível:
* **Windows:** `.exe` independente. Instalação via `winget` ou pacote `.msi`. Hardware: CPU ou DirectML/CUDA.
* **Linux:** AppImage, `.deb`, `.rpm`. Hardware: CPU ou CUDA.
* **macOS:** `Homebrew` ou `.dmg`. Hardware: Apple Silicon (Metal) e Intel.
* **Docker / K8s:** Imagens multi-arch otimizadas com suporte a GPUs (`--gpus all`).
* *Single Binary Deployment:* A interface React compilada é embutida no binário Rust via `rust-embed`. Executar o binário inicia o backend e serve a UI web em `http://localhost:8080`.

---

## 📝 8. Regras Práticas para o LLM Assistente (AI Guidelines)

Ao gerar código para o NexusFlow, o assistente DEVE seguir estas regras estritas:
1. **Rust First & Zero-Copy:** Priorize `arrow-rs`, `datafusion` e `adbc_core`. **Proibido** converter dados para JSON ou usar `String::clone()` no data path. Manipule referências no Arrow ArrayData.
2. **Conectores Híbridos:** Sempre implemente o `RecordBatchBuilder` como fallback para injetar fontes sem ADBC no pipeline.
3. **Tratamento Ergonômico de Erros:** Use `thiserror` para tipos de erros locais e `anyhow` para falhas de orquestração. No Axum, crie uma struct que implemente `IntoResponse` para mapear erros em códigos HTTP corretos.
4. **Isolamento da UI:** O React Flow mantém o *Source of Truth* da DAG em formato JSON estrito, que é interpretado pelo parser no backend.
5. **Feature Flags Rigorosas:** Isole dependências pesadas (`ort`, drivers RDKafka) e suporte de GPU atrás de flags `[features]` no `Cargo.toml`.
6. **Testabilidade:** Gere código testável. Todo conector deve prever uma interface mockável para facilitar a criação de testes de unidade sem dependência do banco de dados real.