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
* **Conectividade Fast-Path (ADBC):** `adbc_core`, `adbc_driver_manager` (Postgres/SQLite hoje; Arrow Flight SQL é aspiracional, nenhum conector implementado ainda)
* **Conectividade Híbrida (Fallback/Bridging):** `odbc-api`, `mongodb`, `rdkafka`, `reqwest`, `redis`, `async-nats`, `lapin` (RabbitMQ)
* **Conectividade Data Lake:** `deltalake`, `iceberg-rust`, `parquet`
* **Conectividade Vetorial (AI):** SDKs (`qdrant-client`, `milvus-sdk-rust`, `lancedb`, `pgvector`, Pinecone)
* **Engine de Transformação & AI:** `datafusion` (SQL em memória), `ort` (ONNX Runtime via CPU/CUDA/Metal)
* **Async Runtime & Observabilidade:** `tokio` (com `mpsc`), `tracing`, `opentelemetry`
* **API Server & Metadados:** `axum` (REST & WebSockets), `sqlx` (Postgres/SQLite), `jsonwebtoken`, `aes-gcm`

### 🎨 Frontend (UI)
* **Framework:** React + Vite (TypeScript)
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
│   │   ├── nexus-connector-mysql/     # mysql (batch, bridging via RecordBatchBuilder — sem ADBC, nenhum driver existe upstream) + mysql-cdc (binlog nativo)
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

> **Estado real vs. visão de produto**: a matriz abaixo mostra o que está
> implementado **hoje**. Conectores marcados como *não implementado* existem
> apenas no roadmap/enterprise — não são expostos em `GET /connectors` e não
> têm crate em `crates/nexus-connectors/`.

```text
                      +-----------------------------------+
                      |    ROUTER DE CONECTORES (RUST)    |
                      +-----------------------------------+
                                        |
    +-----------------------------------+-----------------------------------+
    |                                   |                                   |
    v                                   v                                   v
[ FAST-PATH (ADBC) ]            [ FAST-PATH (ARROW FLIGHT) ]        [ HÍBRIDO (BRIDGING) ]
- Postgres ✅                   - (nenhum implementado)             - REST / SaaS ✅ (reqwest)
- SQLite ✅                                                         - MongoDB ✅
- MySQL (batch, ADBC) ❌ não impl.                                  - MySQL (batch, bridging) ✅
- DuckDB ADBC ✅ (upsert real, `ON CONFLICT`)                       - Kafka ✅ (genérico, sem CDC, source+sink)
- Snowflake ❌ não impl.                                            - ODBC ✅
- BigQuery ❌ não impl.                                             - CSV ✅
- ClickHouse ADBC ✅ (sink append-only, sem upsert)                 - Webhook ✅ (sink)
                                                                    - MQTT ✅ (telemetria IoT/sensor, source apenas)
                                                                    - Redis Streams ✅ (source+sink, sem consumer group)
                                                                    - NATS ✅ (core, sem JetStream, source+sink)
                                                                    - RabbitMQ ✅ (AMQP 0-9-1, auto-ack, source+sink)

[ CDC NATIVO — sem Debezium/Kafka ]
- Postgres WAL (`postgres-cdc`) ✅
- MongoDB Change Streams (`mongodb-cdc`) ✅
- MySQL binlog (`mysql-cdc`) ✅
```

* **Entrada (Source):** Lê via ADBC binário quando disponível. Fontes sem ADBC são convertidas via `RecordBatchBuilder`.
* **Saída (Destination):** Descarrega via ADBC ou converte batch Arrow para queries parametrizadas (batch insert).
* **Suporte a CDC (Change Data Capture):** Leitura de logs de transação nativos (WAL no Postgres, binlog no MySQL, Change Streams no MongoDB) convertidos em eventos Arrow contendo opcodes (`I`, `U`, `D`) para cargas incrementais. Debezium+Kafka foi removido (ver `ARCHITECTURE.md §7`, `ROADMAP.md` Fase 18).
>
> Atualizações recentes: RBAC com papel `Admin` funcional e API de gestão de usuários (`GET/POST/DELETE /users`); alertas Slack, MS Teams, PagerDuty, Email e webhook genérico implementados (`nexus-server/src/alerts.rs`); rate-limit de login por IP; execução de runs em task destacada com 202 Accepted e reaper de runs órfãs no boot; `GET /pipelines/{id}/preview` (primeiras N linhas de um source/sink persistido) e dbt como ETL real via `DbtConfig.output`/`PipelineSpec.post_dbt_sinks` (ver `ARCHITECTURE.md §13`); conectores `csv` (source+sink, delimitador configurável, local ou `s3://`/`gs://`/`az://`) e sink `webhook` genérico; logs de execução por run com marcos percentuais (10%, 20%, ..., 100%) no painel do Canvas; conector `clickhouse` (ADBC nativo, driver oficial via `dbc install clickhouse`, sink append-only por não ter upsert leve).

### 4.2. Engine de Streaming, Backpressure e Checkpointing
O núcleo opera via canais assíncronos (`mpsc::channel`).
* **Backpressure:** Limitando o canal `mpsc(100)`, evita-se estouro de memória RAM (OOM) se a leitura for mais rápida que a escrita.
* **Gestão de Estado (Checkpoints):** Para cargas incrementais e CDC, o Worker de Destino, a cada *commit* bem-sucedido no banco alvo, emite uma mensagem de estado (ex: `last_updated_at` = '2023-10-01') que o `nexus-server` persiste no SQLite/Postgres. Se o pipeline falhar, ele retoma exatamente do último cursor validado. Pra CDC especificamente: `mysql-cdc`/`mongodb-cdc` persistem a posição (`binlog_filename`+`binlog_position` / `resume_token`) automaticamente em `CheckpointCursor.resume_state` a cada run e reinjetam na config do próximo — sem isso, cada disparo do scheduler reiniciaria do zero. `postgres-cdc` não precisa disso (resume é server-side, via o próprio replication slot). `deltalake-cdc`/`iceberg-cdc`/`ailake-cdc` ainda dependem de cursor estático na config (`starting_version`/`starting_snapshot_id`) — não auto-persistido ainda.

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
* **Backends de modelo:**
  - `cpu` / `embeddings` (ONNX local via `ort`) — implementado e testado; baixa modelo do Hugging Face Hub em runtime, cache local.
  - `api` / `embeddings-api` (HTTP externo compatível com OpenAI) — implementado e testado com mock.
  - `cuda` (NVIDIA) e `metal` (Apple) registram o execution provider correto no ONNX Runtime, compilam, mas **não foram validados em hardware real** (sandbox é Linux sem GPU) — fallback silencioso pra CPU se driver/hardware não estiver presente.
* **Destinos:** LanceDB, Qdrant, Milvus, Pinecone, ChromaDB e pgvector.
* **Integração com `nexus-server`:** o estágio de embedding é uma feature opcional do servidor (`embeddings` para ONNX local, `embeddings-api` para HTTP externo); o catálogo de conectores e o Canvas já expõem o node de embedding quando a feature está ligada.

### 4.4. Transformações Opcionais e ELT/ETL (dbt)
* **Modo Padrão (Sem dbt):** Movimentação com transformação leve SQL em memória via DataFusion.
* **Modo Padrão ELT (Com dbt Opcional):** Após a carga dos dados brutos no destino, o backend Rust invoca via subprocesso assíncrono o `dbt-core` (`dbt run/build`) para transformações no Data Warehouse.
* **Modo ETL real (opcional, extensão do ELT):** Se `DbtConfig.output` estiver setado, o backend lê de volta o resultado transformado pelo dbt (do mesmo warehouse) e grava em `PipelineSpec.post_dbt_sinks` — `[Source] → [carga bruta] → [dbt transforma] → [lê de volta] → [Sink final]` num único `run`, sem precisar encadear um segundo pipeline manualmente. Ver `ARCHITECTURE.md §13`.

---

## 🔒 5. Segurança, Autenticação e RBAC Granular

* **Autenticação e RBAC:** Controle de acesso por JWT com níveis: `Read` (visualizar logs), `Execute` (rodar jobs), `Write` (editar Canvas) e `Admin` (gerenciar acessos).
* **Segredos:** Credenciais e URIs salvas no banco são criptografadas em repouso via **AES-256-GCM**. Nenhuma senha é exposta em plain text na UI.

---

## 🔔 6. Observabilidade, Telemetria e Alertas

* **Logs Estruturados:** Utilização da crate `tracing` formatando logs em JSON para captura via OpenTelemetry, permitindo rastrear gargalos em tasks assíncronas.
* **Alertas Assíncronos (`tokio::spawn`):** Slack (Block Kit), MS Teams (Adaptive Card), PagerDuty Events API v2, Email (SMTP STARTTLS) e Webhook genérico (JSON) estão implementados.
* **UI Real-Time:** WebSockets transmitem progresso por partição (batches/rows/bytes escritos), MB/s/rows/s são derivados no cliente e um frame `{"hardware_stats": {...}}` com CPU e memória do processo é intercalado a cada 2s. **GPU ainda não é transmitida** (vendor-specific, nada no código depende disso ainda).

---

## 📦 7. Deploy e Distribuição Multiplataforma

O NexusFlow é cross-platform com compilação estática sempre que possível. **Repo ficou público em 2026-09-05** e, no mesmo dia, todo workflow (`ci.yml`, `release.yml`, `connectors-heavy.yml`, `build-windows-installer.yml`) saiu do self-hosted único (que existia por causa de um bloqueio de billing da org pra runners hospedados, inclusive `ubuntu-latest` puro) pra runners hospedados do GitHub — grátis/ilimitados em repo público, o que resolveu esse bloqueio de vez, não só pra Linux:
* **Linux:** AppImage, `.deb`, `.rpm` — validados de ponta a ponta, buildados automaticamente a cada push/PR pra `main` (x86_64 apenas — os 3 scripts em `scripts/package-*.sh` hardcodam essa arch) via `release.yml`'s `build` job, agora em `ubuntu-latest`. arm64 (`ubuntu-24.04-arm`) ainda não foi adicionado ao matrix — não é mais bloqueio de billing, só não priorizado ainda. Hardware: CPU; CUDA runtime pronto via imagem Docker `nvidia/cuda`, mas aceleração real depende da feature `cuda` ainda não validada em GPU real.
* **Windows:** `.msi` real via `cargo-wix` + `packaging/windows/main.wxs`, workflow separado (`.github/workflows/build-windows-installer.yml`, só `workflow_dispatch`) agora em `windows-latest` hospedado — o setup vcpkg/OpenSSL que antes era manual e único na máquina self-hosted virou um passo explícito (`vcpkg install openssl:x64-windows-static-md`) rodando do zero a cada execução, contra o vcpkg que a própria imagem `windows-latest` já traz. Timeout bumpado pra 150min depois que uma execução real mostrou que `cargo build --features connectors-all` sozinho leva 44+ min. Ainda não instalado/testado numa máquina Windows real por um humano, e só ship o binário do servidor — build dos drivers ADBC (Postgres/SQLite) pra `.dll` continua sem existir. O job `build-windows` dentro do `release.yml` (full `connectors-all`, diferente do `.msi` standalone acima) continua **removido dessa chain automática** por ora — religar é decisão separada, não um bloqueio técnico pendente.
* **macOS:** repo ficou público 2026-09-05 (todo runner hospedado é grátis/ilimitado, resolvendo o bloqueio de billing que existia antes) — `release.yml`'s `build` job ganhou leg `macos`/`arm64` no `macos-latest` hospedado nesse mesmo dia; `scripts/build-adbc-*-driver.sh` ganharam suporte a `.dylib`. **Ainda não validado numa release real de ponta a ponta.** Limitação real (não temporária até religar CI, uma decisão de arquitetura): esse binário **não** inclui conectores enterprise — o passo que builda isso usa `docker build`, indisponível nos runners macOS hospedados; Windows contorna isso com um `[patch]` de Cargo em vez de Docker, mesmo truque ainda não replicado pro macOS. Formula Homebrew em `packaging/macos/nexusflow.rb` (checksum placeholder até a primeira release real publicar o tarball).
* **Docker / K8s:** Imagem Docker publicada no **GHCR** (`ghcr.io/ailake-io/nexusflow`, usa `GITHUB_TOKEN`) a cada push pra `main`, com tag `v{X.Y.Z}` e `latest`. Build com `FEATURES=embed-ui,connectors-all`, então a imagem já lista os 31 conectores OSS sem precisar de build local (verificado via `GET /connectors`, contagem inclui DuckDB/Redis/NATS/RabbitMQ desta sessão). Container roda como usuário não-root (`nexusflow`, uid 1001) com `HEALTHCHECK` em `/health`. Suporte a GPU via `--build-arg RUNTIME_IMAGE=nvidia/cuda:...` + `--gpus all`, mas aceleração real ainda não validada. Docker Hub (`ailake/nexusflow`) está fora do CI atual até `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` serem configurados.
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