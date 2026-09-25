# NexusFlow

**Universal Rust Data & Vector Framework** — movimentação, transformação, vetorização e orquestração de dados (ETL/ELT/Streaming) de altíssima performance.

> 🎉 **A partir de agora, o core open source do NexusFlow está liberado** — Apache-2.0, todos os 31 conectores OSS, use e distribua livremente (ver [`LICENSING.md`](./LICENSING.md)). A **Store de conectores enterprise** (compra self-service via Stripe) está em desenvolvimento final e deve abrir em breve — enquanto isso, o catálogo enterprise já implementado pode ser consultado em [`docs/ENTERPRISE_CONNECTORS.md`](./docs/ENTERPRISE_CONNECTORS.md).

> Status: ✅ MVP completo e além — 24 crates de conector (31 nomes no catálogo com as variantes CDC: Postgres/SQLite/ClickHouse/DuckDB fast-path, MySQL/MongoDB/Kafka/Redis/NATS/RabbitMQ/MQTT/REST/ODBC/CSV bridging, sinks vetoriais, data lake formats, AI Lake e webhook — Kafka já com source+sink) linkáveis via feature flag, API + UI + observabilidade + distribuição Linux + Kubernetes (`packaging/kubernetes/`, validado num minikube real) funcionando end-to-end. Além do ETL/ELT core: **catálogo de dados** pesquisável com flag de PII (`GET /catalog/datasets`), **orquestração cross-pipeline** (`depends_on` com modo `any`/`all`), **detecção de anomalia** por volume (z-score, alertando nos 5 canais já existentes), **mascaramento de PII** por tokenização determinística (`NEXUS_MASKING_SALT`), e **distribuição de carga** entre workers via fila Postgres (`NEXUS_QUEUE_MODE=true`, opt-in) — ver `ROADMAP.md` Fases 25–29. Windows já produziu e instalou um `.msi` real numa máquina real (2026-09-06) — mas o job `build-windows` do CI de release automático segue removido; macOS já buildou e rodou de ponta a ponta num runner `macos-latest` real (2026-09-06), mas ninguém instalou ainda numa máquina física própria.

## O que é

NexusFlow move dados de qualquer fonte para qualquer destino via **Apache Arrow** (zero-copy em memória), com fast-path nativo (ADBC / Arrow Flight SQL) e fallback híbrido (ODBC/JDBC, REST/SaaS, NoSQL, Kafka). Também atua como **AI Lakehouse Builder**: chunking + embeddings + carga em bancos vetoriais.

Interface visual node-based (React Flow) sobre um core 100% Rust.

Detalhes completos de stack, arquitetura e regras de código: ver [`CLAUDE.md`](./CLAUDE.md). Pra instalar e rodar agora: [`docs/GETTING_STARTED.md`](./docs/GETTING_STARTED.md).

## Recursos principais

- **31 conectores OSS** (fast-path ADBC pra Postgres/SQLite/DuckDB/ClickHouse, bridging genérico pro resto) + **6 CDCs nativos** (Postgres WAL, MongoDB Change Streams, MySQL binlog, Delta Lake, Iceberg, AI-Lake) — sem Debezium/Kafka no meio.
- **Transformação sem escrever código** — blocos de limpeza/transformação configuráveis (filtrar, renomear, converter tipo, preencher nulos, agregar, etc.), encadeáveis no Canvas com preview por bloco; alternativa a escrever SQL/Python direto (ver [`docs/USER_GUIDE.md` §12](./docs/USER_GUIDE.md#12-blocos-de-transformaçãolimpeza-sem-código)).
- **AI Lakehouse**: chunking (fixed-size/recursive/semantic) + embeddings (ONNX local ou API OpenAI-compatible) + carga em 6 bancos vetoriais (LanceDB, Qdrant, Milvus, pgvector, Pinecone, ChromaDB).
- **LLMOps**: node `llm` em lote (OpenAI-compatible ou Anthropic nativo), RAG ad-hoc (`POST /rag/query`) sobre os mesmos bancos vetoriais, avaliação sistemática por golden dataset, versionamento git embutido de pipelines/prompts.
- **dbt opcional** — ELT clássico ou ETL real (lê de volta o resultado transformado e grava num destino final, tudo num único run).
- **Plataforma de dados**: catálogo pesquisável com flag de PII, orquestração cross-pipeline (`depends_on`), detecção de anomalia por volume (z-score), mascaramento de PII por tokenização determinística, distribuição de carga entre workers via fila Postgres.
- **RBAC** (Read/Execute/Write/Admin), segredos criptografados (AES-256-GCM), alertas em 5 canais (Slack, Teams, PagerDuty, Email, webhook), observabilidade estruturada (`tracing` + OTel + Prometheus).
- **Single binary**: um processo só, API + WebSocket + UI web embutida — sem dependência externa além do banco de metadados (SQLite ou Postgres).

## Quickstart

Jeito mais rápido: imagem já publicada no **Docker Hub**
(`thiagolange/nexusflow`, todos os 31 conectores já linkados), sem
precisar buildar nada:

```bash
# volume nomeado nasce root-owned; o container roda como uid 1001 (não-root)
docker volume create nexusflow_data
docker run --rm -v nexusflow_data:/data alpine chown -R 1001:1001 /data

docker run -d -p 8080:8080 \
  -e NEXUS_JWT_SECRET="$(openssl rand -hex 32)" \
  -e NEXUS_ENCRYPTION_KEY="$(openssl rand -hex 32)" \
  -e NEXUS_ADMIN_USERNAME=admin -e NEXUS_ADMIN_PASSWORD=troque-isto \
  -e NEXUS_CHECKPOINT_DB="sqlite:///data/nexusflow.db" \
  -e NEXUS_AUTH_DB="sqlite:///data/nexusflow-auth.db" \
  -e NEXUS_PIPELINES_DB="sqlite:///data/nexusflow-pipelines.db" \
  -v nexusflow_data:/data \
  thiagolange/nexusflow:latest
# abre http://localhost:8080
```

> Sem o volume + as 3 `NEXUS_*_DB` acima, o container sobe e morre com
> `unable to open database file` — o binário roda como usuário não-root e
> o diretório de trabalho padrão não é gravável por ele. Ver
> [`docs/GETTING_STARTED.md` §3](./docs/GETTING_STARTED.md#3-vari%C3%A1veis-de-ambiente).

### Docker Compose

```yaml
# docker-compose.yml
services:
  nexusflow:
    image: thiagolange/nexusflow:latest
    ports:
      - "8080:8080"
    environment:
      NEXUS_JWT_SECRET: ${NEXUS_JWT_SECRET}
      NEXUS_ENCRYPTION_KEY: ${NEXUS_ENCRYPTION_KEY}
      NEXUS_ADMIN_USERNAME: admin
      NEXUS_ADMIN_PASSWORD: ${NEXUS_ADMIN_PASSWORD}
      NEXUS_CHECKPOINT_DB: sqlite:///data/nexusflow.db
      NEXUS_AUTH_DB: sqlite:///data/nexusflow-auth.db
      NEXUS_PIPELINES_DB: sqlite:///data/nexusflow-pipelines.db
    volumes:
      - nexusflow_data:/data
    restart: unless-stopped

volumes:
  nexusflow_data:
```

```bash
# .env (mesmo diretório do docker-compose.yml) — gerar uma vez
cat > .env <<EOF
NEXUS_JWT_SECRET=$(openssl rand -hex 32)
NEXUS_ENCRYPTION_KEY=$(openssl rand -hex 32)
NEXUS_ADMIN_PASSWORD=troque-isto
EOF

# volume nomeado nasce root-owned (mesmo motivo do docker run acima)
docker volume create nexusflow_data
docker run --rm -v nexusflow_data:/data alpine chown -R 1001:1001 /data

docker compose up -d
# abre http://localhost:8080
```

Pra Postgres em vez de SQLite (múltiplas réplicas), troque as 3 variáveis `NEXUS_*_DB` no `docker-compose.yml` — mesmo formato de URL da seção seguinte.

### Instaladores prontos (sem Docker)

Além da imagem Docker acima, já tem binário pra baixar direto — todos com **todos os 31 conectores** já linkados:

| Plataforma | Como instalar | Status |
|---|---|---|
| Linux (qualquer distro) | `curl -fsSL https://raw.githubusercontent.com/ailake-io/nexusflow/main/scripts/install.sh \| sh` | ✅ validado |
| Linux (Debian/Ubuntu) | `.deb` — [releases](https://github.com/ailake-io/nexusflow/releases) | ✅ validado |
| Linux (Fedora/RHEL) | `.rpm` — [releases](https://github.com/ailake-io/nexusflow/releases) | ✅ validado |
| Linux (qualquer distro) | AppImage — [releases](https://github.com/ailake-io/nexusflow/releases) | ✅ validado |
| Windows | `.msi` — [releases](https://github.com/ailake-io/nexusflow/releases) | ✅ instalado numa máquina Windows real (2026-09-06) |
| Windows | `winget install Ailake.NexusFlow` | ⏳ manifesto submetido, PR pendente de review em `microsoft/winget-pkgs` |
| macOS (Apple Silicon) | `brew install ailake-io/nexusflow/nexusflow` | ✅ build validado num runner real; ninguém ainda rodou numa máquina física própria |
| Kubernetes | Manifests kustomize em [`packaging/kubernetes/`](./packaging/kubernetes/) | ✅ validado num minikube real |

Detalhe completo de cada instalador (variáveis de ambiente, dependências de sistema, build a partir do source): [`docs/GETTING_STARTED.md` §1](./docs/GETTING_STARTED.md#1-instalação).

### Produção: Postgres em vez de SQLite

SQLite (padrão acima) é suficiente pra testar localmente, mas só permite **uma réplica** rodando por vez. Pra produção — múltiplas réplicas atrás de um load balancer, ou simplesmente mais robustez de banco — aponte as mesmas 3 variáveis pra um Postgres em vez de arquivos SQLite; o backend troca automaticamente pelo scheme da URL, sem flag nem env var extra:

```bash
docker run -d -p 8080:8080 \
  -e NEXUS_JWT_SECRET="$(openssl rand -hex 32)" \
  -e NEXUS_ENCRYPTION_KEY="$(openssl rand -hex 32)" \
  -e NEXUS_ADMIN_USERNAME=admin -e NEXUS_ADMIN_PASSWORD=troque-isto \
  -e NEXUS_CHECKPOINT_DB="postgres://user:senha@seu-postgres:5432/nexusflow" \
  -e NEXUS_AUTH_DB="postgres://user:senha@seu-postgres:5432/nexusflow" \
  -e NEXUS_PIPELINES_DB="postgres://user:senha@seu-postgres:5432/nexusflow" \
  thiagolange/nexusflow:latest
```

As três podem apontar pro mesmo banco (tabelas não colidem) ou bancos separados. Com Postgres, múltiplas réplicas do NexusFlow podem compartilhar o mesmo backend com segurança (SQLite não pode — não use volume `ReadWriteMany` com ele) e o scheduler de cron coordena via `pg_try_advisory_lock`, garantindo que só uma réplica dispara cada pipeline agendado por tick. Manifests prontos pra Kubernetes/Docker Swarm (multi-réplica + Postgres compartilhado, já validados) e o guia de migração de dados existentes de SQLite: [`docs/GETTING_STARTED.md` §3](./docs/GETTING_STARTED.md#metadados-em-postgres-multi-réplica--k8s).

Mais opções (curl|sh, .deb/AppImage, build from source, habilitar conectores extras): [`docs/GETTING_STARTED.md`](./docs/GETTING_STARTED.md).

## Documentação

| Arquivo | Conteúdo |
|---|---|
| [`docs/GETTING_STARTED.md`](./docs/GETTING_STARTED.md) | Instalação, configuração e primeiro pipeline — comece por aqui |
| [`docs/USER_GUIDE.md`](./docs/USER_GUIDE.md) | Referência completa: config de cada conector, transform, embeddings, agendamento |
| [`docs/PROJECT_REVIEW.md`](./docs/PROJECT_REVIEW.md) | Backlog técnico unificado: bugs, melhorias e divergências documentação × código |
| [`CLAUDE.md`](./CLAUDE.md) | Visão geral, stack, estrutura de diretórios, regras de código pro assistente AI |
| [`ARCHITECTURE.md`](./ARCHITECTURE.md) | Arquitetura técnica detalhada: roteador de conectores, streaming/backpressure, checkpointing, pipeline de embeddings |
| [`ROADMAP.md`](./ROADMAP.md) | Fases de desenvolvimento, milestones, critérios de conclusão do MVP |
| [`CONTRIBUTING.md`](./CONTRIBUTING.md) | Como contribuir, convenções de código, processo de PR |
| [`LICENSING.md`](./LICENSING.md) | Modelo open-core: o que é OSS vs. o que é pago |
| [`docs/ENTERPRISE_CONNECTORS.md`](./docs/ENTERPRISE_CONNECTORS.md) | Catálogo de conectores enterprise já implementados no repo privado (37 crates) e lógica de priorização do que falta |
| [`docs/ENTERPRISE_LICENSING.md`](./docs/ENTERPRISE_LICENSING.md) | Design do sistema de licenciamento enterprise — verificação JWT/Ed25519 e checkout Stripe/`nexus-licensing` já validados de ponta a ponta (modo teste); deploy com credenciais Stripe live é trabalho futuro |
| [`packaging/kubernetes/README.md`](./packaging/kubernetes/README.md) | Deploy em Kubernetes via kustomize — validado num minikube real |
| [`LICENSE`](./LICENSE) | Apache License 2.0 (community edition) |

## Licença

Community Edition sob **Apache-2.0**, liberada agora — use, modifique e distribua livremente. Conectores enterprise são distribuídos separadamente sob licença comercial — ver [`LICENSING.md`](./LICENSING.md). A Store de compra self-service (checkout Stripe) está em desenvolvimento final e deve abrir em breve; o checkout já foi validado de ponta a ponta em modo teste, ver [`docs/ENTERPRISE_LICENSING.md`](./docs/ENTERPRISE_LICENSING.md).
