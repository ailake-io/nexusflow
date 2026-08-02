# NexusFlow

**Universal Rust Data & Vector Framework** — movimentação, transformação, vetorização e orquestração de dados (ETL/ELT/Streaming) de altíssima performance.

> Status: ✅ MVP completo e além — todos os 16 conectores (fast-path + híbridos + AI Lakehouse + data lake formats) linkáveis via feature flag, API + UI + observabilidade + distribuição multiplataforma funcionando end-to-end. Sem release taggeado no GitHub ainda.

## O que é

NexusFlow move dados de qualquer fonte para qualquer destino via **Apache Arrow** (zero-copy em memória), com fast-path nativo (ADBC / Arrow Flight SQL) e fallback híbrido (ODBC/JDBC, REST/SaaS, NoSQL, Kafka). Também atua como **AI Lakehouse Builder**: chunking + embeddings + carga em bancos vetoriais.

Interface visual node-based (React Flow) sobre um core 100% Rust.

Detalhes completos de stack, arquitetura e regras de código: ver [`CLAUDE.md`](./CLAUDE.md). Pra instalar e rodar agora: [`docs/GETTING_STARTED.md`](./docs/GETTING_STARTED.md).

## Quickstart

```bash
docker build -t nexusflow .
docker run -d -p 8080:8080 \
  -e NEXUS_JWT_SECRET="$(openssl rand -hex 32)" \
  -e NEXUS_ENCRYPTION_KEY="$(openssl rand -hex 32)" \
  -e NEXUS_ADMIN_USERNAME=admin -e NEXUS_ADMIN_PASSWORD=troque-isto \
  nexusflow
# abre http://localhost:8080
```

Mais opções (curl|sh, .deb/AppImage, build from source, habilitar conectores extras): [`docs/GETTING_STARTED.md`](./docs/GETTING_STARTED.md).

## Documentação

| Arquivo | Conteúdo |
|---|---|
| [`docs/GETTING_STARTED.md`](./docs/GETTING_STARTED.md) | Instalação, configuração e primeiro pipeline — comece por aqui |
| [`CLAUDE.md`](./CLAUDE.md) | Visão geral, stack, estrutura de diretórios, regras de código pro assistente AI |
| [`ARCHITECTURE.md`](./ARCHITECTURE.md) | Arquitetura técnica detalhada: roteador de conectores, streaming/backpressure, checkpointing, pipeline de embeddings |
| [`ROADMAP.md`](./ROADMAP.md) | Fases de desenvolvimento, milestones, critérios de conclusão do MVP |
| [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md) | Detalhamento de engenharia marco a marco: crates/arquivos concretos, ordem de execução, critério de "pronto" |
| [`CONTRIBUTING.md`](./CONTRIBUTING.md) | Como contribuir, convenções de código, processo de PR |
| [`LICENSING.md`](./LICENSING.md) | Modelo open-core: o que é OSS vs. o que é pago |
| [`LICENSE`](./LICENSE) | Apache License 2.0 (community edition) |

## Licença

Community Edition sob **Apache-2.0**. Conectores enterprise são distribuídos separadamente sob licença comercial — ver [`LICENSING.md`](./LICENSING.md).

## Stack (resumo)

Rust (Edition 2021) · Apache Arrow / DataFusion · ADBC / Arrow Flight · Tokio · Axum · React Flow (frontend).

Lista completa em [`CLAUDE.md` §2](./CLAUDE.md#%EF%B8%8F-2-tech-stack).
