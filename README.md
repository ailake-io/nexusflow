# NexusFlow

**Universal Rust Data & Vector Framework** — movimentação, transformação, vetorização e orquestração de dados (ETL/ELT/Streaming) de altíssima performance.

> Status: ✅ MVP completo e além — 24 crates de conector (31 nomes no catálogo com as variantes CDC: Postgres/SQLite/ClickHouse/DuckDB fast-path, MySQL/MongoDB/Kafka/Redis/NATS/RabbitMQ/MQTT/REST/ODBC/CSV bridging, sinks vetoriais, data lake formats, AI Lake e webhook — Kafka já com source+sink) linkáveis via feature flag, API + UI + observabilidade + distribuição Linux funcionando end-to-end. Windows já produziu um `.msi` real via workflow manual (ver nota abaixo), mas o job `build-windows` do CI de release automático segue removido; macOS tem specs mas ainda não foi validado em máquina real.

## O que é

NexusFlow move dados de qualquer fonte para qualquer destino via **Apache Arrow** (zero-copy em memória), com fast-path nativo (ADBC / Arrow Flight SQL) e fallback híbrido (ODBC/JDBC, REST/SaaS, NoSQL, Kafka). Também atua como **AI Lakehouse Builder**: chunking + embeddings + carga em bancos vetoriais.

Interface visual node-based (React Flow) sobre um core 100% Rust.

Detalhes completos de stack, arquitetura e regras de código: ver [`CLAUDE.md`](./CLAUDE.md). Pra instalar e rodar agora: [`docs/GETTING_STARTED.md`](./docs/GETTING_STARTED.md).

## Quickstart

```bash
docker build -t nexusflow .

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
  nexusflow
# abre http://localhost:8080
```

> Sem o volume + as 3 `NEXUS_*_DB` acima, o container sobe e morre com
> `unable to open database file` — o binário roda como usuário não-root e
> o diretório de trabalho padrão não é gravável por ele. Ver
> [`docs/GETTING_STARTED.md` §3](./docs/GETTING_STARTED.md#3-vari%C3%A1veis-de-ambiente).

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
| [`docs/ENTERPRISE_CONNECTORS.md`](./docs/ENTERPRISE_CONNECTORS.md) | Candidatos a conector enterprise e lógica de priorização (Excel já implementado no repo privado; os demais são trabalho futuro) |
| [`docs/ENTERPRISE_LICENSING.md`](./docs/ENTERPRISE_LICENSING.md) | Design do sistema de licenciamento enterprise (verificação JWT/Ed25519 já implementada e funcionando; checkout Stripe/`nexus-licensing` em produção é trabalho futuro) |
| [`LICENSE`](./LICENSE) | Apache License 2.0 (community edition) |

## Licença

Community Edition sob **Apache-2.0**. Conectores enterprise são distribuídos separadamente sob licença comercial — ver [`LICENSING.md`](./LICENSING.md).

## Stack (resumo)

Rust (Edition 2021) · Apache Arrow / DataFusion · ADBC · Tokio · Axum · React Flow (frontend).

> **Nota sobre validação de plataforma:** repo ficou público em 2026-09-05, e todo CI saiu do self-hosted único pra runner hospedado do GitHub (grátis/ilimitado em repo público) no mesmo dia. Linux (binário nativo, `.deb`, AppImage, `.rpm`, Docker) é o único caminho validado de ponta a ponta em máquina real, e todos os 3 pacotes buildam automaticamente em CI a cada push/PR pra `main` (agora em `ubuntu-latest`). Windows: `.msi` via `.github/workflows/build-windows-installer.yml` (workflow separado, `workflow_dispatch` manual, agora em `windows-latest`) — o setup vcpkg/OpenSSL que resolvia o bug real do `mysql_cdc` (só suporta OpenSSL nativo, sem rustls) virou passo explícito a cada execução; ainda não instalado/testado numa máquina Windows real por um humano, e `winget` continua não configurado. O job `build-windows` original dentro do `release.yml` (matrix automático a cada push/PR) segue removido dessa chain por decisão, não por bloqueio técnico. macOS: `release.yml`'s `build` job ganhou leg `macos-latest`/arm64 no mesmo dia, gerando um binário OSS-only (sem conectores enterprise — esse passo usa Docker, indisponível em runner macOS hospedado) + formula Homebrew em `packaging/macos/nexusflow.rb`; nenhuma release real passou por essa combinação ainda, trate como não validado. Contribuições ou relatórios de teste são bem-vindos.

Lista completa em [`CLAUDE.md` §2](./CLAUDE.md#%EF%B8%8F-2-tech-stack).
