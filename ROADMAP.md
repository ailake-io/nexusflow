# 🗺️ Roadmap — NexusFlow

Ordem por dependência técnica, não por prioridade de negócio isolada. Cada fase assume a anterior estável.

## ⚠️ Pendências ativas (não esquecer)

Consolidado dos itens que ficaram faltando/incompletos ao longo das fases abaixo — checar aqui antes de assumir que algo já está pronto.

1. ~~**Fase 12, Bloco 1 — Enforcement de license não bloqueava nada**~~ — resolvido: `check_connector_license(name, active_license)` em `connectors.rs` é chamado por `validate_source_config`/`validate_sink_config`/`build_source`/`build_sink` de verdade (não só teste unitário); `licensed: bool` em `GET /connectors` já é consumido pelo frontend (cadeado em `ConnectorPalette.tsx`, aba `Store.tsx` com status de license + form de instalação Admin-only). Repo privado `nexus-connectors-enterprise` já existe e tem 24 crates/51 entradas de catálogo (ver Bloco 3b abaixo). O que falta de verdade agora é só o Bloco 2 (serviço `nexus-licensing`/cobrança) e o Bloco 4 (storefront/checkout) — ver blocos abaixo. Follow-up registrado: o cadeado hoje é só decorativo até Salvar/Executar (sem limite de uso real pra "testar antes de comprar") — plano de trial limitado documentado no Bloco 1 abaixo.
2. ~~**Marco 13 do roadmap original — CDC nativo sem Kafka/Debezium**~~ — resolvido: Fase 18 (`postgres-cdc`/`mongodb-cdc`/`mysql-cdc`), sinal de adoção confirmado.
3. **`nexus-ai`: features `cuda`/`metal` registram o execution provider ONNX Runtime correto (`ort::ep::CUDA`/`ort::ep::CoreML`), mas não validadas em hardware real** (sandbox é Linux sem GPU) — só confirmado que compilam e que o EP é registrado antes do load da sessão; runtime faz fallback silencioso pra CPU se o driver/hardware não estiver presente. `api` (embeddings via HTTP externa, endpoint compatível com OpenAI) implementada e testada (mock via `wiremock`) — sem chamada real contra OpenAI/Azure/etc neste sandbox. O perfil `cuda` do Docker já tem a infra de runtime pronta (base image + `--gpus all`).
4. **Alertas: Slack, MS Teams, PagerDuty, Email e Webhook genérico — todos os 5 canais de `CLAUDE.md §6` implementados** (ver `nexus-server/src/alerts.rs`).
5. **Repo ficou público 2026-09-05 — todo workflow saiu do self-hosted pra runner hospedado, resolvendo o bloqueio de billing do item 16 por completo (não só macOS/Windows).** Windows: `build-windows-installer.yml` migrou de self-hosted pra `windows-latest` — o setup vcpkg/OpenSSL que resolvia o bug real do `mysql_cdc`/OpenSSL nativo (sem rustls) virou passo explícito rodando do zero a cada execução, contra o vcpkg pré-instalado na imagem hospedada. Timeout bumpado 60→150min depois que uma execução real mostrou o `cargo build --features connectors-all` sozinho levando 44+ min. Instalado e validado numa máquina Windows real (2026-09-06, `.msi` publicado de verdade) — só ship o binário do servidor; build dos drivers ADBC (Postgres/SQLite) pra `.dll` continua sem existir. `winget` continua não configurado (ver seção própria de distribuição). O job `build-windows` original dentro do `release.yml` (full `connectors-all`, matrix automático a cada push/PR) segue **removido dessa chain por ora** — decisão separada, não bloqueio técnico. **macOS entrou no matrix de `release.yml`'s `build` job** (`macos-latest`, arm64) no mesmo dia — esse leg específico produz um binário OSS-only (o passo que builda o enterprise usa `docker build`, indisponível em runner macOS hospedado). **Correção 2026-09-09**: revisão anterior deste item dizia "não validado"/"sem conectores enterprise" pro macOS como um todo — errado. `build-macos-installer.yml` (workflow separado, `[patch]` de Cargo em vez de Docker, mesmo truque do Windows) rodou de verdade num `macos-latest` real em 2026-09-06 (63m54s, `connectors-all` + todo conector enterprise, achou e corrigiu bugs reais — `libpq` keg-only, scripts `duckdb`/`clickhouse` hardcodando extensão Linux) e dispara sozinho desde 2026-09-08. Homebrew: formula em `packaging/macos/nexusflow.rb` aponta pro tarball real publicado (`v0.1.3`), `sha256` real e conferido, não placeholder. Scripts `scripts/build-adbc-*.sh` ganharam suporte a `.dylib`/`sysctl` pro macOS, validados nesse mesmo run real. O que falta: um humano de fato rodando `brew install`/o binário numa máquina Mac física. **2026-09-08: `build-windows-installer.yml` e `build-macos-installer.yml` deixaram de ser só `workflow_dispatch` isolado** — ganharam trigger `workflow_run` em `Release`/branch `main`, então agora disparam sozinhos como parte da chain automática (`ci.yml` → `connectors-heavy.yml` → `release.yml` → esses dois + `docker-hub-publish.yml`, ver item 8); o `workflow_dispatch` manual continua disponível pra rebuild avulso.
6. ~~**`.deb`/`.rpm`/AppImage validados manualmente mas nunca wireados em CI**~~ — resolvido: `release.yml`'s `build` job (Linux x86_64) agora chama `scripts/package-{deb,rpm,appimage}.sh` automaticamente a cada push/PR pra `main` e sobe os 3 artifacts junto com o tarball — antes só o tarball cru era produzido em CI, os 3 scripts existiam mas nunca eram invocados. arm64 fica de fora por ora (os scripts hardcodam amd64/x86_64). `.rpm` também já tinha sido validado manualmente com `rpmbuild` real antes disso (`scripts/package-rpm.sh` buildou `nexusflow-0.1.0-1.x86_64.rpm` de ponta a ponta; corrigido de brinde um `Requires:` incompleto — faltava `unixODBC`/`cyrus-sasl-lib`, equivalentes RPM do `unixodbc`/`libsasl2-2` que o `.deb` já lista, não pegos pelo scanner automático do rpmbuild porque são dlopen'd, não linkados direto no ELF).
7. **Estatísticas de hardware (CPU/RAM) implementadas** — `sysinfo` via `nexus-server::hardware_stats`, frame `{"hardware_stats": {...}}` intercalado no WebSocket de progresso a cada 2s (mesmo canal do `ProgressEvent`, discriminado pela chave). Sem GPU — `sysinfo` não expõe utilização de GPU (é vendor-specific, NVML pra NVIDIA etc.) e nada no código depende disso ainda.
8. ~~**Imagem Docker publicada no GHCR**~~ — **migrado pro Docker Hub em 2026-09-08**: o job `docker-publish` (GHCR, `GITHUB_TOKEN`) foi removido de vez do `release.yml`, substituído por um workflow próprio (`.github/workflows/docker-hub-publish.yml`), disparando via `workflow_run` em `Release` + `workflow_dispatch` manual, autenticando com `secrets.DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` (confirmados existentes no repo via `gh secret list`). Publica `thiagolange/nexusflow`, tags `v{X.Y.Z}` + `latest`, mesmo `FEATURES=embed-ui,connectors-all`, build amd64 apenas.
9. **Admin (gestão de usuários) tem tela no Canvas** — `UsersPanel.tsx` cobre criar/promover/excluir contra as rotas já existentes (`GET/POST /users`, `GET/DELETE /users/{username}`, `PUT /users/{username}/role`). Nav item só aparece pra role Admin (decodificado do JWT client-side, sem verificar assinatura) — enforcement real continua 100% no servidor (`auth.rs`).
10. Ver também a seção **Débitos conhecidos** no fim deste arquivo (secrets sem KMS, RBAC sem escopo por recurso, versões de dependência pinadas, advisories RustSec aceitos).
11. ~~**Estágio `embedding` do `PipelineSpec` sem UI no Canvas**~~ — resolvido: node dedicado `kind: 'embedding'` no Canvas (mesmo padrão do node `dbt`, painel próprio em `NodeInspector.tsx` já que `EmbeddingModelSpec`/`ChunkingSpec` são unions com tag que o `SchemaForm` genérico não resolve). `lib/dag.ts`'s `PipelineSpec` agora declara `embedding`; `toPipelineSpec`/`fromPipelineSpec` fazem o round-trip completo (Onnx↔Api, fixed_window↔recursive_character) sem perder config ao editar/salvar.
12. **Fase 16 (Preview + dbt ETL) é backend-only** — `GET /pipelines/{id}/preview` não tem botão/tabela no Canvas ainda (só curl/Postman); o node dbt tem handle de saída (Fase 18 adicionou, consistência visual), mas painel de config pra `dbt.output` — hoje só configurável via API/JSON direto. Ambos deliberadamente adiados até validar se o formato backend-only já resolve o suficiente.
13. ~~**Fase 18 (CDC nativo) sem toggle no Canvas**~~ — resolvido: `NodeInspector` tem switch Batch/CDC pra Postgres/MongoDB (MySQL é CDC-only, sem batch pra alternar).
14. ~~**Manifests k8s reais (Deployment/Service/PVC/Secret/HPA) ainda não escritos**~~ — resolvido: Fase 19, `packaging/kubernetes/` + `packaging/swarm/`. **Validado num minikube real em 2026-09-06/07**: multi-réplica com Postgres compartilhado, health probes, HPA+metrics-server funcionando. Bug real achado e corrigido nessa validação: `deployment.yaml` referenciava a tag da imagem via `${NEXUSFLOW_VERSION:?msg}` (sintaxe shell) direto no YAML — `kubectl apply -k`/`kubectl kustomize` nunca substitui isso (confirmado empiricamente, a string ficava literal). Corrigido com o transformer nativo do kustomize (`images:` em `kustomization.yaml`), `deployment.yaml` passou a referenciar a imagem sem tag.
15. **Guia de deploy público na web — não implementado, não documentado.** O instalador/binário sozinho não é suficiente pra rodar em produção acessível publicamente. Falta: (a) reverse proxy com TLS na frente do `nexus-server` (o binário serve HTTP puro na porta 8080, sem TLS embutido); (b) bootstrap de segredos reais (chave AES-256-GCM de credenciais, secret de assinatura JWT) — hoje sem processo documentado de geração/rotação; (c) criação do primeiro usuário Admin via `POST /users` documentada como passo de setup; (d) troca de SQLite pra Postgres pro backend de metadados quando for multi-usuário concorrente (`sqlx` já suporta os dois, só falta o guia); (e) firewall/security group expondo só 443/80, nunca a porta 8080 direta. Caminho mais simples: a imagem Docker já publicada no Docker Hub (`thiagolange/nexusflow`, non-root, `/health`) atrás de Caddy/nginx com TLS via Let's Encrypt, ou os manifests k8s do item 14 acima — documentar como `docs/guides/DEPLOY_WEB.md` (ainda não existe).
16. ~~**Billing da org GitHub bloqueado — afeta todo runner hospedado, não só macOS/Windows.**~~ — **resolvido 2026-09-05**: o bloqueio ("recent account payments have failed or your spending limit needs to be increased") era de conta inteira, mas só afetava minutos **pagos** de repo privado — repo público tem minutos de Actions hospedados grátis/ilimitados, independente do billing da conta. Tornar o repo público removeu o bloqueio de vez; todo workflow voltou a rodar em runner hospedado do GitHub (`ubuntu-latest`/`windows-latest`/`macos-latest`) em vez do self-hosted único que existia só por causa desse bloqueio. arm64 Linux (`ubuntu-24.04-arm`) ainda não foi re-adicionado ao matrix — não é mais bloqueio técnico, só não priorizado ainda.
17. **Ideia de conector: Apache Fluss (streaming storage pra lakehouse, upsert + CDC nativo) — registrado, não iniciado.** Complementaria bem o par CDC nativo + sinks Delta/Iceberg já existentes (`ARCHITECTURE.md §7`), mas sem crate Rust disponível hoje — só client Java/Flink com protocolo binário próprio, sem equivalente ao `rdkafka` que o conector Kafka usa. Vira bridging connector do zero (implementar o protocolo, não só consumir SDK pronto). Baixa prioridade — nem Fase 12 (conectores enterprise, deliberadamente por último) chegou nisso ainda, e aqui nem catálogo (`docs/ENTERPRISE_CONNECTORS.md`) tem entrada. Reavaliar se/quando surgir um client Rust (oficial ou da comunidade).

## Fase 0 — Fundação (workspace) ✅
- [x] `Cargo.toml` workspace + crates vazios: `nexus-core`, `nexus-ai`, `nexus-server`, e `crates/nexus-connectors/` já como workspace de sub-crates (não crate único) — ver `CLAUDE.md §3` e `ARCHITECTURE.md §3`
- [x] Traits base (`Source`, `Sink`, `Transform`) em `nexus-core`
- [x] `ConnectorRegistry` em `nexus-core` (registro de conectores, consumido por `nexus-server`)
- [x] `RecordBatchBuilder` genérico (adapter fallback)
- [x] `src/` como bootstrap fino (só sobe `nexus-server`, zero lógica de orquestração própria)
- [x] CI básico (fmt, clippy, test) por crate
- [x] Documentar decisão de escopo single-node (`ARCHITECTURE.md §6`) — não é item de código, é alinhamento antes de codar scheduler

## Fase 1 — MVP Fast-Path ✅
- [x] `nexus-connector-postgres` (ADBC real, source e sink), próprio crate — `nexus-connector-sqlite` acabou entrando na Fase 2 junto do transform
- [x] DAG parser: JSON estrito (2 nodes: source → sink, sem transform ainda)
- [x] Canal `mpsc` por partição (config de tamanho por pipeline, não fixo em 100) — ver `ARCHITECTURE.md §4`
- [x] Checkpoint por partição (`CheckpointCursor{partition_id, last_updated_at, offset}`) persistido em SQLite
- [x] Contrato de idempotência documentado e testado no `Sink` de referência (upsert, não insert puro) — ver `ARCHITECTURE.md §5`

## Fase 2 — Transformação leve (DataFusion) ✅
- [x] Node de transform via SQL em memória (`datafusion`)
- [x] Suporte a múltiplos sources → um transform → um sink no DAG (fan-in de N sources, fan-out pra M sinks) — mais `nexus-connector-sqlite` como segundo conector, provando o `ConnectorRegistry`

## Fase 3 — Conectores híbridos ✅
- [x] `nexus-connector-rest` genérico via `reqwest` + `RecordBatchBuilder`
- [x] `nexus-connector-mongodb` (bson → Arrow)
- [x] `nexus-connector-odbc` bridging (legado, feature `legacy`)
- [x] `nexus-connector-kafka` (base pra CDC da Fase 4, feature `consumer`)
- [x] `nexus-connector-csv` — source+sink de texto delimitado (CSV/TSV/TXT com separador configurável), `uri` local ou `s3://`/`gs://`/`az://` via `object_store` (feature `csv`)
- [x] Conector sink `webhook` (dentro de `nexus-connector-rest`, feature `rest`) — API/webhook genérico de saída, method configurável (POST/PUT/PATCH/DELETE), `body_mode` array ou per-row, sem consciência de CDC (API arbitrária não tem semântica acordada pra `__opcode`)

## Fase 4 — CDC (escopo faseado, ver `ARCHITECTURE.md §7`) ✅
- [x] ~~CDC via Debezium + Kafka~~ — implementado, depois **removido** na Fase 18 (sem usuário dependendo dele; substituído pelo CDC nativo)
- [x] Resume automático a partir do checkpoint por partição em falha
- [x] CDC nativo (Postgres/MongoDB/MySQL, sem Debezium/Kafka) — deixou de ser condicional, ver Fase 18

## Fase 5 — AI Lakehouse (`nexus-ai`) ✅ (GPU não validada em hardware real)
- [x] Chunking (fixed-size, recursive, semantic)
- [x] Embeddings via `ort` (feature `cpu`) — `cuda`/`metal` registram o execution provider certo (compilam, não validados em GPU/Apple Silicon real) — `api` (endpoint HTTP compatível com OpenAI, feature independente de `cpu`) implementada e testada com mock
- [x] Node `embedding` no Canvas (fora do plano original, ver pendência #11 acima) — troca de backend (Onnx local ↔ API externa) e de estratégia de chunking direto na UI, sem precisar editar JSON à mão
- [x] Sinks vetoriais: pgvector → Qdrant → LanceDB → Milvus → Pinecone → ChromaDB (nessa ordem, do mais simples de operar ao mais complexo)

## Fase 6 — Data Lake formats ✅
- [x] Sink Parquet puro
- [x] Delta Lake (`deltalake`)
- [x] Iceberg (`iceberg-rust`)
- [x] AI-Lake (`nexus-connector-ailake`, formato próprio Parquet+HNSW — não estava no escopo original desta fase, adicionado depois)

## Fase 7 — `nexus-server` (API + Auth) ✅
- [x] Axum REST: CRUD de pipelines, execução, status
- [x] JWT + RBAC (`Read`/`Execute`/`Write`/`Admin`)
- [x] Segredos AES-256-GCM em repouso
- [x] WebSocket: progresso de execução em tempo real

## Fase 8 — Frontend (React Flow) ✅
- [x] Canvas node-based: criar/editar DAG, source of truth em JSON
- [x] Painel de execução em tempo real (MB/s, linhas/s, logs)
- [x] Tela de credenciais (sem exibir segredo em plain text)
- [x] Admin: gestão de usuários (criar/promover/excluir), visível só pra role Admin

## Fase 9 — Observabilidade & Alertas ✅
- [x] `tracing` estruturado (JSON) + OpenTelemetry (traces via OTLP + métricas Prometheus em `/metrics`)
- [x] Alertas assíncronos: Slack (Block Kit), MS Teams (Adaptive Card), PagerDuty (Events API v2), Email (SMTP STARTTLS), Webhook genérico (JSON puro)
- [x] Estatísticas de hardware (CPU/RAM via `sysinfo`) intercaladas no WebSocket de progresso a cada 2s — sem GPU (vendor-specific, nada depende disso ainda)

## Fase 10 — dbt (ELT opcional) ✅
- [x] Subprocesso assíncrono invocando `dbt run`/`build`/`test` pós-carga (feature `dbt`), com resultado de lineage/qualidade no histórico de execução

## Fase 11 — Distribuição multiplataforma ✅ (Windows/macOS validados em CI real, não numa máquina física de usuário)
- [x] Single binary com frontend embutido (`rust-embed`, feature `embed-ui`)
- [x] Empacotamento: AppImage/deb/rpm (Linux, todos testados, e desde a rodada de release CI abaixo buildados automaticamente em CI) — `.msi` (Windows) roda em CI (`build-windows-installer.yml`, dispara sozinho após cada release desde 2026-09-08, ver item 5 das Pendências ativas) e já foi instalado numa máquina Windows real (2026-09-06); Homebrew/dmg (macOS) roda em CI real desde 2026-09-06 (`build-macos-installer.yml`, `macos-latest`, 63m54s, `connectors-all` + todo conector enterprise) e dispara sozinho após cada release; nenhum dos dois teve um humano de fato instalando numa máquina física própria ainda. winget (Windows) continua sem spec/CI nenhum.
- [x] Imagem Docker com perfil `cuda` selecionável via `--build-arg RUNTIME_IMAGE` (base image + `--gpus all` prontos; aceleração real pendente da Fase 5's `cuda` feature), publicada no **Docker Hub** (`thiagolange/nexusflow`) a cada push pra `main` — GHCR foi descontinuado em 2026-09-08 (ver item 8 das Pendências ativas). Build amd64 com `FEATURES=embed-ui,connectors-all`.
- [x] Script de instalação `curl | sh` (`scripts/install.sh`) + `.github/workflows/release.yml`

## Fase 12 — Enterprise connectors / store de plugins pagos (paralelo, repo separado)

Mecanismo escolhido: **plugin compilado** (feature flag), não `.so`/dlopen em
runtime — o gate de license (`requires_license` em `ConnectorDescriptor`, já
existe em `nexus-core/registry.rs`) só faz sentido com o conector já linkado
no binário; runtime dylib exigiria ABI estável entre o Rust do server e do
plugin, que não existe nativamente. Estrutura: 2 repos — `nexusflow` (público,
sem mudança) + `nexus-connectors-enterprise` (privado, único — contém tanto
o(s) crate(s) de conector quanto um `main.rs` fino que depende de
`nexus-server` via git dependency). Ver `LICENSING.md` pro modelo de
licenciamento e `docs/ENTERPRISE_CONNECTORS.md` pro catálogo/priorização.

- [x] **Bloco 0 — Decisões e infra base**: mecanismo compilado + estrutura de
  2 repos decididos (acima). `POST /license`/`GET /license` + `LicenseStore`
  (JWT Ed25519, `crates/nexus-server/src/license.rs`/`license_store.rs`) e
  `submit_enterprise_connector!` (`nexus-core/registry.rs`, marca
  `requires_license: Some(slug)`) já implementados — nenhum crate real ainda
  chama o macro (só o teste unitário do próprio `registry.rs`).
- [x] **Bloco 1 — Enforcement real no `nexus-server` OSS**:
  `check_connector_license(name, active_license)` em `connectors.rs`,
  chamado no topo de `validate_source_config`/`validate_sink_config`/
  `build_source`/`build_sink` — `Option<LicenseClaims>` threaded desde
  `AppState.license_store` até cada call site (save, run, preview).
  Frontend: ícone de cadeado no `ConnectorPalette.tsx` pra `licensed:
  false` (hoje deixa configurar/arrastar livremente, bloqueia só no
  Salvar/Executar), aba **Store** nova (status de license, form de
  instalação Admin-only, catálogo "em breve"). Ver
  `docs/ENTERPRISE_LICENSING.md §5`.
  - [ ] **Follow-up: trial limitado em vez de "configura livre, bloqueia só
    no fim"** (decisão do usuário, 2026-08-18 — implementar depois de
    validar o Excel end-to-end). Problema com o comportamento atual: o
    cadeado no `ConnectorPalette.tsx` é só decorativo — o conector aparece
    arrastável/configurável sem license nenhuma, e o único bloqueio real
    (`check_connector_license`) só dispara em Salvar/Executar/Preview. Isso
    dá pra testar a config sem nunca salvar, o que é aceitável — mas não dá
    pra confundir "nunca salvou" com "limite de uso", porque não há limite
    nenhum: um pipeline ad-hoc (`POST /pipelines/run` sem `pipeline_id`
    salvo — ver `tests::run_ad_hoc_*`) já passa pelo mesmo
    `check_connector_license` de qualquer run salvo, então hoje isso *já*
    bloqueia ad-hoc sem license — mas se o usuário mantiver a aba aberta e
    ficar clicando Executar repetidas vezes sem nunca dar Save, não existe
    nenhum contador que pare esse loop antes da license real.
    **Comportamento desejado**: conector sem license cobrindo não aparece
    no `ConnectorPalette.tsx` por padrão (só na Store). Store mostra dois
    botões pro conector bloqueado: **Testar** (inicia um trial com limite
    real, rastreado no servidor) e **Comprar**. Só depois de "Testar" o
    conector aparece liberado no Canvas, mas com um teto de uso de verdade
    — não "enquanto não salvar" (que não é limite nenhum).
    **Esboço de implementação** (decisões a fechar quando for construir):
    - Um jeito de emitir a license de trial sem depender do Bloco 2/gateway
      de pagamento — `nexus-server` pode assinar ele mesmo uma
      `LicenseClaims` de trial localmente (endpoint novo tipo `POST
      /license/trial`), já que trial é grátis e não precisa de
      cobrança/nota fiscal.
    - `LicenseClaims` (ou uma tabela nova em `license_store.rs`) precisa
      carregar o teto do trial (ex. `trial_max_runs: Option<u32>`) e um
      contador persistido, incrementado a cada execução real (dentro de
      `build_source`/`build_sink`, não em `validate_source_config` —
      validar/salvar não deveria gastar cota de trial). Ao bater o teto,
      `check_connector_license` passa a rejeitar mesmo com a license de
      trial ainda "instalada" — mesma mensagem de erro de license ausente,
      só que apontando pra comprar em vez de instalar.
    - Store precisa mostrar quanto do trial já foi usado (`GET /license`
      já devolve as claims decodificadas — só falta o contador).
- [ ] **Bloco 2 — Infra `nexus-licensing`**: serviço separado (repo privado)
  que emite as license keys — Mercado Pago (Checkout Pro), webhooks v2,
  NFe/NFSe via NFE.io/eNotas. Design completo em `docs/ENTERPRISE_LICENSING.md`.
  Não bloqueia o Bloco 1 (licenses de teste geradas na mão já bastam pra
  validar o enforcement).
- [x] **Bloco 3a — Ponto de extensão de plugin (pré-requisito)**: até aqui,
  `build_source`/`build_sink`/`validate_source_config`/
  `validate_sink_config` (`nexus-server/src/connectors.rs`) eram um `match
  node.connector.as_str()` fechado — um crate de conector fora do
  workspace (repo privado) não tinha como adicionar um arm a esse `match`.
  `nexus-core/registry.rs` ganhou `SourceBuilder`/`SinkBuilder`
  (`submit_source_builder!`/`submit_sink_builder!`, mesmo padrão
  `inventory` do `ConnectorDescriptor`); o `other` arm de cada uma das 4
  funções acima cai nesse registry antes de rejeitar. Ver `ARCHITECTURE.md
  §3`. Sem isso, nenhum conector enterprise real conseguia rodar via
  binário compilado separado — não é específico do Excel, destrava
  qualquer conector futuro do Bloco 3.
- [x] **Bloco 3b — Primeiro conector pago: Excel**: `.xlsx` source + sink
  (leitura/escrita local ou S3/GCS/Azure, mesma UX de campos separados do
  `csv`, seleção de aba/sheet) — prioridade tier-2 em
  `docs/ENTERPRISE_CONNECTORS.md` (baixa barreira técnica, alto volume em
  PME). Repo privado `nexus-connectors-enterprise` criado e ativo — bem
  além do escopo original de "primeiro conector": **37 crates** hoje
  (contagem real via `Cargo.toml` do repo, 2026-09-05 — número sobe com
  frequência, ver `docs/ENTERPRISE_CONNECTORS.md` pra lista viva por
  categoria em vez de um total fixo aqui) — Excel, BigQuery, Snowflake,
  Redshift, Synapse, MSSQL/MSSQL CDC, Oracle/Oracle LogMiner CDC, SAP
  HANA, Teradata, Vertica, Salesforce, HubSpot, Zendesk, ServiceNow,
  Dynamics 365, NetSuite, Workday, SharePoint, Dropbox, Google
  Sheets/Drive, Shopify, Stripe, Meta/Google/LinkedIn/TikTok/X Ads, GA4,
  YouTube Analytics, Kinesis, Pulsar, Starburst (Trino), Databricks,
  Elasticsearch/OpenSearch, Weaviate, Azure AI Search, Vertex AI Vector
  Search — ver `docs/DOCKER_LOCAL_TESTING.md` desse repo pra lista
  completa com campos/exemplo de config por conector.
- [x] **Bloco 5 — Gate de capabilities não-conector (LLMOps Marco L8)**:
  `"llm-lineage-tracking"` (`GET /lineage/generation/{id}`) e
  `"reactive-rag-cdc"` (`*-cdc` source + `embedding` no passthrough)
  reaproveitam o enforcement do Bloco 1 (`check_connector_license`), mas
  registrados via `submit_enterprise_connector!` dentro do próprio
  `nexus-server` (`capability_registry.rs`), não num crate privado —
  esse código já roda sempre no binário público, diferente de um
  conector real que só existe quando o crate enterprise está linkado.
  `ConnectorCapability` ganhou uma 4ª variante (`Capability`) só pra
  esses dois, filtrada de `GET /connectors` (nunca vira node type no
  Canvas). Ver `docs/ENTERPRISE_LICENSING.md §5`.
- [ ] **Bloco 4 — Storefront mínimo**: página de venda + checkout, mesmo que
  simples (Mercado Pago Checkout Pro cobre a parte de pagamento sem UI
  custom pra dado de cartão).

## Fase 13 — Todos os conectores linkados no binário (fora do plano original)
- [x] Os conectores do workspace aninhado `crates/nexus-connectors` (20 batch + 6 CDC nativos = 26 nomes de catálogo) agora também são feature opcional em `nexus-server` (`connectors-all`), aparecendo de verdade no catálogo `GET /connectors` — antes só postgres/sqlite estavam linkados no binário servido pra UI.

## Fase 14 — Formulário de config por schema real (fora do plano original)
- [x] Cada Config struct de conector deriva `schemars::JsonSchema`; `GET /connectors` expõe esse schema (`config_schema`). Canvas renderiza um formulário real (`SchemaForm.tsx`, recursivo: texto/número/boolean/enum/array-de-objeto) em vez de pedir JSON escrito à mão — descrições vêm dos doc comments do Rust. Ver `ARCHITECTURE.md §3`.

## Fase 15 — Agendamento automático + gestão completa de pipelines no Canvas (fora do plano original)
- [x] `PipelineSpec.schedule` (cron 5 ou 6 campos) + scheduler em background no `nexus-server` (poll de 30s), reusando o mesmo caminho de execução do run manual (histórico/dbt/alertas idênticos). Validado end-to-end com servidor real rodando: disparo automático sem nenhuma chamada manual a `/run`. Ver `ARCHITECTURE.md §12`.
- [x] Canvas ganha **Save** (criar/atualizar), **Edit** (recarrega config completa de um pipeline salvo, incl. segredos de conector, via `GET /pipelines/{id}/spec` — role `Write`) e mantém **Delete** — antes só dava pra montar/rodar um pipeline no Canvas sem nunca conseguir persisti-lo.
- [x] Aba "Status": lista todos os pipelines salvos com flag verde/amarelo/vermelho/cinza (sucesso/em execução/falha/nunca rodou), baseado em `last_run_status`/`last_run_at` novos em `PipelineSummary`.

## Fase 16 — Preview de dados + dbt como ETL real (fora do plano original)
- [x] `GET /pipelines/{id}/preview?node={resolved_name}&limit={n}` — primeiras N linhas (default 50, teto 500) de qualquer node source/sink de um pipeline persistido, reusando `build_source`; conector sink-only devolve 400 com mensagem clara. Backend-only por ora, sem botão no Canvas. Ver `ARCHITECTURE.md §13`.
- [x] dbt deixa de ser só ELT: `DbtConfig.output` + `PipelineSpec.post_dbt_sinks` fecham o ciclo `Source → carga bruta → dbt transforma → lê resultado de volta → Sink final` num `run` só, sem precisar de um segundo pipeline manual. Testado com Postgres real (testcontainers) + `dbt-fusion` CLI real end-to-end (`crates/nexus-server/tests/dbt_etl_pipeline.rs`). Canvas: node dbt ganhou handle de saída (consistência visual); painel de config do destino (`dbt.output`) ainda não implementado — configuração via API/JSON só.

## Fase 18 — CDC nativo: Postgres, MongoDB e MySQL, sem Debezium/Kafka (fora do plano original)

O Marco 13 do roadmap original deixava CDC nativo condicional — só entraria se o overhead de operar Debezium+Kafka virasse bloqueador real de adoção confirmado. Esse sinal chegou (hardware mais simples não aguenta 3 JVMs rodando). Ver `ARCHITECTURE.md §7`.

- [x] `postgres-cdc` (feature `cdc` do `nexus-connector-postgres`, crate `pg_walstream`) — lê direto do protocolo de replicação lógica do Postgres (`pgoutput`). O replication slot é criado automaticamente no primeiro connect; a publicação (`CREATE PUBLICATION ... FOR TABLE ...`) precisa existir de antemão (não criada automaticamente, mesmo pré-requisito operacional que o Debezium já exige). Resume via o próprio slot — Postgres guarda o ponto server-side, sem precisar de checkpoint externo. Correção real feita depois: `event_stream.update_applied_lsn(...)` não era chamado (bug real — o slot nunca avançava `confirmed_flush_lsn`, WAL acumulava sem limite e todo restart reprocessava desde a criação do slot); chamado agora a cada evento. Testado com Postgres real via testcontainers.
- [x] `mongodb-cdc` (mesmo crate `nexus-connector-mongodb`, sem dependência nova) — Change Streams nativo do driver oficial (`Collection::watch()`). Requer MongoDB como replica set (mesmo single-node serve). Testado com MongoDB real via testcontainers — inclusive um caso real onde `full_document: updateLookup` não retorna o documento atualizado (fallback pra `document_key`, linha não é descartada).
- [x] `mysql-cdc` (novo crate `nexus-connector-mysql`, dependência `mysql_cdc`) — lê o binlog direto, CDC-only (sem modo batch). Colunas mapeadas **posicionalmente** (protocolo binlog não carrega nome de coluna por padrão), diferente de Postgres/MongoDB que casam por nome. Requer `binlog_format=ROW`/`binlog_row_image=FULL` e um usuário com `REPLICATION SLAVE`/`REPLICATION CLIENT`. Testado com MySQL real via testcontainers.
- [x] **Resume real de checkpoint pra `mysql-cdc`/`mongodb-cdc`** (fora do plano original — mysql/mongo tinham campo de config pra retomar posição, mas nada capturava/persistia a posição entre runs): `Source::position_handle()` (novo método default-`None` em `nexus-core::traits`, não quebra os outros conectores) devolve um `Arc<Mutex<Option<String>>>` que o source atualiza a cada evento (`"{filename}:{position}"` no MySQL, resume token serializado no Mongo); `CheckpointCursor.resume_state`/`CheckpointStore::get()` persistem e leem essa posição; `runner.rs::run_passthrough_pipeline` injeta o valor de volta na config (`binlog_filename`/`binlog_position` no MySQL, `resume_token` no Mongo) antes de reconectar, e evita marcar a partição CDC como "já feita" (que impediria reexecução). `mssql-cdc`/`oracle-cdc` (repo privado enterprise) ganharam o mesmo mecanismo em seguida — os 5 CDC nativos que existem hoje retomam de verdade.
- [x] `nexus-connector-kafka` continua existindo — deixa de ser o único caminho de CDC, segue disponível só como fonte genérica de Kafka (sem semântica de CDC).
- [x] Canvas: os 3 novos conectores aparecem no catálogo dinâmico (`GET /connectors`) automaticamente, e o `NodeInspector` ganhou um toggle Batch/CDC no mesmo node (Postgres/MongoDB) — troca `data.connector` entre `postgres`↔`postgres-cdc`/`mongodb`↔`mongodb-cdc` e limpa o config (campos não são compatíveis entre os dois modos). Só aparece pra `role: source` (nenhum CDC nativo tem `Sink`) e só se os dois nomes existirem no catálogo real (não hardcoded — some se o binário não tiver a feature `cdc` linkada). MySQL não tem toggle: `mysql-cdc` não tem um `mysql` batch pra alternar (CDC-only).
- [x] **Debezium+Kafka removido** (pós-Fase 18, sem usuário dependendo dele): envelope `Debezium` de `nexus-connector-kafka` (código + testes unitários), teste de integração `cdc_debezium_integration.rs` (3 JVMs via testcontainers) e `docs/cdc-reference/` deletados. `nexus-connector-kafka` mantido só como fonte genérica de Kafka.

## Fase 17 — Backend Postgres pros metadados + leader election + migração (fora do plano original, motivado por prontidão k8s)

- [x] Os 3 metadata stores (`auth_store`, `pipeline_store`, `checkpoint_store`) — antes travados em SQLite (feature `postgres` do `sqlx` só existia em `[dev-dependencies]`) — agora suportam Postgres via `NEXUS_CHECKPOINT_DB`/`NEXUS_AUTH_DB`/`NEXUS_PIPELINES_DB` apontando pra uma URL `postgres://`/`postgresql://`, detectado automaticamente pelo scheme (`db::MetadataPool`). Pré-requisito real pra rodar >1 réplica em k8s — SQLite não pode ser compartilhado com segurança entre réplicas. Ver `ARCHITECTURE.md §14`.
- [x] Leader election do scheduler de cron via `pg_try_advisory_lock` do Postgres (sem infra nova tipo etcd/Redis) — sem isso, >1 réplica lendo o mesmo Postgres dispararia cada pipeline agendado em dobro. No-op em SQLite.
- [x] Ferramenta de migração (`cargo run --bin migrate-metadata`) copia usuários/pipelines/histórico/checkpoints de SQLite pra Postgres preservando IDs, idempotente. `spec_ciphertext` copiado byte a byte — exige mesma `NEXUS_ENCRYPTION_KEY` nos dois lados.
- [x] Testado com Postgres real via testcontainers: os 3 stores, leader election (2 "réplicas" disputando o lock, failover ao perder conexão) e a ferramenta de migração (IDs não-sequenciais + `setval` da sequence).
- [x] Manifests k8s reais escritos na Fase 19, ver abaixo.

---

## Fase 19 — Manifests de deployment: Kubernetes e Docker Swarm

- [x] `packaging/kubernetes/`: Deployment (2 réplicas, `securityContext` não-root uid 1001 alinhado ao Dockerfile, liveness/readiness em `/health`), Service (ClusterIP), ConfigMap + Secret (template) pras env vars do `GETTING_STARTED.md §3`, PVC opcional pro cache de embeddings (`XDG_CACHE_HOME`, `hf_hub` já honra essa env var sem mudança de código), HPA (CPU, requer metrics-server), `kustomization.yaml` amarrando tudo. Validado offline com `kubeconform` (schema K8s 1.29) — não testado num cluster gerenciado real.
- [x] `packaging/swarm/docker-stack.yml`: mesmo binário/imagem, `replicas: 2`, healthcheck herdado do Dockerfile, `update_config`/`restart_policy`. Sem secret nativo do Swarm (monta como arquivo, `nexus-server` só lê env var) — usa substituição `${VAR}` do compose, injetado no shell do `docker stack deploy`. Validado com `docker compose config` — não testado num swarm multi-node real.
- [x] Nenhuma mudança em `nexus-server`/`nexus-core` — só manifests de infra em cima do que a Fase 17 (Postgres + leader election) já tornou seguro.
- [x] Documentado o trade-off do HPA/autoscaling: escalar pra baixo mata runs de pipeline em voo daquele pod/container (`shutdown_signal` não espera supervisors de run já disparados) — recuperável via checkpoint por partição (`ARCHITECTURE.md §5`), não é perda de dado, mas não é limpo.
- [x] Sem Helm chart, sem Ingress/TLS, sem manifest de Postgres — de propósito (specific ao ambiente do operador); ver `packaging/kubernetes/README.md`/`packaging/swarm/README.md`.

**Critério de pronto:** manifests aplicáveis (`kubectl apply -k` / `docker stack deploy`) sem erro de schema, documentação do pré-requisito Postgres + trade-offs de autoscaling. **Atingido** (validação offline; validação num cluster/swarm real fica pro operador, fora do escopo de CI deste repo).

---

## Fase 20 — Logs de execução por run no Canvas

- [x] `nexus_server::progress::RunLogEvent`/`RunLogger` — narração textual (info/warn/error) de um run, emitida via broadcast (ao vivo) **e** persistida em `RunLogStore` (tabela nova `pipeline_run_logs`, mesmo padrão dual-dialeto do `MetadataPool`). A persistência acontece na emissão, não no forwarding pro WebSocket — evita duplicar linha por subscriber conectado. Ver `ARCHITECTURE.md §15`.
- [x] Motivador: um run disparado pelo scheduler não tinha ninguém com o WebSocket de progresso aberto pra ver o que aconteceu, e o canal de broadcast morre junto com o run — sem persistência, não tinha como inspecionar depois.
- [x] `GET /pipelines/{id}/runs/{run_id}/logs` (role `Read`) — replay completo, funciona pra run em andamento, terminado ou agendado.
- [x] `nexus-core` intocado de propósito: em vez de mudar o tipo público `ProgressEvent`/`ProgressSender` (usado em ~10 testes do crate), o log viaja num `broadcast::channel` separado só dentro de `nexus-server`.
- [x] Pontos de emissão: início/fim de run, contagem de partições/sources/sinks, falha de connect por partição/source/sink, etapas do dbt, resumo final (linhas/partições ou erro sanitizado — mesmo `error::sanitize_error` de sempre).
- [x] Canvas: `ExecutionPanel` ganhou modo terminal expansível (frames `type: "log"` do mesmo WebSocket, sem socket novo); `RunHistoryPanel` ganhou botão "Logs" por execução, alimentado por `GET .../logs` (`useRunLogs`) — funciona pra qualquer run, inclusive um agendado que ninguém acompanhou ao vivo.

**Critério de pronto:** log de execução visível no Canvas pra um run manual (ao vivo) e pra um run agendado inspecionado depois pelo histórico — testado via integração real (`run_logs_endpoint_replays_start_and_failure_lines_after_the_run_finished`). **Atingido.**

---

## Fase 21 — CDC nativo pra Delta Lake, Iceberg e AI-Lake (lakehouse)

Extensão da Fase 18 pros formatos de data lake que já tinham conector batch. Ver `ARCHITECTURE.md §16` pro detalhe técnico completo (pegadinhas reais de cada um, descobertas via teste de integração — não hipotéticas).

- [x] `deltalake-cdc` — Change Data Feed nativo (`DeltaTable::scan_cdf()`), sem dependência nova. Esforço baixo: a biblioteca já resolve o trabalho difícil (decodificação do log de transação); só precisou ordenar por `_commit_version` antes de processar (DataFusion não garante ordem de commit no resultado).
- [x] `iceberg-cdc` — sem scan incremental nativo no `iceberg` 0.10.0, construído à mão (manifest list + manifest walk via API pública do crate). **Insert-only**: `IcebergSink` só comita `fast_append` hoje (sem row-delta/equality-delete commitável na API pública ainda), então não existe update/delete pra detectar de dados escritos por este sistema.
- [x] `ailake-cdc` — mais simples que o Iceberg porque `ailake-catalog`'s `CatalogProvider` já expõe `list_files`/`list_equality_deletes` "as of snapshot", dispensando manifest walk manual. Suporta `I`/`D` reais (`AilakeSink::delete` já comita equality-deletes) — `U` não é inferido de propósito (sem informação de ordem entre insert/delete da mesma chave, o delete sempre vence). Achado à parte: `AilakeSink::upsert` (batch sem `__opcode`) é append cego hoje, não faz delete-antes-do-insert como o `DeltaSink` — duas escritas da mesma chave viram duas linhas físicas.
- [x] Resume via campo estático no config (`starting_version`/`starting_snapshot_id`), sem auto-avanço via checkpoint entre runs — mesmo precedente do `start_offsets` do Kafka. **Diferente dos 3 CDC nativos da Fase 18** (que ganharam resume automático de verdade depois, ver Fase 18 acima) — os 3 formatos de lake aqui ainda não têm `position_handle`/checkpoint automático, é trabalho pendente se algum usuário precisar.
- [x] Nenhuma dependência nova em nenhum dos 3 — tudo via API já pública das dependências existentes de cada conector.

**Critério de pronto:** teste de integração real por conector (escrever insert/update/delete via o sink batch já existente, ler de volta via a fonte CDC nova, validar opcode e valores) — sem testcontainers, os 3 formatos já são embarcados/locais. **Atingido.**

---

## Fase 22 — Conector MQTT (telemetria IoT/sensor)

Protocolo padrão de telemetria IoT (AWS IoT Core, Azure IoT Hub, HiveMQ, Mosquitto falam todos MQTT nativamente) — mesmo critério de "protocolo aberto sem lock-in" que já justificava `kafka` como OSS, não enterprise.

- [x] `nexus-connector-mqtt` (feature `mqtt` + `nexus-connector-mqtt/client`, dependência `rumqttc`) — mesmo padrão arquitetural do `kafka`: `read_batches` faz `tokio::time::timeout` sobre o eventloop assíncrono do broker, bufferizando até `max_messages`/`poll_timeout_ms`, transformando o modelo *push* do MQTT no modelo *pull* que o engine espera.
- [x] `topic_filter` aceita wildcard MQTT (`+`, `#`) — uma subscription pode misturar vários sensores lógicos numa leitura só, então toda linha ganha a coluna extra `__mqtt_topic` com o tópico exato de onde veio (mesmo precedente do `__opcode` em CDC).
- [x] **Resume é 100% server-side, achado real**: sessão persistente do MQTT (`clean_session: false` + `client_id` fixo, sempre ligado) faz o broker guardar mensagens QoS 1/2 publicadas offline e reentregar na reconexão — sem `Source::position_handle`/`CheckpointCursor` nenhum, mesmo padrão do `postgres-cdc` (replication slot) e do `kafka` (offset de consumer group).
- [x] TLS com CA privada/mTLS (`ca_cert_path`/`client_cert_path`/`client_key_path`) — necessário pra brokers cloud que exigem client-cert (AWS IoT Core sempre exige).
- [x] Testado com broker Mosquitto real via testcontainers (`testcontainers-modules` feature `mosquitto`) — publica em múltiplos tópicos sob wildcard, valida batch + coluna `__mqtt_topic`.
- [ ] Fora de escopo v1: payload binário/CBOR (só JSON), MQTT 5 (usa `rumqttc` padrão = 3.1.1), sink MQTT (publicar em vez de assinar — sem caso de uso claro hoje).

**Candidato relacionado, não implementado**: OPC-UA (protocolo industrial/SCADA) — comprador diferente (chão de fábrica, disposto a pagar, mesmo padrão de Oracle/SAP), complexidade de protocolo maior (modelo de informação tipado, não é só pub/sub). Registrado como candidato enterprise em `docs/ENTERPRISE_CONNECTORS.md`, decisão de implementar fica com o usuário.

**Critério de pronto:** broker real (Mosquitto via testcontainers), publica telemetria fake em múltiplos tópicos sob um wildcard, `MqttSource` consome e devolve linhas com `__mqtt_topic` correto. **Atingido.**

---

## Fase 23 — Conector ClickHouse (ADBC nativo, repo público)

Estava registrado como candidato enterprise em `docs/ENTERPRISE_CONNECTORS.md` sob a premissa "ADBC básico OSS, avançado pago" — investigação numa sessão anterior derrubou essa premissa: RBAC e cluster mode (`Distributed`/`Replicated`, ClickHouse Keeper) são recursos OSS do próprio ClickHouse self-hosted, não existe feature "avançada" genuína pra reservar como paga (diferente de Snowflake/Oracle/SAP, que têm licenciamento pago real). Driver ADBC também é oficial (ClickHouse, Inc.) e grátis. Decisão: vai pro repo público, mesma categoria de Postgres/SQLite.

- [x] `nexus-connector-clickhouse` (feature `clickhouse`) — mesmo esqueleto ADBC do `nexus-connector-postgres` (`driver.rs`/`config.rs`/`source.rs`/`sink.rs`), única option key `uri` confirmada contra a doc real do driver (adbc-drivers.org/drivers/clickhouse/), não múltiplas chaves como Snowflake.
- [x] Instalação de um comando só (`dbc install clickhouse`, ADBC Driver Foundry) — diferente de Postgres/SQLite, que exigem compilar `libadbc_driver_*.so` na mão.
- [x] **Sink append-only, achado real**: ClickHouse não tem `ON CONFLICT`/upsert leve (`ALTER TABLE ... UPDATE/DELETE` são mutations assíncronas pesadas). `write_batch` rejeita explicitamente batches de CDC com `__opcode` de delete em vez de descartar silenciosamente — dedup fica a cargo do usuário via `ReplacingMergeTree`/`CollapsingMergeTree`, mecanismo idiomático do próprio ClickHouse.
- [x] `partition_column` em vez de `primary_key` (nome do Postgres, implica unicidade que o ClickHouse não impõe) — qualquer coluna orderável usada só pra particionar leitura em paralelo.
- [ ] Sem teste de integração real (mesma ressalva de todo conector ADBC do repo — nem `postgres` tem, `ADBC_DRIVER_POSTGRESQL_PATH` também não é setado em CI). Só unit tests dos SQL builders.

**Critério de pronto:** `cargo test -p nexus-connector-clickhouse` cobrindo os SQL builders (incluindo rejeição de SQL injection), `cargo build --features connectors-all` linkando o conector, `GET /connectors` listando `clickhouse`. **Atingido** (sem validação contra instância ClickHouse real — mesma ressalva que Snowflake/BigQuery/Databricks já carregam).

---

## Fase 24 — Expansão de conectores (gap analysis, OSS + enterprise)

Motivada por uma análise de lacunas nesta sessão: conectores de streaming existiam só como source (nunca publicavam), e vários candidatos do `docs/ENTERPRISE_CONNECTORS.md` seguiam sem crate por falta de esforço, não por falta de demanda. Escopo: implementar tudo que fizesse sentido, exceto Db2 (exclusão explícita do usuário) e SAP BAPI/IDoc (achado durante a implementação: bloqueio legal, não técnico — SDK NetWeaver da SAP é proprietário e não redistribuível sem licença comercial direta, sem caminho Rust possível).

**OSS (`crates/nexus-connectors/`):**
- [x] Kafka ganhou sink (produtor via `rdkafka`, feature `producer`) — só tinha source antes.
- [x] `duckdb` — fast-path ADBC oficial (`dbc install duckdb`), upsert real via `ON CONFLICT`, diferente do append-only do ClickHouse.
- [x] `redis` — Streams (`XADD`/`XREAD`), não KV genérico; sem consumer group em v1.
- [x] `nats` — pub/sub core, não JetStream (sem persistência/replay).
- [x] `rabbitmq` — AMQP 0-9-1, sempre auto-ack no source (sem redelivery manual em v1).

**Enterprise (`nexus-connectors-enterprise`, repo privado, 24 crates novos/alterados):**
- [x] Kinesis e Pulsar ganharam sink (mesma lacuna do Kafka).
- [x] Teradata, Vertica — mesmo esqueleto ODBC do HANA; Teradata usa `UPDATE ... ELSE INSERT` (sem `MERGE` nativo), Vertica usa `MERGE` real.
- [x] HubSpot, Zendesk, Google Sheets — REST/JSON, testados com `wiremock`, mesmo padrão do Salesforce/Stripe.
- [x] Dropbox, Google Drive — **acharam o próprio padrão** durante a implementação: em vez de listar metadado de arquivo como linha, reusam o parsing `arrow-csv` do `nexus-connector-csv` público contra uma pasta com CSV/TSV, já que `object_store` não tem backend pra essas duas nuvens.
- [x] ServiceNow, Dynamics 365, SharePoint — REST/OAuth mais complexo (paginação OData v4 via `@odata.nextLink` em Dynamics/SharePoint, offset simples no ServiceNow).
- [x] NetSuite — SuiteQL (não a SOAP SuiteTalk antiga) pra leitura, REST Record API pra escrita; schema inferido dinamicamente das colunas da query (sem `fields` fixo).
- [x] Workday — **source-only, permanente**: RaaS (Report-as-a-Service) cobre leitura real, mas o write-path de verdade do Workday é SOAP (`Put_Worker` etc.), superfície de protocolo separada e muito maior, não uma simplificação de v1 — mesmo racional do Stripe ser read-only, só que por limitação técnica em vez de decisão de produto.

**Achados reais durante a verificação** (não só desenvolvimento): um `| tail` mascarando exit code de pipe escondeu 3 bugs reais por um tempo (variant `redis::Value::Data` renomeado pra `BulkString` na 0.27; `hasMore` do NetSuite sem `#[serde(rename)]`, quebrando paginação silenciosamente; `needless_range_loop`/`if_same_then_else` do clippy em 3 crates) — todos recorrigidos após passar a rodar tudo com exit code real (redirecionado a arquivo, sem pipe). Dois bugs de clippy pré-existentes (não relacionados a este trabalho) em `mssql-cdc`/`oracle-cdc` também corrigidos a pedido do usuário.

**Critério de pronto:** `cargo check`/`test`/`clippy -D warnings` limpos (exit code real, sem `| tail`) no workspace inteiro dos dois repos, `GET /connectors` listando cada conector novo. **Atingido** — 19 itens implementados e commitados (6 ondas), nenhum contra conta/tenant real (mesma ressalva de todo conector REST/ODBC deste repo — só `wiremock`/unit tests).

---

## Fase 25 — Aba Infra: Canvas visual pra Terraform (AWS) — planejado, não implementado

Usuário pediu uma aba nova: desenhar infraestrutura AWS num Canvas
visual (caixinha por recurso, clicar traz a config necessária) e
gerar Terraform. Pesquisado e planejado numa sessão anterior, execução
fica pra um próximo passo — registrado aqui pra não perder o
levantamento.

**Achado que muda a estimativa de esforço pra baixo**: o mecanismo
inteiro já existe, só nunca foi usado fora do domínio "conector de
dado". `nexus_core::registry::submit_connector!` (macro `inventory` +
`schemars::schema_for!`) já gera schema JSON automático de qualquer
struct Rust com `Deserialize + JsonSchema` e expõe via catálogo — é
exatamente "clicar na caixinha, trazer a config necessária", só que
hoje só serve conector. `frontend/src/components/SchemaForm.tsx` já é
100% genérico (renderiza formulário de qualquer JSON Schema desse
formato, zero acoplamento a "conector"). `ConnectorPalette.tsx`/
`DagCanvas.tsx` já têm o padrão de paleta arrastável + Canvas editável
+ salvar/carregar spec. Não precisa reinventar nenhum dos três — só
aplicar o mesmo padrão a um domínio novo.

**Decisão de escopo/segurança, já validada com o usuário**:
- **Só desenho + `terraform plan`** — mostra o que mudaria, nunca
  aplica. `apply`/`destroy` fica de fora, sem nenhum code path pra
  isso (fronteira de código, não de UI — não é botão escondido, é
  capacidade que não existe).
- `terraform plan` precisa de credencial AWS real (chama a API pra
  saber o estado atual e computar o diff), mas uma credencial
  só-leitura (`Describe*`/`List*`/`Get*`) não cria nem destrói nada —
  o `plan` em si é seguro mesmo com credencial real, desde que a
  credencial tenha esse escopo.
- Credencial AWS nunca passa pela API do NexusFlow nem é persistida
  em spec — vem de env var do processo do `nexus-server`
  (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_PROFILE`, o jeito
  que o próprio Terraform já lê por padrão), mesmo princípio que
  `DBT_PROFILES_DIR` já usa hoje pra dbt.
- **Recursos**: as 3 categorias pedidas (dados/streaming, banco de
  dados, VMs/compute). Fazer os ~10+ recursos AWS de uma vez é grande
  demais pra um PR — proposto começar com 6 representativos que
  cobrem as 3 categorias E provam os dois casos reais que existem:
  recurso standalone (`aws_s3_bucket`, `aws_dynamodb_table`) e
  recurso com referência cruzada (`aws_db_instance`/`aws_instance`
  dependendo de `aws_vpc`/`aws_subnet`/`aws_security_group`).
  Kinesis/Glue/MSK/Redshift ficam como expansão incremental depois,
  mesmo mecanismo, baixo risco (mesmo padrão que a allowlist de
  recurso da aba Linhagem já estabeleceu).

**Desenho técnico levantado** (detalhe completo ficou só no plano
efêmero da sessão original, resumo aqui):
- Novo crate `crates/nexus-infra/` — um struct por recurso
  (`Deserialize + Serialize + JsonSchema`), campo que referencia outro
  nó do Canvas usa sufixo `_ref` (convenção pro frontend desenhar
  select-de-nó em vez de texto livre, e pro backend resolver pro
  endereço Terraform certo `aws_subnet.<node_name>.id`). `to_hcl()`
  por recurso — função pura, testável sem terraform instalado, mesmo
  padrão dos SQL builders de conector. Registro paralelo ao de
  conector: `InfraResourceDescriptor` + `submit_infra_resource!`,
  mesmíssimo mecanismo `inventory`/`schemars` de `registry.rs`.
- `InfraSpec` — grafo genérico nó+aresta (mesma forma de
  `lineage::LineageGraph`), não o par fixo source/sink do
  `PipelineSpec`, porque dependência de infra não tem forma fixa.
- Backend: `InfraStore` (mesmo padrão dual-dialeto de
  `pipeline_store.rs`), `GET /infra/resources` (catálogo),
  CRUD `/infra`, `POST /infra/{id}/plan` (monta HCL, roda
  `terraform init -backend=false` + `terraform plan -no-color` como
  subprocess — mesmíssimo padrão de `dbt::run()`).
- Frontend: `InfraCanvas.tsx` (mesma estrutura de `DagCanvas.tsx`,
  mas com `onConnect` de verdade — usuário desenha a aresta de
  dependência arrastando, diferente da Linhagem read-only),
  `ConnectorPalette`/`NodeInspector`/`SchemaForm` reaproveitados quase
  sem alteração.
- Terraform confirmado instalado no ambiente de dev (`v1.15.9`) —
  verificação de ponta a ponta consegue chegar até `terraform init`/
  `terraform validate` (não precisa de credencial AWS), só não chega
  em `terraform plan` de verdade sem conta AWS real pra testar contra
  — mesma ressalva de "sem conta real pra validar" que outros
  conectores desta sessão já carregam.

---

## Fase 25 — Catálogo de dados navegável e buscável ✅

Registro pesquisável de datasets, usando a mesma identidade estável (`resource_identifier`) que `lineage.rs` já calcula, com descrição/tags/owner por dataset e metadado por coluna (incluindo `pii_flag` **manual** — sem heurística automática nesta rodada).

- [x] `crates/nexus-server/src/data_catalog.rs` — `CatalogStore` dual-dialeto (Sqlite/Postgres), mesmo padrão de `pipeline_schema_store.rs`. Índice materializado por evento (upsert a cada run bem-sucedido), diferente do grafo recomputado-por-request de `lineage.rs` — divergência deliberada, necessária pra busca/filtro funcionar.
- [x] `GET /catalog/datasets?q=&tag=&connector=&owner=&has_pii=`, `GET /catalog/datasets/{key}`, `PUT /catalog/datasets/{key}`, `PUT /catalog/datasets/{key}/columns/{column}`, `GET /catalog/tags`.
- [x] 7 testes cobrindo upsert por evento, filtros combinados e persistência dual-dialeto.

**Critério de pronto:** rodar pipeline → dataset aparece em `GET /catalog/datasets`; description/tag/pii_flag persistem entre restart (Sqlite e Postgres); filtro por tag/owner/connector/has_pii funciona. **Atingido.**

## Fase 26 — Orquestração cross-pipeline ✅

`PipelineSpec` ganha `depends_on` com modo configurável (`any` dispara no primeiro upstream que terminar; `all` espera todos no mesmo epoch) — dispara automaticamente os dependentes quando o upstream termina com sucesso.

- [x] `crates/nexus-server/src/pipeline_dependencies.rs` — resolve quem depende de um `pipeline_id` que acabou de suceder; modo `all` usa tabela append-only `pipeline_dependency_state` (PK natural + `ON CONFLICT DO NOTHING`, mesmo idioma de `quality_check_results`).
- [x] Ciclo cross-pipeline detectado em `PipelineSpec::validate()` (rejeitado ao salvar, não só em runtime).
- [x] `GET /pipelines/{id}/dependents`, `GET /pipelines/{id}/dependencies`, `GET /orchestration/graph`.
- [x] 9 testes, incluindo não-duplo-disparo com múltiplas réplicas (mesmo espírito do teste de leader election do `scheduler.rs`).

**Critério de pronto:** ciclo rejeitado ao salvar; A→B modo `any` dispara B uma vez; A,B→C modo `all` só dispara depois de A e B no mesmo epoch. **Atingido** — validado ao vivo nesta sessão (A→B disparando em ~1s).

## Fase 27 — Observabilidade proativa / detecção de anomalia ✅

Anomalia estatística de volume por z-score, navegável em termos de dataset/coluna (Fase 25), alertando pelos canais já existentes (Slack/Teams/PagerDuty/Email/Webhook).

- [x] `nexus_core::quality::QualityCheckKind::RowCount{min, max}` — check nativo de contagem de linhas, além dos já existentes (`not_null`/`unique`/`min`/`max`/`accepted_values`). `violation_count`/`sample_size` estruturados em `quality_check_results` (não só string livre em `message`).
- [x] `crates/nexus-server/src/pipeline_run_volume_store.rs` — tabela append-only `pipeline_run_volume`, populada no hook de sucesso de cada run (orientado a evento, não a relógio).
- [x] `crates/nexus-server/src/anomaly_detector.rs` — z-score sobre janela dos últimos N runs, com cold-start (não dispara alerta antes de ter histórico mínimo).
- [x] `GET /pipelines/{id}/anomalies`, `GET /pipelines/{id}/volume-trend?limit=`. Opt-in por pipeline (`PipelineSpec.anomaly_alerts`).
- [x] 9 testes de `anomaly_detector` (séries sintéticas: estável, com outlier, cold-start) + 5 de `pipeline_run_volume_store`.

**Critério de pronto:** N runs estáveis + 1 outlier dispara alerta; `quality_check_results` com colunas estruturadas fazendo round-trip; endpoint de tendência agregando corretamente. **Atingido.**

## Fase 28 — Mascaramento de PII (tokenização determinística) ✅

Stage de pipeline que tokeniza colunas marcadas como PII, com override por pipeline. Tokenização determinística (HMAC-SHA256 com salt fixo por instalação): mesmo valor de entrada sempre produz o mesmo token — permite `GROUP BY`/join sobre o token — sem caminho de volta ao valor original (não é criptografia reversível, decisão fechada de escopo).

- [x] `crates/nexus-core/src/column_masking.rs` — `ColumnMasker` vetorizado (`arrow::array::ArrayRef`), não a API `crypto.rs::SecretCipher` existente (essa usa nonce aleatório por chamada — GCM não serve pra determinismo).
- [x] `NEXUS_MASKING_SALT` (env var, mesmo nível de aceitação de débito que `NEXUS_ENCRYPTION_KEY`) — sem essa variável configurada, salvar um pipeline com `masking` não-vazio falha explicitamente (`create_pipeline_handler`/`update_pipeline_handler`), não silencioso.
- [x] `PipelineSpec.masking: Vec<ColumnMaskingSpec>`, plugado nos 3 sub-caminhos do runner (`run_streaming_cdc_pipeline`, `run_transform_pipeline`, `run_linear_pipeline`) — mascara **antes** do Transform SQL e antes do sink.
- [x] 9 testes em `nexus-core` (determinismo, colisão trivial, mask_schema).

**Critério de pronto:** mesmo valor sempre gera o mesmo token; valores diferentes não colidem trivialmente; `GROUP BY` no Transform SQL funciona sobre a coluna tokenizada; valor original não recuperável sem o salt. **Atingido.**

## Fase 29 — Distribuição de carga entre pipelines ✅

Escopo fechado: distribuir a **execução** de pipelines diferentes entre um pool de workers — não paralelizar uma pipeline entre máquinas (isso segue fora de escopo, exigiria Ballista/DataFusion distribuído). Opt-in via `NEXUS_QUEUE_MODE=true`; sem isso, comportamento idêntico a hoje (dispatch inline na réplica que recebe a chamada).

- [x] `crates/nexus-server/src/work_queue.rs` — tabela `pipeline_run_queue`, claim via `SELECT ... FOR UPDATE SKIP LOCKED` (Postgres-only — primitivo de concorrência diferente do advisory-lock de `scheduler.rs`, que elege UM líder pra *decisão*, não N workers reivindicando jobs *diferentes*).
- [x] `crates/nexus-server/src/worker.rs` — modo réplica que só reivindica+executa jobs da fila (loop de poll + `runner::run_pipeline` existente, sem lógica interna nova), sem servir tráfego HTTP de API.
- [x] `start_pipeline_run` enfileira em vez de despachar inline quando `state.work_queue` é `Some` — fallback pra dispatch inline se o enqueue falhar (nunca perde o run silenciosamente).
- [x] SQLite recusa modo worker (sem `SKIP LOCKED` útil em single-instance, mesma restrição do leader election).
- [x] 5 testes de `work_queue`, incluindo `postgres_two_workers_never_claim_the_same_job` (2+ réplicas reais via testcontainers).

**Critério de pronto:** N runs enfileirados, cada um reivindicado por exatamente um worker, nenhum duplo-processamento; SQLite recusa/ignora modo worker corretamente. **Atingido.**

## Fase 30 — Blocos de transformação/limpeza sem código (no-code) — backend e frontend implementados, docs pendentes

Pedido de 2026-09-24: caixas de transformação/limpeza configuráveis (sem
escrever código), arrastáveis igual conector, encadeáveis entre 1 fonte e
1 destino, com paleta própria na UI, separada da paleta de conectores.

**Decisões fechadas com o usuário (2026-09-24):**
- Blocos são persistidos **server-side** como lista estruturada (opção
  (b) abaixo) — reabrir o pipeline reidrata as caixas editáveis.
- Blocos e os nodes de código (SQL transform, Python) **coexistem no
  produto como alternativas** — o usuário escolhe, por pipeline, se monta
  a limpeza só com blocos configuráveis ou escreve SQL/Python; não é
  preciso combinar os dois na mesma cadeia pro v1.
- **Agregação (`GROUP BY`) entra no v1**, não fica pra depois — caso de
  uso real do usuário: agregar/limpar antes de vetorizar (destino vetorial)
  ou de gravar num data warehouse (destino relacional). Meu levantamento
  original tinha jogado agregação pra "fora do v1" junto com join por
  engano — só join precisa de 2 inputs; agregação é 1 input, mesmo shape
  de todos os outros blocos, sem motivo real pra adiar.
- **Preview por bloco** (2026-09-24, pedido no meio da implementação):
  cada bloco, ao ser configurado, tem um botão "Visualizar" igual o resto
  do produto já tem (`DataPreviewPanel.tsx`) — mostra uma amostra de como
  os dados ficam depois daquele bloco (e de todos os anteriores na
  cadeia), sem precisar salvar/rodar o pipeline inteiro.
- **Tratamento de nulos mais rico** (2026-09-24): além de "preencher com
  valor fixo" e "remover linha com nulo", o bloco de preenchimento ganha
  mais estratégias (média da coluna, valor de outra coluna) — catálogo
  atualizado abaixo.

### Como o motor funciona hoje (achado que define o desenho)

- `PipelineSpec` tem UM slot opcional por "kind" de estágio (`transform:
  Option<TransformSpec>`, `python`, `dbt`, `embedding`), nunca uma lista.
  Ordem de execução é **fixa por kind** (`sources -> embedding -> llm ->
  transform -> python -> sinks`), não pelas arestas do canvas.
- **As arestas do React Flow no Canvas são só visuais hoje** —
  `toPipelineSpec` (`frontend/src/lib/dag.ts`) nunca lê `edges`, só filtra
  nodes por tipo/role. `atMostOneTransform`/`atMostOneDbt`/etc. são
  validados explicitamente — hoje é literalmente impossível ter 2 nodes de
  transform no mesmo pipeline.
- O node "Transform" existente já roda via DataFusion
  (`crates/nexus-core/src/transform.rs::DataFusionTransform`): registra
  cada source como tabela em memória e roda **uma** string SQL contra elas.
- O node Python é o precedente mais próximo de "sem SQL", mas ainda é
  escrever **código** (script Python livre), não configuração.
- Não existe autocomplete de coluna em lugar nenhum hoje (nem no SQL
  transform) — schema real só fica conhecido **depois** de rodar
  (`pipeline_schema_store.rs`); `GET /pipelines/{id}/preview` exige
  pipeline já salvo.

### Restrição nova (achada ao desenhar o preview): 1 source só no v1

Preview por bloco precisa rodar a cadeia ad-hoc, sem pipeline salvo — mais
simples de implementar (e de entender pro usuário) se `clean_blocks` só
aceitar **exatamente 1 source**, mesma restrição que hoje já existe pro
caminho sem transform SQL e pro node Python sozinho (`dag.rs::validate()`).
Múltiplas fontes exigiriam registrar N tabelas antes de rodar o preview e
o usuário escolher em qual delas cada bloco atua — fora de escopo do v1,
mesmo raciocínio que já tirou join do catálogo.

### Decisão de arquitetura recomendada (menor risco, reaproveita tudo)

**Blocos = builder visual que gera SQL, não um motor de execução novo.**
Cada bloco é uma operação tipada (enum com tag, mesmo padrão de
`QualityCheckKind` em `nexus-core/src/quality.rs`) que sabe se traduzir
num fragmento SQL. N blocos encadeados viram **uma** string SQL com CTEs
(`WITH step_0 AS (...), step_1 AS (...) SELECT * FROM step_N`), que
alimenta o `DataFusionTransform` já existente sem tocar em `runner.rs`,
checkpoint, lineage ou no motor de streaming CDC — zero motor de execução
novo.

Consequência prática: a cadeia de blocos é **linear** (sem ramificação/
merge dentro da cadeia) — combina com o resto do motor (que também não
tem fan-out/fan-in dentro de um estágio) e cobre "vários boxes conectados
em sequência" do pedido original. Um grafo de blocos com ramificação real
exigiria um executor de DAG genérico (reescrita grande de
`runner.rs`/checkpoint/lineage) — não recomendado pro v1.

### O que muda em cada camada

**Backend (`nexus-core`)**
- Novo `crates/nexus-core/src/clean.rs`: enum `CleanBlockKind` (tag), cada
  variante = uma operação (catálogo abaixo), `+ fn to_sql_fragment(&self,
  input_table: &str) -> String` por variante — puro, sem I/O, testável
  isoladamente (comparação de string).
- `PipelineSpec` ganha `#[serde(default)] pub clean_blocks:
  Vec<CleanBlockSpec>` (`CleanBlockSpec{name?: String, kind:
  CleanBlockKind}`) — a ordem da lista é a ordem de execução.
- `compile_clean_blocks(blocks, input_table) -> String` monta a cadeia de
  CTEs. Se `clean_blocks` não estiver vazio, o SQL compilado vira o
  `transform.sql` efetivo antes de `DataFusionTransform::new(...)` — sem
  novo estágio no `runner.rs`, só um passo de compilação antes do que já
  existe. Sem migração de schema — campo novo com `#[serde(default)]`.

**Backend (`nexus-server`)** — nada de novo em runtime; só refletir
`clean_blocks` onde `transform` já é citado (mesmos pontos que qualquer
campo novo de `PipelineSpec` toca: `lineage.rs`, `lib.rs`,
`pipeline_store.rs`, `migrate.rs`).

**Frontend**
- `frontend/src/lib/dag.ts`: novo `kind: 'clean'` node data
  (`{blockKind: <tag>, ...campos}`); `toPipelineSpec`/`fromPipelineSpec`
  juntam todos os nodes `clean` em `clean_blocks: []` **na ordem em que
  aparecem da esquerda pra direita no canvas** (posição X, não aresta) —
  mais simples que inferir ordem por grafo, e o layout atual dos outros
  nodes já ensina essa leitura.
- Novo `CleanBlockPalette.tsx` (espelho de `ConnectorPalette.tsx`): lista
  fixa (hardcoded, não vem de `GET /connectors`) dos blocos do catálogo
  abaixo, arrastável igual conector.
- **Abas/toggle entre as duas paletas** — pedido explícito ("clicando em
  conectores abre os conectores, e outra com transformações"): duas abas
  (`Conectores` / `Transformações`) no topo da paleta lateral de
  `DagCanvas.tsx`, trocando o conteúdo abaixo, mesma largura/posição.
- `dag-nodes.tsx` + `node-card.tsx`: novo `CleanBlockNodeView`, accent
  novo no `NodeCard`, ícone por tipo de bloco.
- `NodeInspector.tsx`: painel de config por `blockKind` (mesmo padrão já
  usado pro union do node embedding — `<select>` do tipo + campos
  condicionais). Colunas por texto livre, sem autocomplete no v1 (mesmo
  nível do SQL transform hoje).
- Traduções en/pt de cada bloco + labels.

**Onde compilar blocos → SQL: decidido, (b)** — mantendo o raciocínio
das duas opções por completude:
- (a) *Client-side* (`dag.ts` já manda o SQL final como `transform.sql`,
  sem campo novo no backend): zero mudança de API, mas reabrir um
  pipeline salvo só tem o SQL final, não os blocos — não dá pra
  "desmontar" de volta em caixas editáveis. Quebra o objetivo de UX.
- (b) *Server-side, `clean_blocks` persistido* (recomendado): frontend
  manda a lista estruturada, backend compila o SQL toda vez que
  roda/valida. Reabrir o pipeline reidrata as caixas exatamente como
  estavam — único jeito de cumprir "configurar, não escrever código" de
  forma persistente.

### Preview por bloco — reaproveita o `/connectors/preview` ad-hoc que já existe

`preview_adhoc_handler` (`POST /connectors/preview`, `crates/nexus-server/
src/lib.rs`) já prova que dá pra prever dado **sem pipeline salvo**: monta
um `PipelineSpec` descartável só com o source, chama `build_source` +
`read_preview_rows` (lê N batches reais do conector, sem tocar sink). Pra
preview de bloco, o mesmo caminho + um passo: compilar `clean_blocks[0..=i]`
pra SQL (`compile_clean_blocks`) e rodar via `DataFusionTransform` sobre os
batches lidos, antes de virar JSON.

- Refatorar `read_preview_rows` em duas partes: `read_preview_batches(source,
  limit) -> Vec<RecordBatch>` (lê+corta, sem serializar) e a conversão pra
  JSON por cima — `read_preview_rows` atual vira `read_preview_batches(...)`
  + essa conversão; o handler novo reaproveita só a parte de batches.
- Novo `POST /pipelines/preview-clean-blocks`, body `{source: NodeSpec,
  blocks: Vec<CleanBlockSpec>, limit?: usize}` (mesmo tier de trust do
  `/connectors/preview` — `Execute`, não precisa de pipeline salvo):
  `build_source` no `source`, `read_preview_batches`, `compile_clean_blocks`
  pra SQL, `DataFusionTransform::new(sql).apply([(nome_tabela, schema,
  batches)])`, resultado vira JSON do mesmo jeito que o preview de hoje.
- Frontend: botão "Visualizar" no painel de config de cada node `clean`
  (`NodeInspector.tsx`), reaproveitando `DataPreviewPanel.tsx` — manda o
  source conectado + todos os blocos até (e incluindo) o que está sendo
  editado, na ordem por posição X do canvas.

### Catálogo de blocos v1 (~13, cobre a maioria dos casos de limpeza)

1. **Filtrar linhas** — coluna, operador (=, !=, >, <, >=, <=, contém,
   começa com, é nulo, não é nulo), valor
2. **Selecionar/remover colunas** — lista de colunas a manter ou remover
3. **Renomear coluna** — de/para
4. **Converter tipo** — coluna, tipo alvo (int, float, texto, data,
   booleano)
5. **Remover espaços (trim)** — coluna(s)
6. **Buscar e substituir texto** — coluna, buscar, substituir (texto
   literal; regex fica pra v2)
7. **Preencher nulos** — coluna + estratégia: valor fixo, média da
   coluna (`COALESCE(col, (SELECT AVG(col) FROM <cte_anterior>))`, só
   numérico) ou valor de outra coluna (`COALESCE(col_a, col_b)`). Mediana/
   moda ficam fora do v1 — mediana depende de suporte a percentil no
   DataFusion (não confirmado), moda não tem expressão SQL simples de 1
   linha; registrar como possível v2.
8. **Remover linhas com nulo** — coluna(s)
9. **Remover duplicadas** — todas as colunas ou lista específica
10. **Maiúsculas/minúsculas/capitalizar** — coluna, modo
11. **Coluna calculada simples** — nome da nova coluna, expressão entre
    2 colunas (`col_a + col_b`, concatenar) — builder bem simples, não um
    editor de fórmula genérico
12. **Ordenar linhas** — coluna, asc/desc
13. **Agregar (`GROUP BY`)** — `group_by: [coluna, ...]` +
    `aggregations: [{coluna, função: soma/média/contagem/contagem
    distinta/mín/máx, nome_saída}]`. Único bloco que muda a cardinalidade
    das linhas (N:1) — os blocos posteriores da cadeia operam sobre o
    resultado já agregado, não sobre as linhas originais. Compila pra
    `SELECT <group_by>, <fn>(<coluna>) AS <nome_saída> ... GROUP BY
    <group_by>` na CTE da vez.

Fora do v1 (split de coluna por delimitador, regex, pivot, join entre
fontes) — cada um muda a forma do schema de outro jeito (join precisa de
2 inputs) ou é composto (pivot = agregação + reshape); fica pra fase
seguinte depois de validar o v1 com os básicos.

### Uso pra vetorizar/data warehouse — restrição conhecida da ordem do motor

Caso de uso do usuário: agregar/limpar dados e então (a) vetorizar num
banco vetorial, ou (b) gravar num data warehouse relacional.

- **(b) funciona direto no v1**: `source -> blocos (agregação/limpeza) ->
  sink relacional` não usa `embedding` nenhum — a ordem fixa do motor
  (`sources -> embedding -> llm -> transform -> python -> sinks`,
  `runner.rs`) não entra no caminho.
- **(a) esbarra numa restrição que já existe hoje, independente dos
  blocos**: o motor sempre roda `embedding` **antes** do estágio de
  transform (SQL ou blocos) — não dá pra "agregar primeiro, embeddar
  depois" dentro de **um único** pipeline, nem pra quem já escreve SQL
  hoje. Workaround já suportado pelo produto (Fase 26, orquestração):
  **dois pipelines encadeados via `depends_on`** — pipeline A agrega/limpa
  e grava numa tabela de staging; pipeline B lê essa tabela, embedda e
  grava no banco vetorial. Não é bonito, mas funciona sem mudar a ordem
  fixa do motor. Reordenar isso de verdade (permitir `transform` antes de
  `embedding` na mesma run) é uma mudança de arquitetura separada, maior
  — registrar como possível Fase 31 se virar prioridade real, não incluído
  no escopo desta fase.

### Checklist

- [x] ~~Decisão: compilar client-side vs. server-side~~ — **server-side**,
      confirmado com o usuário 2026-09-24.
- [x] ~~Decisão: `clean_blocks` e o node SQL avançado são exclusivos ou
      combináveis~~ — **alternativas**, usuário escolhe por pipeline,
      confirmado 2026-09-24.
- [x] `nexus-core::clean.rs` — enum + compilador pra SQL + testes
      unitários (25 testes, executando SQL real via `DataFusionTransform`,
      não só comparando string). Achados no caminho: `SELECT * REPLACE
      (...)`, `SELECT * EXCEPT (...)`, `DISTINCT ON` e `ROW_NUMBER() OVER`
      são todos suportados por este DataFusion 54.1 (confirmado com testes
      reais, não assumido) — "remover colunas" voltou pro catálogo v1 via
      `EXCEPT`. `ORDER BY` dentro de uma CTE não sobrevive à query externa
      (confirmado por um teste que falhou) — hoisted pra query final.
      Bug real pego por um teste de round-trip JSON: `FillNulls.column` e
      `NullFillStrategy::OtherColumn.column` colidiam no mesmo nível
      achatado — renomeado pra `fallback_column`.
- [x] `PipelineSpec.clean_blocks` + validação (`dag.rs`) — exatamente 1
      source, mesma regra do caminho sem transform SQL; exclusividade com
      `transform`/`python`. Fonte `-cdc` **passou a ser suportada**
      (Fase 31, 2026-09-25, ver checklist da Fase 31): a rejeição total
      original virou uma checagem específica só pro que realmente
      derruba `__opcode` — bloco `aggregate` (muda cardinalidade,
      incompatível com semântica por-evento do CDC) e `select_columns`
      que exclui/derruba a coluna. 12 testes em `dag.rs` (8 originais +
      4 da Fase 31: aceita CDC quando preserva opcode, rejeita
      `aggregate` com CDC, rejeita `select_columns` derrubando opcode
      nos dois modos, aceita `select_columns` mantendo opcode
      explicitamente).
- [x] Refatorar `read_preview_rows` em `read_preview_batches` + conversão
      JSON separada (sem mudar comportamento do endpoint existente)
- [x] `POST /pipelines/preview-clean-blocks` (preview ad-hoc por bloco) —
      mesmo padrão de `/connectors/preview` (`Execute`, sem pipeline
      salvo), com o mesmo scan de SSRF (`validate_security_with`) que o
      ad-hoc de conector já tinha — achado ao implementar: minha primeira
      versão esqueceu esse scan, corrigido antes de mergear.
- [x] Ajustar os pontos que hoje citam `transform`/`python: None` nos
      arquivos de store/lineage/migração — 5 pontos no total (achado um a
      mais que os 4 originalmente previstos, em `data_catalog.rs`, dentro
      de um bloco `#[cfg(test)]` que só o clippy `--all-targets` compila).
- [x] Teste de integração real de ponta a ponta (`runner.rs`): CSV em
      disco → filtro + rename + sort → CSV de saída, via `run_pipeline`
      de verdade, não só o compilador isolado.
- [x] `CleanBlockPalette.tsx` + abas Conectores/Transformações em
      `DagCanvas.tsx` — abas via `useState<'connectors'|'transformations'>`
      no próprio `DagCanvas`; `ConnectorPalette`/`CleanBlockPalette` viraram
      `flex-1` sem largura/borda própria (o wrapper novo passou a dono
      dessas classes).
- [x] `CleanBlockNodeView` + accent novo em `node-card.tsx` — accent `teal`,
      ícone `Wand2`, subtítulo por `blockKind` (`cleanBlockSubtitle`, 13
      casos).
- [x] Config por bloco em `NodeInspector.tsx` — achado real: o bloco
      `embedding` (sem guarda de `data.kind`) contava com ser o último
      ramo depois de 4 `if` sequenciais pra TS estreitar o tipo; o 6º
      membro da union (`clean`) quebrou essa exaustividade implícita (17
      erros `TS2322`, campos virando `unknown`). Corrigido com um `if
      (data.kind === 'clean')` explícito antes do bloco de embedding.
      Botão "Visualizar" reaproveita o padrão de `NodePreview.tsx` via
      `CleanBlockPreview.tsx` novo.
- [x] `dag.ts`: serialização por posição X, ida e volta
      (`toPipelineSpec`/`fromPipelineSpec`) — `toCleanBlockSpec`/
      `fromCleanBlockSpec`, 13 casos cada; bug real pego por teste de
      round-trip JSON no backend (`fallback_column`) replicado aqui no
      tipo TS pra não reabrir a mesma colisão.
- [x] Traduções en/pt — achado real: `canvas.clean.operator` (e mais 5:
      `dataType`, `nullStrategy`, `caseMode`, `computeOperator`,
      `direction`) precisavam ser ao mesmo tempo label de campo (string) e
      mapa de opções do enum (objeto) — colisão estrutural real, não dá
      pra um objeto JS ter a mesma chave como string e objeto. Corrigido
      sufixando os 6 labels com `Label` (`operatorLabel` etc.), espelhado
      em `NodeInspector.tsx`.
- [x] Testes: 27 testes em `dag.test.ts` (10 `toPipelineSpec` + 2
      `fromPipelineSpec` de clean blocks, cobrindo ordenação por X,
      exclusividade com transform/python, >1 source, fan-out de sinks,
      validação de coluna/valor obrigatório, mini-DSL de agregação e a
      distinção `column`/`fallback_column`) — suite completa (56 arquivos)
      sem regressão; `tsc -b` e `oxlint` limpos (só warnings pré-existentes
      não relacionados).
- [x] Docs: `USER_GUIDE.md` §12 (seção nova, exemplo prático + preview +
      restrição de vetorizar num único pipeline), `ARCHITECTURE.md` §19
      (decisão "compila pra SQL" + achados de DataFusion + bug do
      `fallback_column`)

### Riscos

- **Nome de coluna sem autocomplete** — mesmo nível de fricção do SQL
  transform hoje; melhoria futura via `pipeline_schema_store` (schema da
  última run) alimentando um dropdown, só funciona após a 1ª execução.
- **Escapar identificador SQL** (nome de coluna com espaço/aspas) — usar
  aspas duplas do DataFusion consistentemente; testar com nome "sujo".
- **Cadeia linear, sem ramificação** — pedido futuro de "dividir em 2
  caminhos" fica fora de escopo do v1; documentar a limitação na UI.

**Estimativa (chute):** backend (enum+compilador+validação+testes,
agregação incluída) ~2–2,5d; frontend (paleta+node+inspector+
serialização+i18n, config de agregação é o bloco com mais campos)
~2,5–3,5d; docs+testes de integração ~1d. Total ~6–7 dias.

**Critério de pronto:** pipeline `csv -> [filtrar] -> [renomear] ->
[remover duplicadas] -> csv` roda de ponta a ponta pelo Canvas sem o
usuário escrever SQL/código nenhum; salvar e reabrir mantém as 3 caixas
editáveis; SQL gerado testado unitariamente pros 12 blocos.

---

## Fase 31 — Agente de IA com tool-calling (estilo n8n AI Agent) — planejado, não implementado

Pedido de 2026-09-25: "criar um agente usando os dados processados do
NexusFlow e fazer esse agente fazer diversas coisas tipo como é no n8n
hoje". Inspiração explícita: o node **AI Agent** do n8n — um LLM que,
dado um objetivo, decide sozinho **qual ferramenta chamar, em que
ordem**, olha o resultado e decide o próximo passo, até dar uma resposta
final ou esgotar um limite de passos. Diferente de tudo que o NexusFlow
já tem: o node `llm` (Fase 26/`ARCHITECTURE.md §17`) é 1 chamada por
linha sem escolha nenhuma, e o RAG (`POST /rag/query`) é um fluxo fixo
(busca → responde), sem loop e sem ferramenta nenhuma além da busca
vetorial embutida.

**Decisões fechadas com o usuário (2026-09-25):**
- Ferramentas reais, com efeito no mundo (não só "responder texto") —
  automação de verdade, não um RAG mais chique.
- Precisa **superar** o n8n hoje, não só igualar — ver seção própria
  abaixo com os diferenciais escolhidos.
- **As duas formas de execução de ferramenta convivem**: cada ferramenta
  tem um modo configurável, `auto` (o agente executa sozinho) ou
  `precisa_aprovação` (humano aprova/rejeita antes de rodar) — não é
  uma escolha única por agente, é por ferramenta, dentro do mesmo
  agente.

### Por que isso não é o node `llm` nem o RAG — achado que define o desenho

`crates/nexus-ai/src/llm/client.rs`/`anthropic_client.rs` hoje fazem
**só completion de disparo único**: `ChatCompletionRequest` não tem
campo `tools`, `ChatMessage` é uma mensagem só (sem histórico
multi-turno), a resposta não tem `tool_calls`/`tool_use` nenhum. Não dá
pra "encaixar" um loop de agente em cima disso sem adicionar de
verdade: (1) parâmetro `tools` no request (JSON Schema por ferramenta,
formato que OpenAI-compatible e Anthropic exigem, cada um com sintaxe
própria), (2) histórico de mensagens multi-turno (`user` → `assistant`
com `tool_calls` → `tool` com o resultado → repete), (3) parsing da
resposta pra extrair qual ferramenta foi pedida e com que argumentos.
Essa é a única peça de integração LLM genuinamente nova da fase — tudo
mais reaproveita infraestrutura que já existe.

`PipelineSpec` também não serve de molde pro agente: um agente não é
fonte→transform→destino, é **prompt + conjunto de ferramentas + modelo
+ política de aprovação por ferramenta**. Um `AgentSpec` novo, análogo
em espírito a `PipelineSpec` mas com forma própria, é mais simples que
forçar isso dentro de `PipelineSpec` (que já tem `llm: Option<...>`
pensado pra 1-chamada-por-linha, não pra loop).

### Como ser melhor que o n8n hoje (diferenciais reais, não só paridade)

Escolhidos por serem **infraestrutura que o NexusFlow já tem e o n8n
não** (ou só tem via plugin/gambiarra) — diferencial de verdade, não
lista de desejos:

1. **Aprovação humana por ferramenta é configuração nativa, não um node
   de "esperar" caseiro.** No n8n, human-in-the-loop se monta na mão
   com um node de espera + webhook externo. Aqui é um campo
   (`approval: auto | require_approval`) por ferramenta dentro do
   `AgentSpec` — o motor já sabe pausar, persistir o passo pendente e
   retomar exatamente dali quando aprovado (mesmo espírito de resumir
   do cursor exato que o CDC já faz, `ARCHITECTURE.md §5`).
2. **Custo/tokens por execução, nativo desde o dia 1** — reaproveita
   exatamente o que `pipeline_run_llm_stats_store.rs` já agrega pro
   node `llm`; no n8n isso não existe pronto, cada um constrói sozinho
   com um node de código.
3. **Avaliação contínua de qualidade** — o mesmo golden dataset/scoring
   (`LlmEvalCase`, `EvalScoringMode::TokenSimilarity`/`LlmJudge`) que já
   audita o node `llm` (Fase 26 Marco L7) passa a rodar também contra
   respostas do agente: "ele está respondendo bem?" ao longo do tempo,
   não só "rodou sem erro".
4. **Versionamento de prompt com histórico real** — `PromptTemplateStore`
   (nunca sobrescreve, cada save é versão nova) + o versionamento git
   embutido (`git_history_store.rs`, `ARCHITECTURE.md §18`) já
   existentes cobrem o prompt de sistema do agente de graça — dá pra
   comparar/reverter uma mudança de prompt contra o histórico de eval
   acima. No n8n o prompt é uma string solta dentro do node, sem
   histórico nenhum a não ser que o usuário monte fora da ferramenta.
5. **Auditoria amarrada ao RBAC que já existe** — quem aprovou/rejeitou
   um passo vai pro `audit_log` (mesma tabela que já registra
   login/CRUD de pipeline), gateado por papel (`Execute` só vê e roda;
   aprovar ação de ferramenta exige `Write` — a decidir no detalhe,
   ver checklist). RBAC do n8n (community) é bem mais raso que os 4
   papéis que o NexusFlow já tem.

### Design por camada

**`nexus-ai` (LLM/tool-calling — a peça nova de verdade)**
- Novo `crates/nexus-ai/src/llm/agent.rs`: `AgentTool { name, description,
  parameters_json_schema }` (uma ferramenta descrita do jeito que a API
  do modelo espera) e `AgentLoopStep`/`AgentLoopOutcome` (`FinalAnswer`
  | `ToolCallRequested{tool, args}`) — o loop em si (chamar modelo →
  decidir → chamar ferramenta → alimentar resultado de volta) fica
  **fora** de `nexus-ai` (que não tem I/O de conector nenhum, CLAUDE.md
  §8.3) — só a mecânica de "1 turno" mora aqui.
- `client.rs`/`anthropic_client.rs`: estender `ChatCompletionRequest`
  com `tools: Option<Vec<ToolSchema>>` e parsear `tool_calls` da
  resposta (formato OpenAI-compatible); Anthropic usa `tools` +
  `tool_use`/`tool_result` no formato próprio dele (já documentado como
  "não é formato OpenAI" no comment existente do arquivo). Histórico de
  mensagens vira `Vec<ChatMessage>` em vez de mensagem única.

**`nexus-server` — o loop de verdade + persistência + API**
- Novo `AgentSpec` (`nexus-core`, mesmo lugar de `PipelineSpec`):
  `{name, prompt: PromptRef, model: LlmModelConfig, tools:
  Vec<AgentToolConfig>, max_steps: u32}`, onde `AgentToolConfig
  {tool: AgentToolKind, approval: ApprovalMode}` e `ApprovalMode = Auto
  | RequireApproval`. Reaproveita `LlmModelConfig` (`Api`/`Anthropic`)
  e `PromptRef{name, version}` que o node `llm` já usa — zero tipo
  novo pra essas duas partes.
- Catálogo de ferramentas v1 (`AgentToolKind`), cada uma um wrapper
  fino sobre código que **já existe**, sem motor de execução novo:
  1. `QueryData{source: NodeSpec, sql: Option<String>}` — roda SQL via
     `DataFusionTransform` sobre um source, ou lê preview de um
     pipeline salvo (`read_preview_batches`, já usado pelo preview de
     conector e pelo de clean blocks da Fase 30).
  2. `SearchVectors{connector, config}` — reaproveita `search.rs` dos 6
     conectores vetoriais (mesmo motor do RAG, Fase 26).
  3. `RunPipeline{pipeline_id, wait_for_result: bool}` — dispara um
     pipeline salvo via o mesmo caminho de `POST /pipelines/{id}/run`
     já existente.
  4. `CallWebhook{url, method, body_template}` — request HTTP genérico,
     mesma validação de SSRF (`validate_security_with`/`dns_guard.rs`)
     que todo conector `rest`/`webhook`/alerta já passa.
- Novo `crates/nexus-server/src/agent_runner.rs`: o loop real —
  monta o histórico de mensagens, chama o modelo, se vier
  `ToolCallRequested` verifica `ApprovalMode` da ferramenta: `Auto`
  executa e alimenta o resultado de volta no loop; `RequireApproval`
  **pausa** (grava o passo como `pending_approval`, não chama nada
  ainda) e dispara notificação (`AlertNotifier::notify_agent_approval_needed`,
  mesmo idioma de `notify_pipeline_run`/`notify_anomaly` — 5 canais já
  prontos). Loop pára de vez em `max_steps` (guard-rail contra loop
  infinito, dor real conhecida do n8n) ou em `FinalAnswer`.
- Novas tabelas (dual-dialeto Sqlite/Postgres, mesmo padrão de
  `pipeline_run_llm_stats_store.rs`): `agent_runs(id, agent_id,
  status, started_at, finished_at, total_tokens, total_cost)` e
  `agent_steps(id, run_id, step_number, kind, tool, args, result,
  approval_status, approved_by, approved_at)` — **1 tabela de steps
  serve tanto de trace passo-a-passo (abrir uma execução) quanto de
  fonte pra métrica agregada** (taxa de sucesso, custo, latência ao
  longo do tempo), sem duplicar dado em dois lugares.
- Endpoints novos: `POST /agents` (CRUD, papel `Write`), `POST
  /agents/{id}/run` (dispara, papel `Execute` — mesmo tier do resto),
  `GET /agents/{id}/runs` / `GET /agents/{id}/runs/{run_id}` (trace +
  métricas), `POST /agents/runs/{run_id}/steps/{step_id}/approve` /
  `/reject` (papel a decidir — `Write` é o candidato natural, mesmo
  nível de quem edita o Canvas).
- Disparo: reaproveita os 2 mecanismos que `PipelineSpec` já tem, sem
  inventar um terceiro — `AgentSpec.schedule: Option<String>` (mesmo
  cron do scheduler existente) pra rodar sozinho, e o endpoint
  `POST /agents/{id}/run` pra sob-demanda/chat. Encadeamento reativo
  (agente dispara ao fim de um pipeline) fica de fora do v1 — poderia
  reusar `depends_on`/`pipeline_dependencies.rs` (Fase 26) no futuro,
  mas não é pedido agora.

**Frontend**
- Nova aba/área "Agentes" (paralela a "Pipelines", não faz parte do
  Canvas de DAG — ferramenta não tem ordem fixa, quem decide a ordem é
  o modelo em runtime, não o usuário arrastando nodes).
- Painel de config do agente: prompt (reaproveita o seletor de
  `PromptRef` que o node `llm` já tem), modelo (mesmo `<select>`
  Api/Anthropic), lista de ferramentas ligadas com o
  toggle `auto`/`precisa aprovação` por ferramenta.
- Painel de execução: lista de `agent_runs` (status, custo, duração) →
  clicar abre o trace de `agent_steps` daquela execução, passo a passo,
  com um passo `pending_approval` mostrando botões Aprovar/Rejeitar
  inline (sem precisar sair do painel pra aprovar).
- Notificação de aprovação pendente: reaproveita os 5 canais de alerta
  já configurados (Slack/Teams/PagerDuty/Email/webhook) — mensagem com
  link direto pro passo pendente.

### Riscos

- **Custo de loop longo** — um agente com `max_steps` alto e ferramentas
  caras (busca vetorial + LLM a cada passo) pode gastar muito antes de
  parar; `max_steps` é obrigatório no `AgentSpec`, sem default "sem
  limite".
- **Formato de tool-calling diverge entre OpenAI-compatible e
  Anthropic** — schemas JSON diferentes, parsing de resposta diferente;
  self-hosted (vLLM/Ollama) pode não suportar `tools` de verdade mesmo
  falando "OpenAI-compatible" — precisa de teste real contra pelo menos
  1 servidor local antes de dar como pronto.
- **Ferramenta com efeito real (`RunPipeline`/`CallWebhook`) chamada 2x
  se o processo cair entre "executou" e "gravou o resultado"** — sem
  idempotência nova pro v1 (fora de escopo), documentar como debate
  conhecido, mesmo nível de risco que qualquer chamada de rede sem
  retry idempotente hoje.
- **Prompt injection via dado processado** — o agente lê dado real
  (via `QueryData`/`SearchVectors`) que pode conter texto malicioso
  tentando manipular o próximo passo do loop; ferramentas com efeito
  real (`RunPipeline`/`CallWebhook`) começarem como `RequireApproval`
  por padrão é a mitigação do v1, não filtro de conteúdo.

### Checklist

- [ ] `nexus-ai`: `tools` no request + parsing de `tool_calls`/`tool_use`
      pros 2 backends, histórico multi-turno — testado contra mock HTTP
      (wiremock) simulando 1 tool-call + 1 resposta final.
- [ ] `AgentSpec` + `AgentToolKind`/`ApprovalMode` (`nexus-core`)
- [ ] `agent_runner.rs`: loop completo, `max_steps`, pausa/retomada em
      `RequireApproval` — teste de integração com ferramenta mock
      (sem chamar LLM real) validando pausa exata e retomada do ponto
      certo.
- [ ] 4 ferramentas v1 (`QueryData`, `SearchVectors`, `RunPipeline`,
      `CallWebhook`) — cada uma testada isoladamente contra o que já
      reaproveita (CSV real, vetor real, pipeline real, mock HTTP).
- [ ] `agent_runs`/`agent_steps` stores (dual-dialeto) + endpoints CRUD/
      run/approve/reject.
- [ ] `AlertNotifier::notify_agent_approval_needed` (5 canais).
- [ ] Reaproveitar eval (`LlmEvalCase`) e custo/tokens
      (`pipeline_run_llm_stats_store`-like) pro agente.
- [ ] Frontend: aba Agentes, config de ferramentas com toggle de
      aprovação, painel de execução com trace + aprovar/rejeitar
      inline.
- [ ] Docs: `USER_GUIDE.md` (seção nova), `ARCHITECTURE.md` (loop de
      tool-calling, decisão de schema das 2 tabelas novas).

**Estimativa (chute):** tool-calling em `nexus-ai` (2 backends) ~1,5d;
`AgentSpec`+loop+persistência+aprovação ~3d; 4 ferramentas ~2d;
frontend (aba nova + painel de execução) ~2,5d; docs+testes de
integração ~1d. Total ~10 dias — bem maior que a Fase 30 porque o
tool-calling em si é peça de infraestrutura genuinamente nova, não só
composição do que já existe.

**Critério de pronto:** agente configurado com `QueryData` (auto) +
`RunPipeline` (precisa de aprovação) responde uma pergunta real
consultando dado processado, decide sozinho chamar `QueryData`, e ao
tentar `RunPipeline` pausa esperando aprovação — aprovar via API resume
o loop exatamente dali e o agente conclui; todo o trace (2+ passos)
visível em `GET /agents/{id}/runs/{run_id}`.

---

---

**Critério de "MVP pronto"**: Fases 0–3 + 7 (parcial: auth básica) + 8 (canvas mínimo) funcionando end-to-end — mover dados de Postgres pra Postgres via canvas visual, com checkpoint por partição, retry e escrita idempotente. **Atingido e superado** — Fases 0–11 e 13–29 completas, só falta Fase 12 (enterprise, repo separado) e os itens condicionais/parciais marcados acima.

## Débitos conhecidos (aceitos pro MVP, resolver antes de vender enterprise)

- **Secrets via env var, sem KMS/rotação** — ok pra self-host single-tenant; precisa migrar pra KMS (AWS/GCP/Vault) antes do primeiro cliente enterprise (`ARCHITECTURE.md §10`).
- **RBAC sem escopo por recurso** — 4 papéis globais chega pro MVP; SaaS multi-tenant vai exigir permissão por pipeline/credencial.
- ~~**Ciclo de vida do modelo ONNX indefinido**~~ — decidido 2026-07-30: HF Hub em runtime + cache local (`ARCHITECTURE.md §8`).
- **Execução single-node** — decisão deliberada de escopo, não limitação a esconder do usuário (`ARCHITECTURE.md §6`). Documentar isso claramente também no README quando o produto for anunciado publicamente.
- **5 advisories RustSec aceitos (ver `.github/workflows/ci.yml`'s `cargo-audit` job)**: `RUSTSEC-2023-0071` (rsa, via `jsonwebtoken`'s RS256 — sem correção disponível upstream), `RUSTSEC-2026-0194`/`-0195` (quick-xml, via `object_store`/`datafusion` 54.1.0 — mesmo pin de arrow 58.x abaixo), `RUSTSEC-2025-0009`/`RUSTSEC-2024-0336` (ring/rustls, via `milvus-sdk-rust`'s tonic 0.8.3 — sem release mais nova do SDK). Reavaliar cada um quando a dependência que os carrega soltar uma versão nova.
- **`GHSA-2f9f-gq7v-9h6m` (Apache Thrift, "Memory Allocation with Excessive Size Value", corrigido em `thrift` 0.23.0) — aceito, sem `--ignore` no `cargo-audit` porque não existe `RUSTSEC-ID` pra isso (só aparece como Dependabot alert no GitHub, dispensado como `tolerable_risk` em 2026-09-02).** Rastreado via `cargo tree -i thrift@0.17.0 --all-features`: o `parquet` que o workspace usa (58.4.0) **não depende mais do crate `thrift`** — a rota vulnerável é 100% via `nexus-connector-ailake`, que puxa os crates externos `ailake-catalog`/`ailake-parquet` (pacote separado do mesmo projeto `ailake-io/ai-lakehouse`, fixado em `0.1.12`), que ainda usam `parquet 52.2.0` com o `thrift` velho internamente — sem release mais nova desses crates pra atualizar. Fix real precisa sair de lá, não deste repo. Exploração exigiria um arquivo Parquet malicioso alcançável por um source/sink `ailake` — já atrás do mesmo tier de confiança (`Write`) que outros conectores locais documentados em `ARCHITECTURE.md §10`.
- **`arrow-array`/`arrow-schema` fixados em `58.4.0` e `adbc_core`/`adbc_driver_manager`/`adbc_ffi` em `0.23.0` (não a última, `0.24.0`) em todo o workspace** — **atualização 2026-09-02**: verificado via `cargo update` (só resolução de dependências, não compilado/testado) que `datafusion` 55.0.0 (mais nova que a 54.1.0 pinada hoje) já resolve limpo contra `arrow-array`/`arrow-schema`/`parquet` 59.x — a metade do bloqueio original ("datafusion ainda não suporta arrow 59+") não é mais verdade. Não confirmado ainda se `adbc_core`/`adbc_driver_manager`/`adbc_ffi` 0.24.0 também resolve nesse mesmo grafo (não testado). Migração real (editar os ~30 `Cargo.toml` que fixam a versão, `cargo check`/`test`/`clippy` em cada conector) ainda não feita — só a checagem de resolução.
- **CDC nativo (`*-cdc`) combinado com um node de transform SQL ainda passa por `PipelineEngine::drain_sources`** — materializa tudo em memória antes de aplicar o SQL via DataFusion, o que nunca termina pra um source CDC em volume realista (WAL/binlog/change-stream não têm fim natural). O resume automático da Fase 18 e o streaming per-micro-batch só cobrem o caminho "passthrough" (sem transform, exatamente 1 source CDC + 1 sink). Não é regressão — nunca funcionou —, mas achado ao verificar o mecanismo de resume, antes não documentado. Ver `ARCHITECTURE.md §7`.

---

## Fase 26 — LLMOps: node `llm`, RAG e avaliação sistemática

Mergeado em `develop` em 2026-09-07 (`518cfa3`, PR #79). Plano marco a marco em `docs/LLMOPS_IMPLEMENTATION_PLAN.md`, ideação original em `docs/MLOPS_LLMOPS_PLAN.md`, resumo arquitetural em `ARCHITECTURE.md §17-18`.

- [x] **Marco L1 — Node `llm` + tracing básico**: 1 chamada por linha, backend `Api` (qualquer endpoint OpenAI-compatible) ou `Anthropic` (Messages API nativa, adicionado como emenda ao L1). Log estruturado por chamada, nunca prompt/resposta cru por padrão.
- [x] **Marco L2 — Custo/tokens agregado**: tokens/custo por run, exposto em `GET /pipelines/{id}/runs`.
- [x] **Marco L3 — Cache de resposta**: Redis, chave por hash(modelo+prompt+params), TTL configurável.
- [x] **Marco L4 — Versionamento de prompt**: `PromptTemplateStore` (`POST`/`GET /prompts`) — cada save é uma versão nova, nunca sobrescreve.
- [x] **Marco L5 — Linhagem row→geração**: `POST /rag/query` (RAG ad-hoc, fora do engine de batch) + `GET /lineage/generation/{id}`. Primeira capacidade de busca vetorial do repo — todo conector vetorial (LanceDB/Qdrant/Milvus/pgvector/Pinecone/ChromaDB) só tinha sink antes disso.
- [x] **Marco L6 — RAG reativo via CDC**: `embedding` destravado no caminho passthrough — combinação `*-cdc` source + `embedding` sem node `transform` (antes só funcionava sem CDC).
- [x] **Marco L7 — Avaliação sistemática**: golden dataset (`LlmNodeSpec.eval`) re-rodado a cada run, score por similaridade de token, persistido em `llm_eval_results`, visível no `QualityPanel.tsx`.
- [x] **Marco L8 — Empacotamento enterprise**: `GET /lineage/generation/{id}` e a combinação CDC+embedding (L6) viram pagos, reaproveitando o mecanismo de license já existente (`ROADMAP.md` Fase 12 Bloco 5) — `POST /rag/query` em si continua OSS pra qualquer vetor store. Mesmo commit `518cfa3` também implementou o versionamento git embutido + mirror pro GitHub (`git-history-github-sync`, `ARCHITECTURE.md §18`), um terceiro slug de capability paga, na época sem doc dedicada.
- [x] **L7 — follow-up** (fora do plano original): eval passa a rodar em todo caminho de execução (`run_linear_pipeline`/passthrough e `run_streaming_cdc_pipeline`, não só `run_transform_pipeline`); scoring "LLM como juiz" (`EvalScoringMode::LlmJudge`) como alternativa ao token-similarity, com fallback automático se a nota não parsear.
- [x] **RAG multi-vetor** (fora do plano original): `POST /rag/query` deixa de ser só LanceDB — Qdrant, Milvus, pgvector, Pinecone e ChromaDB ganharam capacidade de busca própria (`search.rs` em cada crate de conector). Testado com container real (embedding real + banco real) pra Qdrant/Milvus/pgvector/ChromaDB; Pinecone via mock HTTP (único sem self-host).
- [x] **Venda das 3 capabilities na Store + 2 bugs reais corrigidos** (2026-09-08/09, fora do plano original — `feature/llmops-store`, mergeada em `develop`): `llm-lineage-tracking`/`reactive-rag-cdc`/`git-history-github-sync` viraram produtos compráveis na Store, reusando o checkout Stripe já validado pro Excel (`docs/ENTERPRISE_LICENSING.md`). No processo, achado e corrigido: (1) `git-history-github-sync` era vendável mas fisicamente impossível de compilar em qualquer binário já publicado (`bin/Cargo.toml` do repo enterprise nunca expunha `llm`/`version-history`, e `version-history` nem entrava no `connectors-all` deste repo); (2) o default de `NEXUS_GIT_HISTORY_PATH` crashava no boot da imagem publicada (caminho não gravável pelo usuário não-root — corrigido com `std::env::temp_dir()`, depois de uma primeira tentativa com `$HOME` também falhar). Ambos nunca detectados antes porque ninguém tinha ligado `version-history` num binário publicado desde que foi implementado. Detalhe completo em `ARCHITECTURE.md §18`.

**Achados reais durante a verificação** (não só desenvolvimento): notificações de tarefa em background se mostraram não confiáveis nesta sessão — "completed"/exit 0 reportado pra processos que na real tinham morrido sem rodar nada, escondendo por um tempo 2 bugs reais do Marco L8 (`schemars` só em `[dev-dependencies]` quando `capability_registry.rs` precisa dele sempre; `ConnectorCapability::Capability` sem qualificar `nexus_core::`) e um bug do L7 original (teste de scoring com duas frases cuja similaridade batia exatamente no threshold de pass/fail). Todos corrigidos depois de rodar tudo em foreground com timeout explícito.

**Critério de pronto:** todos os 8 marcos + 3 rodadas extra implementados e testados (unitário + integração real via testcontainers onde fazia sentido — Redis, Postgres/pgvector, Qdrant, Milvus, ChromaDB; mock HTTP só pra Anthropic/OpenAI-compatible e Pinecone; checkout Stripe real em modo teste pra venda das capabilities). **Atingido e mergeado em `develop`** — LLMOps core em `518cfa3` (PR #79, 2026-09-07), venda + fixes em `feature/llmops-store` (2026-09-09).
