# 🗺️ Roadmap — NexusFlow

Ordem por dependência técnica, não por prioridade de negócio isolada. Cada fase assume a anterior estável.

## ⚠️ Pendências ativas (não esquecer)

Consolidado dos itens que ficaram faltando/incompletos ao longo das fases abaixo — checar aqui antes de assumir que algo já está pronto.

1. **Fase 12 — Enterprise connectors**: gate técnico no `nexus-server` implementado (`POST /license`, `GET /license/status`, `LicenseStore` com verificação JWT, hooks no registry pra futuro `is_connector_licensed`). Repo privado de conectores enterprise, serviço `nexus-licensing` e integração de pagamento ainda não implementados.
2. ~~**Marco 13 do roadmap original — CDC nativo sem Kafka/Debezium**~~ — resolvido: Fase 18 (`postgres-cdc`/`mongodb-cdc`/`mysql-cdc`), sinal de adoção confirmado.
3. **`nexus-ai`: features `cuda`/`metal` registram o execution provider ONNX Runtime correto (`ort::ep::CUDA`/`ort::ep::CoreML`), mas não validadas em hardware real** (sandbox é Linux sem GPU) — só confirmado que compilam e que o EP é registrado antes do load da sessão; runtime faz fallback silencioso pra CPU se o driver/hardware não estiver presente. `api` (embeddings via HTTP externa, endpoint compatível com OpenAI) implementada e testada (mock via `wiremock`) — sem chamada real contra OpenAI/Azure/etc neste sandbox. O perfil `cuda` do Docker já tem a infra de runtime pronta (base image + `--gpus all`).
4. **Alertas: Slack, MS Teams, PagerDuty, Email e Webhook genérico — todos os 5 canais de `CLAUDE.md §6` implementados** (ver `nexus-server/src/alerts.rs`).
5. **Windows (`.msi`) removido do CI de release por ora — chegou a rodar, achou um bug real.** `build-windows` (self-hosted `windows-connectors-heavy`, mesma máquina do `connectors-heavy.yml`) rodou de verdade uma vez e achou um erro genuíno: `cargo build --features connectors-all` falha em `openssl-sys` — `nexus-connector-mysql` (CDC binlog) depende da crate `mysql_cdc`, que só suporta OpenSSL nativo (sem rustls), e essa máquina Windows não tem OpenSSL/vcpkg instalado. Fix é setup manual na máquina (`vcpkg install openssl:x64-windows-static-md` + `vcpkg integrate install` + variável `VCPKG_ROOT`), não algo que o workflow resolve sozinho. Job removido do `release.yml` até isso ser feito (definição completa preservada no histórico do git); religar depois. Mesmo depois desse fix, o `.msi` ainda não terá sido instalado/testado numa máquina Windows real por um humano, e só ship a binário do servidor — build dos drivers ADBC (Postgres/SQLite) pra `.dll` continua sem existir (MSVC+vcpkg separado do OpenSSL acima). `winget` continua não configurado. **macOS removido do matrix de release CI por ora** (não é o mesmo caso do Windows: não existe runner self-hosted de macOS pra trocar, e os runners hospedados `macos-13`/`macos-14` do GitHub bateram num bloqueio de billing da org — ver item 16 — que também cancelava os builds Linux via fail-fast do matrix). Homebrew/`.dmg`: specs em `packaging/macos/`, nunca validados em máquina real, sem build script dos drivers ADBC pra macOS (`.dylib`) — os scripts atuais (`scripts/build-adbc-*.sh`) só geram `.so`.
6. ~~**`.deb`/`.rpm`/AppImage validados manualmente mas nunca wireados em CI**~~ — resolvido: `release.yml`'s `build` job (Linux x86_64) agora chama `scripts/package-{deb,rpm,appimage}.sh` automaticamente a cada push/PR pra `main` e sobe os 3 artifacts junto com o tarball — antes só o tarball cru era produzido em CI, os 3 scripts existiam mas nunca eram invocados. arm64 fica de fora por ora (os scripts hardcodam amd64/x86_64). `.rpm` também já tinha sido validado manualmente com `rpmbuild` real antes disso (`scripts/package-rpm.sh` buildou `nexusflow-0.1.0-1.x86_64.rpm` de ponta a ponta; corrigido de brinde um `Requires:` incompleto — faltava `unixODBC`/`cyrus-sasl-lib`, equivalentes RPM do `unixodbc`/`libsasl2-2` que o `.deb` já lista, não pegos pelo scanner automático do rpmbuild porque são dlopen'd, não linkados direto no ELF).
7. **Estatísticas de hardware (CPU/RAM) implementadas** — `sysinfo` via `nexus-server::hardware_stats`, frame `{"hardware_stats": {...}}` intercalado no WebSocket de progresso a cada 2s (mesmo canal do `ProgressEvent`, discriminado pela chave). Sem GPU — `sysinfo` não expõe utilização de GPU (é vendor-specific, NVML pra NVIDIA etc.) e nada no código depende disso ainda.
8. **Imagens Docker: publicadas no GHCR *e* no Docker Hub, multi-arch (amd64+arm64)** (`docker-publish` job em `.github/workflows/release.yml`). Modelo de release virou contínuo: dispara em todo push (não mais PR) pra `main`, não mais por tag `git` — a tag da imagem vem direto da versão em `Cargo.toml` (`v0.1.0` hoje) + `latest`, mesma convenção que a `GitHub Release` do job `publish` usa. GHCR usa `GITHUB_TOKEN` (sem credencial externa); Docker Hub (`ailake/nexusflow`) precisa dos secrets `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` configurados no repo (token de acesso, não a senha da conta) — **ainda não configurados**, então o push pro Docker Hub falha até alguém com admin do repo criar esses 2 secrets. arm64 builda via QEMU emulado (`docker/setup-qemu-action`), não runner ARM nativo como o job `build` (tarballs) usa — cargo compila mais lento sob emulação, timeout do job em 180min pra acomodar; ainda não validado com um push real pra `main` (só revisão de config).
9. **Admin (gestão de usuários) tem tela no Canvas** — `UsersPanel.tsx` cobre criar/promover/excluir contra as rotas já existentes (`GET/POST /users`, `GET/DELETE /users/{username}`, `PUT /users/{username}/role`). Nav item só aparece pra role Admin (decodificado do JWT client-side, sem verificar assinatura) — enforcement real continua 100% no servidor (`auth.rs`).
10. Ver também a seção **Débitos conhecidos** no fim deste arquivo (secrets sem KMS, RBAC sem escopo por recurso, versões de dependência pinadas, advisories RustSec aceitos).
11. ~~**Estágio `embedding` do `PipelineSpec` sem UI no Canvas**~~ — resolvido: node dedicado `kind: 'embedding'` no Canvas (mesmo padrão do node `dbt`, painel próprio em `NodeInspector.tsx` já que `EmbeddingModelSpec`/`ChunkingSpec` são unions com tag que o `SchemaForm` genérico não resolve). `lib/dag.ts`'s `PipelineSpec` agora declara `embedding`; `toPipelineSpec`/`fromPipelineSpec` fazem o round-trip completo (Onnx↔Api, fixed_window↔recursive_character) sem perder config ao editar/salvar.
12. **Fase 16 (Preview + dbt ETL) é backend-only** — `GET /pipelines/{id}/preview` não tem botão/tabela no Canvas ainda (só curl/Postman); o node dbt tem handle de saída (Fase 18 adicionou, consistência visual), mas painel de config pra `dbt.output` — hoje só configurável via API/JSON direto. Ambos deliberadamente adiados até validar se o formato backend-only já resolve o suficiente.
13. ~~**Fase 18 (CDC nativo) sem toggle no Canvas**~~ — resolvido: `NodeInspector` tem switch Batch/CDC pra Postgres/MongoDB (MySQL é CDC-only, sem batch pra alternar).
14. ~~**Manifests k8s reais (Deployment/Service/PVC/Secret/HPA) ainda não escritos**~~ — resolvido: Fase 19, `packaging/kubernetes/` + `packaging/swarm/`.
15. **Guia de deploy público na web — não implementado, não documentado.** O instalador/binário sozinho não é suficiente pra rodar em produção acessível publicamente. Falta: (a) reverse proxy com TLS na frente do `nexus-server` (o binário serve HTTP puro na porta 8080, sem TLS embutido); (b) bootstrap de segredos reais (chave AES-256-GCM de credenciais, secret de assinatura JWT) — hoje sem processo documentado de geração/rotação; (c) criação do primeiro usuário Admin via `POST /users` documentada como passo de setup; (d) troca de SQLite pra Postgres pro backend de metadados quando for multi-usuário concorrente (`sqlx` já suporta os dois, só falta o guia); (e) firewall/security group expondo só 443/80, nunca a porta 8080 direta. Caminho mais simples: a imagem Docker já publicada no GHCR (multi-arch, non-root, `/health`) atrás de Caddy/nginx com TLS via Let's Encrypt — documentar como `docs/guides/DEPLOY_WEB.md` (ainda não existe).
16. **Billing da org GitHub bloqueado — afeta todo runner hospedado, não só macOS/Windows.** Descoberto ao mergear a PR do item 5: `build (linux, x86_64, ubuntu-latest)` e `build (linux, arm64, ubuntu-24.04-arm)` falharam com o mesmo erro ("recent account payments have failed or your spending limit needs to be increased") que já tinha bloqueado macOS/Windows — ou seja, é bloqueio de conta inteira, não uma cota específica de runner caro. Mitigado: `build`'s entrada `linux/x86_64` movida pro self-hosted `[self-hosted, Linux]` (mesma máquina do `ci.yml`), sem dependência de billing — e essa é a única entrada do matrix agora. `linux/arm64` **removido do matrix** (não só desabilitado) — sem alternativa self-hosted, aparecia como run vermelho a cada push mesmo sem afetar x86_64 (`fail-fast: false` evita cancelamento cruzado, mas não evita o próprio job aparecer falho). Religar quando o billing for resolvido em `Settings > Billing and plans` — só o usuário (admin da org) resolve isso; definição exata do matrix entry preservada no histórico do git.

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

## Fase 11 — Distribuição multiplataforma ✅ (Windows/macOS não validados em máquina real)
- [x] Single binary com frontend embutido (`rust-embed`, feature `embed-ui`)
- [x] Empacotamento: AppImage/deb/rpm (Linux, todos testados, e desde a rodada de release CI abaixo buildados automaticamente em CI) — `.msi` (Windows), winget (Windows) e Homebrew/dmg (macOS) têm specs em `packaging/` mas nenhum roda em CI hoje (`.msi` chegou a rodar e achou um bug real de OpenSSL, ver item 5 das Pendências ativas; os demais nunca foram wireados) nem foram validados em máquina real
- [x] Imagem Docker com perfil `cuda` selecionável via `--build-arg RUNTIME_IMAGE` (base image + `--gpus all` prontos; aceleração real pendente da Fase 5's `cuda` feature), publicada no GHCR *e* Docker Hub (`ailake/nexusflow`) a cada push pra `main` — modelo de release contínua, não mais por tag `git` (ver item 8 das Pendências ativas) —, multi-arch (amd64 nativo + arm64 via QEMU) — ver esse mesmo item sobre o trade-off de build sob emulação
- [x] Script de instalação `curl | sh` (`scripts/install.sh`) + `.github/workflows/release.yml`

## Fase 12 — Enterprise connectors (paralelo, repo separado)
- [ ] Definir primeiro conector pago (candidato: Snowflake/Databricks avançado)
- [ ] Mecanismo de license key (JWT) validado em `nexus-server`
- [ ] Ver [`LICENSING.md`](./LICENSING.md)

## Fase 13 — Todos os conectores linkados no binário (fora do plano original)
- [x] Os 14 conectores que só existiam no workspace aninhado `crates/nexus-connectors` agora também são feature opcional em `nexus-server` (`connectors-all`), aparecendo de verdade no catálogo `GET /connectors` — antes só postgres/sqlite estavam linkados no binário servido pra UI.

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

- [x] `postgres-cdc` (feature `cdc` do `nexus-connector-postgres`, crate `pg_walstream`) — lê direto do protocolo de replicação lógica do Postgres (`pgoutput`). O replication slot é criado automaticamente no primeiro connect; a publicação (`CREATE PUBLICATION ... FOR TABLE ...`) precisa existir de antemão (não criada automaticamente, mesmo pré-requisito operacional que o Debezium já exige). Resume via o próprio slot — Postgres guarda o ponto server-side, sem precisar de checkpoint externo. Testado com Postgres real via testcontainers.
- [x] `mongodb-cdc` (mesmo crate `nexus-connector-mongodb`, sem dependência nova) — Change Streams nativo do driver oficial (`Collection::watch()`). Requer MongoDB como replica set (mesmo single-node serve). Testado com MongoDB real via testcontainers — inclusive um caso real onde `full_document: updateLookup` não retorna o documento atualizado (fallback pra `document_key`, linha não é descartada).
- [x] `mysql-cdc` (novo crate `nexus-connector-mysql`, dependência `mysql_cdc`) — lê o binlog direto, CDC-only (sem modo batch). Colunas mapeadas **posicionalmente** (protocolo binlog não carrega nome de coluna por padrão), diferente de Postgres/MongoDB que casam por nome. Requer `binlog_format=ROW`/`binlog_row_image=FULL` e um usuário com `REPLICATION SLAVE`/`REPLICATION CLIENT`. Testado com MySQL real via testcontainers.
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
- [x] Mesmo padrão de resume dos outros 3 CDC nativos (Fase 18): campo estático no config (`starting_version`/`starting_snapshot_id`), sem auto-avanço via checkpoint entre runs — mesmo precedente do `start_offsets` do Kafka.
- [x] Nenhuma dependência nova em nenhum dos 3 — tudo via API já pública das dependências existentes de cada conector.

**Critério de pronto:** teste de integração real por conector (escrever insert/update/delete via o sink batch já existente, ler de volta via a fonte CDC nova, validar opcode e valores) — sem testcontainers, os 3 formatos já são embarcados/locais. **Atingido.**

---

**Critério de "MVP pronto"**: Fases 0–3 + 7 (parcial: auth básica) + 8 (canvas mínimo) funcionando end-to-end — mover dados de Postgres pra Postgres via canvas visual, com checkpoint por partição, retry e escrita idempotente. **Atingido e superado** — Fases 0–11 e 13–17 completas, só falta Fase 12 (enterprise, repo separado) e os itens condicionais/parciais marcados acima.

## Débitos conhecidos (aceitos pro MVP, resolver antes de vender enterprise)

- **Secrets via env var, sem KMS/rotação** — ok pra self-host single-tenant; precisa migrar pra KMS (AWS/GCP/Vault) antes do primeiro cliente enterprise (`ARCHITECTURE.md §10`).
- **RBAC sem escopo por recurso** — 4 papéis globais chega pro MVP; SaaS multi-tenant vai exigir permissão por pipeline/credencial.
- ~~**Ciclo de vida do modelo ONNX indefinido**~~ — decidido 2026-07-30: HF Hub em runtime + cache local (`ARCHITECTURE.md §8`).
- **Execução single-node** — decisão deliberada de escopo, não limitação a esconder do usuário (`ARCHITECTURE.md §6`). Documentar isso claramente também no README quando o produto for anunciado publicamente.
- **5 advisories RustSec aceitos (ver `.github/workflows/ci.yml`'s `cargo-audit` job)**: `RUSTSEC-2023-0071` (rsa, via `jsonwebtoken`'s RS256 — sem correção disponível upstream), `RUSTSEC-2026-0194`/`-0195` (quick-xml, via `object_store`/`datafusion` 54.1.0 — mesmo pin de arrow 58.x abaixo), `RUSTSEC-2025-0009`/`RUSTSEC-2024-0336` (ring/rustls, via `milvus-sdk-rust`'s tonic 0.8.3 — sem release mais nova do SDK). Reavaliar cada um quando a dependência que os carrega soltar uma versão nova.
- **`arrow-array`/`arrow-schema` fixados em `58.4.0` e `adbc_core`/`adbc_driver_manager`/`adbc_ffi` em `0.23.0` (não a última, `0.24.0`) em todo o workspace** — `datafusion` 54.1.0 (última versão publicada) ainda depende de arrow 58.x, enquanto adbc 0.24.0 já exige arrow ≥59. Sem overlap entre as duas, então fixamos tudo em 58.4.0/0.23.0 pra ter um `RecordBatch` só no grafo de dependências. Reavaliar quando o datafusion soltar uma versão em cima de arrow 59+.
