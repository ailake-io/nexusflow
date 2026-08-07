# Plano de Ação — Revisão Completa NexusFlow

Este documento consolida os achados de uma revisão completa do projeto (backend Rust, conectores, frontend, CI/CD e documentação) e propõe um plano de correções priorizado.

## 1. Resumo Executivo

O NexusFlow tem uma base arquitetural sólida para um MVP, com bons pontos em:
- Separação em crates e conectores feature-gated.
- Criptografia AES-256-GCM de segredos em repouso.
- RBAC hierárquico e middleware de autorização.
- Checkpointing por partição e contrato CDC com opcode.

No entanto, existem **riscos críticos de segurança e dados** que precisam de atenção imediata, além de gargalos de performance, débitos técnicos e divergências entre documentação e código. Este plano organiza os itens por prioridade e sugere uma ordem de execução.

---

## 2. Itens Críticos (corrigir primeiro)

### Segurança

| # | Problema | Onde | Impacto | Ação |
|---|---|---|---|---|
| C1 | `validate_security()` não é chamado em create/update de pipelines | `nexus-server/src/lib.rs:505,561` | Usuário Write persiste spec inseguro; Execute executa depois | ✅ Resolvido: `validate_security()` é chamada em `create_pipeline_handler` e `update_pipeline_handler` |
| C2 | Bypass de SSRF via `user:pass@host` em URL | `nexus-core/src/dag.rs:316-320` | Acesso a metadata endpoints de cloud | Usar `url::Url` para parsing robusto de host |
| C3 | Path traversal relativo permitido em `dbt.project_dir` | `nexus-core/src/dag.rs:274` | Execução de dbt em diretório arbitrário | Rejeitar `..`, canonicalizar contra diretório base |
| C4 | `validate_security()` só inspeciona chaves de primeiro nível | `nexus-core/src/dag.rs:292-314` | URLs internas em JSON aninhado/array passam | ✅ Resolvido: `validate_security()` percorre JSON recursivamente (objetos e arrays) |
| C5 | `is_internal_host()` não cobre IPv6 link-local, CGNAT, `metadata.google.internal` etc. | `nexus-core/src/dag.rs:322-368` | SSRF para endpoints internos comuns | ✅ Resolvido: blocklist expandida para CGNAT, link-local, loopback e hostnames de metadata cloud |
| C6 | Segredos literais no CI | `.github/workflows/ci.yml:125-126` | JWT secret e encryption key expostos em logs/YAML | ✅ Resolvido: segredos gerados dinamicamente via `openssl rand -hex 32` no step de smoke test |
| C7 | `LoginRateLimiter` cresce sem limites | `nexus-server/src/rate_limit.rs:12-38` | Ataque de memória via spoofing de IP | ✅ Resolvido: limite máximo de IPs rastreados com evicção do mais antigo |

### Dados / Confiabilidade

| # | Problema | Onde | Impacto | Ação |
|---|---|---|---|---|
| C8 | `IcebergSink` é append-only → duplicatas em retries | `nexus-connector-iceberg/src/sink.rs:159-172` | Viola contrato de idempotência | Implementar `merge_insert` ou restringir uso |
| C9 | `KafkaSource` comita offsets antes do processamento | `nexus-connector-kafka/src/source.rs:181` | Perda de dados em crash | Commitar offsets só após checkpoint do engine |
| C10 | `KafkaSource` avança offset de mensagens com payload vazio | `nexus-connector-kafka/src/source.rs:154-159` | Dados reais podem ser pulados | Mover update de `last_offsets` para após parse |
| C11 | `OdbcSink` sem transação | `nexus-connector-odbc/src/sink.rs:73-138` | Batch parcial em falha | Envolver em `BEGIN/COMMIT/ROLLBACK` |
| C12 | `PgVectorSink` sem transação e connection task órfã | `nexus-connector-pgvector/src/sink.rs:33-37,48-103` | Estado parcial; erros não propagados | Usar `client.transaction()` e propagar erros |

### Performance / Operacional

| # | Problema | Onde | Impacto | Ação |
|---|---|---|---|---|
| C13 | Modelo ONNX recarregado a cada batch | `nexus-ai/src/embedding/pipeline.rs:127`, `nexus-server/src/runner.rs:218` | Latência extrema; downloads repetidos | Carregar uma vez por run/pipeline |
| C14 | Fontes materializam tabela inteira em memória; sinks fan-out serial | `nexus-core/src/pipeline.rs:213-275` | OOM em tabelas grandes; sinks lentos bloqueiam uns aos outros | ✅ Resolvido: `drain_sources` retém materialização apenas onde exigido pelo SQL transform; `fan_out_write` agora delega a `fan_out_write_stream`, que escreve para cada sink em paralelo via broadcast channel |
| C15 | Ausência de timeouts em I/O de conectores | vários | Runtime bloqueado indefinidamente | `tokio::time::timeout` em conexões/queries |

---

## 3. Itens de Alto Impacto

### Backend

| # | Problema | Onde | Ação |
|---|---|---|---|
| A1 | Sem revogação de JWT | `nexus-server/src/auth.rs:21-24` | ✅ Resolvido: `TokenBlocklist` em memória com `JwtCodec::with_blocklist`; `/auth/logout` adiciona token à blocklist |
| A2 | Scheduler sem coordenação em multi-réplica | `nexus-server/src/scheduler.rs:19-29` | ✅ Escopo deliberado: documentado em `ARCHITECTURE.md §6` que o MVP/OSS é single-node; não é bug, é decisão de escopo |
| A3 | `dbt.rs` sem timeout nem limite de saída | `nexus-server/src/dbt.rs:178-262` | ✅ Resolvido: timeout via `NEXUS_DBT_TIMEOUT_SECONDS` (default 300s) + truncamento de stdout/stderr em 1 MiB |
| A4 | `sanitize_error` não remove credenciais em query strings | `nexus-server/src/error.rs:80-113` | Usar `url::Url` para sanitizar query params |
| A5 | `ProgressHub` usa `std::sync::Mutex` em async | `nexus-server/src/progress.rs:14-37` | Migrar para `tokio::sync::Mutex` |
| A6 | `alerts.rs` fire-and-forget sem observar tasks | `nexus-server/src/alerts.rs:35-46` | Logar falhas; aguardar JoinHandle em testes |
| A7 | `pipeline_id` sem restrição de caracteres/comprimento | `nexus-core/src/dag.rs:162-164` | Validar `[A-Za-z0-9_-]{1,128}` |
| A8 | `NodeSpec.name` não validado como identificador SQL seguro | `nexus-core/src/dag.rs:161-201` | Aplicar `validate_identifier` |
| A9 | `split_by_opcode` trata opcode inválido como upsert | `nexus-core/src/cdc.rs:62-67` | Rejeitar opcodes desconhecidos |

### Conectores

| # | Problema | Onde | Ação |
|---|---|---|---|
| A10 | Materialização eager em fontes SQL/lakehouse | `postgres/source.rs`, `sqlite/source.rs`, `deltalake/source.rs`, `parquet/source.rs`, `ailake/source.rs`, `rest/source.rs` | ✅ Resolvido: todas as sources listadas agora retornam streams lazy; Kafka e Iceberg já eram streaming |
| A11 | `MongoSink` linha a linha | `nexus-connector-mongodb/src/sink.rs:72-98` | ✅ Resolvido: `MongoSink` detecta MongoDB >= 8.0 via `buildInfo` e usa `Client::bulk_write` com `ReplaceOne`/`DeleteOne`; servidores mais antigos mantêm o fallback concorrente com `buffer_unordered` |
| A12 | `OdbcSink` abre conexão por batch e N round-trips | `nexus-connector-odbc/src/sink.rs:73-138` | ✅ Resolvido: worker thread dedicado mantém `Environment`/`Connection` abertos por toda a vida do sink; cada batch é enviado via channel e executado dentro de uma transação `BEGIN/COMMIT/ROLLBACK`; statements são pré-alocados por batch. Bulk parameters columnares permanecem como melhoria futura quando o driver/destino suportar `MERGE` nativo |
| A13 | `AilakeSource` carrega todos os data files em memória antes do stream | `nexus-connector-ailake/src/source.rs:142-161` | ✅ Resolvido: `read_batches` agora retorna um stream lazy que lê e filtra cada arquivo de dados um a um; deletes continuam acumulados em `HashSet` porque são esperados pequenos comparados aos data files |
| A14 | `RestSource` sem timeout/retry/rate-limit | `nexus-connector-rest/src/source.rs:30-53` | ✅ Resolvido: `RestConnectorConfig` adicionou `timeout_seconds`, `retries`, `retry_backoff_seconds` e `requests_per_second`; `reqwest::Client` usa timeout; `fetch_page_with_retry` faz backoff exponencial para erros transientes; `fetch_page_with_rate_limit` enforça RPM entre páginas |
| A15 | `PostgresSource` assume PK `Int64` | `nexus-connector-postgres/src/introspect.rs:29-75` | ✅ Resolvido: `primary_key_bounds` retorna `PkPartitionKind::Int64` para colunas inteiras (Int8/16/32/64) e `PkPartitionKind::NonNumeric` para outros tipos; `nexus-server` cria uma única partição não-particionada (`range = None`) quando o PK não é inteiro |
| A16 | `DeltaSink::open()` mascara erros | `nexus-connector-deltalake/src/sink.rs:33-39` | Distinguir `NotFound` de outros erros |
| A17 | `IcebergSink` pode colidir nomes de arquivo | `nexus-connector-iceberg/src/sink.rs:36-37,115-119` | Usar timestamp/UUID no nome |

### Frontend

| # | Problema | Onde | Ação |
|---|---|---|---|
| A18 | `useRunProgress` não limpa WebSocket/timeout no desmonte | `frontend/src/hooks/useRunProgress.ts:47,67,73` | ✅ Resolvido: `useEffect` de cleanup fecha WebSocket, aborta fetch e limpa timer de inatividade |
| A19 | `useRunProgress` pode aplicar estado de run anterior | `frontend/src/hooks/useRunProgress.ts:103-126` | ✅ Resolvido: `cleanupRun()` reseta estado e refs antes de iniciar nova run; callbacks usam refs estáveis |
| A20 | WebSocket sem tratamento de erro/timeout | `frontend/src/hooks/useRunProgress.ts:73-99` | ✅ Resolvido: `ws.onerror`, `ws.onclose`, timeout de inatividade de 30s e fallback para polling do histórico |
| A21 | Callback instável `onPipelineLoaded` no `App` | `frontend/src/App.tsx:52`, `DagCanvas.tsx:196-200` | ✅ Resolvido: `onPipelineLoaded` envolvido em `useCallback` no `App` |
| A22 | Deleção de pipeline sem confirmação | `frontend/src/components/PipelinesList.tsx:133-140` | ✅ Resolvido: diálogo inline de confirmação com Cancelar/Confirmar |
| A23 | JWT em `sessionStorage` sem CSP | `frontend/src/lib/auth.tsx:11`, `index.html` | ✅ Parcialmente resolvido: 401 global capturado em `api.ts` (`onUnauthorized`); CSP no `index.html` ainda pendente |
| A24 | `radix-ui` inteiro como dependência | `frontend/package.json:20` | Trocar por pacotes individuais |
| A25 | `shadcn` em dependencies | `frontend/package.json:23` | Mover para devDependencies ou remover |
| A26 | Zero testes de comportamento | `frontend/src/lib/utils.test.ts` (único) | Adicionar testes para hooks e componentes |

### CI/CD

| # | Problema | Onde | Ação |
|---|---|---|---|
| A27 | Workflows sem `permissions` explícitas | `.github/workflows/*.yml` | Adicionar `permissions: contents: read` mínimas |
| A28 | Workflows sem `timeout-minutes` | `.github/workflows/*.yml` | Definir timeouts por job |
| A29 | Tags flutuantes de imagens base no Dockerfile | `Dockerfile:17,26,37,46,80` | Pin por digest SHA-256 |
| A30 | Actions de terceiros sem pin por SHA | todos workflows | Pin por SHA com Dependabot |
| A31 | Build ADBC sem pin de tag/commit | `scripts/build-adbc-*.sh:39-40` | Fixar `ADBC_REF` e verificar integridade |
| A32 | Instalação Rust no Windows sem verificação | `.github/workflows/connectors-heavy.yml:39-40` | Verificar checksum/assinatura |
| A33 | Runners self-hosted sem isolamento para PRs | `.github/workflows/ci.yml` | Usar GitHub-hosted para PRs ou isolar |
| A34 | Rebuilds redundantes de frontend/ADBC/binário | vários jobs | ✅ Resolvido: CI `ci.yml` agora faz upload de `frontend/dist` no job `frontend` e o reutiliza nos jobs `clippy` e `test` via `download-artifact` (A34). Release continua construindo ADBC drivers uma vez por tarball; Docker ainda recompila — melhoria adicional possível reaproveitando tarball no Dockerfile |
| A35 | Releases sem assinatura | `.github/workflows/release.yml` | ✅ Resolvido: job `publish` importa chave GPG (`crazy-max/ghaction-import-gpg`) e gera assinaturas `.asc` para cada tarball e para `SHA256SUMS`; artefatos assinados são anexados ao release GitHub. Requer secrets `GPG_PRIVATE_KEY` e `GPG_PASSPHRASE` |

---

## 4. Itens de Médio Impacto

| # | Problema | Onde | Ação |
|---|---|---|---|
| M1 | `nexus-ai` `new_empty_array()` panica para tipos não suportados | `nexus-ai/src/embedding/pipeline.rs:219-240` | Retornar erro ou suportar mais tipos |
| M2 | Schema mutável indevidamente em `apply_embedding` | `nexus-ai/src/embedding/pipeline.rs:109-116` | Preservar nullabilidade original |
| M3 | `nexus-ai` assume nomes fixos de inputs ONNX | `nexus-ai/src/embedding/inference.rs:124-131` | Descobrir inputs dinamicamente |
| M4 | `nexus-ai` `embed_batch` converte `dimension` para `i32` sem checar overflow | `nexus-ai/src/embedding/pipeline.rs:142` | Validar `dimension <= i32::MAX` |
| M5 | `nexus-ai` suporta poucos tipos primitivos em `replicate_array` | `nexus-ai/src/embedding/pipeline.rs:150-217` | Expandir suporte ou documentar |
| M6 | `auth_store.rs` permite username arbitrário | `nexus-server/src/auth_store.rs:70-93` | Validar formato |
| M7 | `seed_admin_if_empty` com condição de corrida | `nexus-server/src/auth_store.rs:55-68` | Usar transação ou `INSERT OR IGNORE` |
| M8 | `auth_store.rs` usa `.expect` na serialização de `Role` | `nexus-server/src/auth_store.rs:82-84,141-144` | Propagar erro |
| M9 | `pipeline_store.rs` não cria índices | `nexus-server/src/pipeline_store.rs:94-122` | Adicionar `CREATE INDEX` |
| M10 | `pipeline_store.rs` `encode_spec` usa `.expect` | `nexus-server/src/pipeline_store.rs:393` | Propagar erro |
| M11 | `run()` usa porta hardcoded 8080 | `nexus-server/src/lib.rs:883` | Ler `NEXUS_PORT` |
| M12 | `telemetry.rs` `try_init()` não é idempotente | `nexus-server/src/telemetry.rs:34-91` | Tornar idempotente |
| M13 | `LoginRateLimiter` responde 429 com texto puro | `nexus-server/src/rate_limit.rs:61-66` | Retornar JSON |
| M14 | `embedded_ui.rs` sem cache headers | `nexus-server/src/embedded_ui.rs:17-27` | Adicionar cache-control |
| M15 | `nexus-ai` não valida assinatura/checksum do modelo | `nexus-ai/src/embedding/model.rs:30-40` | Documentar risco; planejar pin de hash |
| M16 | `ConnectorPalette` não acessível por teclado | `frontend/src/components/ConnectorPalette.tsx` | Adicionar `role`, `tabIndex`, handlers |
| M17 | `FieldHint` não acessível | `frontend/src/components/FieldHint.tsx` | `aria-describedby`, fechar com Escape/clique fora |
| M18 | Polling do status board não pausa em background | `frontend/src/components/PipelineStatusBoard.tsx:52-55` | Usar `document.visibilityState` |
| M19 | IDs de node globais e mutáveis | `frontend/src/components/DagCanvas.tsx:39`, `lib/dag.ts:191` | Usar `crypto.randomUUID()` ou contador no componente |
| M20 | `handleImport` não valida JSON | `frontend/src/components/DagCanvas.tsx:188-191` | Validar schema mínimo |
| M21 | Ausência de Error Boundary | `frontend/src/main.tsx` | Adicionar Error Boundary |
| M22 | Bundle carrega DAG em todas as views | `frontend/src/App.tsx` | ✅ Resolvido: `DagCanvas`, `PipelinesList`, `PipelineStatusBoard` e `UsersPanel` são carregados via `React.lazy`; `Suspense` exibe fallback enquanto a view é carregada; build gera chunks separados para cada rota |
| M23 | `index.html` lang="en" fixo | `frontend/index.html:2` | Sincronizar com idioma selecionado |
| M24 | Falta de smoke test nos releases | `.github/workflows/release.yml` | Extrair tarball e rodar `--version` |
| M25 | Build de Windows/macOS inacabado | `packaging/windows/`, `packaging/macos/` | Validar e finalizar scripts |

---

## 5. Divergências Documentação vs Código

| # | Documentação diz | Código real | Ação |
|---|---|---|---|
| D1 | Stack inclui "Next.js (TypeScript) ou Vite" | Usa Vite apenas | Atualizar `CLAUDE.md` |
| D2 | Conectores MySQL, DuckDB, Snowflake, BigQuery, ClickHouse ADBC | Não existem crates | Atualizar matriz ou implementar |
| D3 | Arrow Flight SQL connectors | Nenhum registrado | Atualizar matriz ou implementar |
| D4 | Alertas Teams/PagerDuty/Email/Webhook | Slack implementado | ✅ Resolvido: Slack, MS Teams, PagerDuty, Email (SMTP STARTTLS) e Webhook genérico implementados; docs atualizadas |
| D5 | Stats de hardware no WebSocket | Não implementado | Atualizar docs ou implementar |
| D6 | CDC nativo via WAL/binlog | Só Debezium+Kafka | Atualizar docs |
| D7 | Features CUDA/Metal/API de embeddings | CPU apenas | ✅ Resolvido: features `api` (HTTP externo), `cuda` (ONNX CUDA EP) e `metal` (CoreML EP) registradas e compiláveis; validação em hardware real pendente |
| D8 | Cron com 5 ou 6 campos (Quartz) | Verificar suporte real | Atualizar docs se só 5 campos |
| D9 | GETTING_STARTED exemplo `postgres → sqlite` com path absoluto | `runner.rs:37-44` rejeita path absoluto | Corrigir exemplo ou regra |
| D10 | README: "MVP completo e além" | Muitos itens aspiracionais | Atenuar declaração ou listar gaps |
| D11 | ARCHITECTURE.md §12: rota `/spec` protegida por Write | Verificar se realmente exige Write | Confirmar e manter docs |

---

## 6. Proposta de Execução (fases)

### Fase 1 — Segurança e Estabilidade (semana 1-2)
- C1-C7: corrigir validações de segurança e rate limiter.
- A1: revogação de JWT (blocklist simples em memória).
- A3: timeouts em dbt subprocess.
- C15, A15, A27-A30: timeouts e hardening de CI.
- Frontend: A18-A21 (cleanup de WebSocket), A22 (confirmação de delete), A23 (CSP/401).

### Fase 2 — Confiabilidade de Dados (semana 2-3)
- C8-C12: idempotência e atomicidade em sinks críticos.
- A9-A10: opcodes inválidos e streaming de fontes.
- A4: sanitização de erros aprimorada.
- A11-A14: bulk operations e resiliência de rede.

### Fase 3 — Performance e Escalabilidade (semana 3-4)
- C13-C14: cache de modelo ONNX e streaming no transform pipeline.
- A10-A14: fontes lazy e bulk writes.
- M22: lazy loading no frontend.

### Fase 4 — Qualidade e Documentação (semana 4-5)
- M1-M5, M16-M23: robustez do frontend e nexus-ai.
- A24-A26: dependências e testes do frontend.
- D1-D11: alinhar documentação com código.
- A31-A35: melhorias de CI/CD e releases assinados.

### Fase 5 — Conectores e Plataformas Adicionais (semana 5+)
- D2-D3: implementar conectores documentados (MySQL, DuckDB, etc.) conforme prioridade de negócio.
- D4-D6: alertas adicionais, stats de hardware, CDC nativo (se houver demanda).
- Windows/macOS packaging finalizado.

---

## 7. Métricas de Sucesso

- Todos os workflows de CI passando com `timeout-minutes` e `permissions` declarados.
- Zero segredos literais em YAML.
- Testes de segurança cobrindo SSRF, path traversal e persistência de configs inseguras.
- Testes de idempotência para `IcebergSink`, `DeltaSink`, `MongoSink` e `PgVectorSink`.
- Frontend com testes para `dag.ts`, `useRunProgress`, `LoginForm` e `PipelinesList`.
- Documentação refletindo o estado real do código.

---

## 8. Notas

- Este plano é um ponto de partida; a ordem pode ser ajustada conforme prioridades de negócio.
- Muitos itens médios/baixos podem ser paralelizados.
- Recomenda-se abrir issues/PRs menores e focados em vez de um PR monolítico.
