# 🛠️ LLMOps no NexusFlow — plano de implementação

> Complementa `docs/MLOPS_LLMOPS_PLAN.md` (levantamento/ideação, pilares e
> priorização). Este documento é o "como": marcos concretos, arquivos/
> crates tocados, e critério de pronto por marco — mesmo padrão de
> `IMPLEMENTATION_PLAN.md` pro resto do sistema. **Nada implementado
> ainda (2026-09-06)** — planejamento antes de codar, não roadmap
> comprometido.

## Ordem dos marcos

Cada marco depende do anterior estar pronto. Ordem por reaproveitamento
de infraestrutura existente (mais barato → mais caro), não por valor de
negócio isolado — mesma lógica do `IMPLEMENTATION_PLAN.md` original.

```
Marco L1 — Node `llm` + tracing básico
Marco L2 — Custo/tokens agregado
Marco L3 — Cache de resposta
Marco L4 — Versionamento de prompt
Marco L5 — Linhagem row→geração (diferencial #1)
Marco L6 — RAG reativo via CDC (diferencial #2) — BLOQUEADO até resolver
           uma limitação real do engine, ver seção própria abaixo
Marco L7 — Avaliação sistemática (golden dataset)
Marco L8 — Empacotamento enterprise
```

---

## Marco L1 — Node `llm` + tracing básico

**Objetivo:** um node de pipeline que chama um LLM (endpoint compatível
com OpenAI, mesmo backend `api` que `nexus-ai::embedding` já usa) e loga
cada chamada via a infra de run-logs que já existe.

- `nexus-core::dag` — novo `LlmNodeSpec` no `PipelineSpec` (campo
  opcional `llm`, mesmo padrão de `embedding: Option<EmbeddingSpec>`):
  ```rust
  pub struct LlmNodeSpec {
      pub prompt_template: String,       // "{corpo}" interpolado por linha
      pub input_columns: Vec<String>,
      pub output_column: String,          // nome da coluna com a resposta
      pub model: LlmModelConfig,          // backend api: base_url, model, api_key_env
      pub max_tokens: Option<u32>,
      pub temperature: Option<f32>,
  }
  ```
  `LlmModelConfig` reaproveita a mesma forma de `ModelConfig` do backend
  `api` de `nexus-ai::embedding` (`base_url`/`model`/`api_key_env`) —
  não duplicar o cliente HTTP, extrair um `nexus-ai::llm_client` comum
  que os dois backends chamam.
- `nexus-ai::llm` (módulo novo) — `call_llm(prompt, config) ->
  LlmResponse { text, tokens_prompt, tokens_completion, latency_ms }`.
  Função pura + I/O isolado no mesmo padrão que `embedding` já segue
  (chunking/embedding são puros, só o sink final faz I/O — aqui a
  chamada em si É o I/O, mas a construção do prompt a partir do
  template é pura e testável sem rede).
- `nexus-server::runner` — `apply_llm_stage`, mesmo formato de
  `apply_embedding_stage` (`runner.rs:1220`), rodando **depois** do
  `embedding` e **antes** do sink (ou como estágio alternativo/paralelo,
  a decidir se um pipeline pode ter os dois estágios juntos — caso de
  uso real: chunk → embed → **e também** perguntar a um LLM algo sobre
  o texto original, ambos escritos no mesmo sink). **Mesma restrição do
  `embedding` por ora**: só no caminho com `transform` (ver "Limitação
  conhecida" abaixo antes de tentar liberar pro passthrough).
- Tracing: `RunLogger` (`nexus-server::progress`) ganha uma chamada por
  invocação de LLM, nível `Info`, mensagem estruturada (JSON dentro da
  string, mesmo padrão que outras mensagens de log já usam pra dado
  estruturado) contendo: `model`, `tokens_prompt`, `tokens_completion`,
  `latency_ms`, `prompt_len_chars`, `response_len_chars`. **Nunca loga o
  prompt/resposta completos** por padrão (podem conter dado sensível do
  cliente) — só metadados. Um modo `log_full_content: bool` opcional no
  `LlmNodeSpec` para debug, desligado por padrão.

**Critério de pronto:** pipeline com node `llm` rodando contra um
endpoint mock (`wiremock`, mesmo padrão de todo conector REST deste
repo), linha de log aparecendo em `GET /pipelines/{id}/runs/{run_id}/logs`
com tokens/latência reais.

---

## Marco L2 — Custo/tokens agregado

**Objetivo:** ver custo total de LLM por pipeline/run, não só por
chamada individual (que já fica nos logs desde o L1).

- `LlmModelConfig` ganha `cost_per_1k_prompt_tokens`/
  `cost_per_1k_completion_tokens: Option<f64>` (preço varia por
  provedor/modelo, sem tabela universal confiável — o usuário informa).
- Tabela nova `pipeline_run_llm_stats` (mesmo padrão dual-dialeto de
  `pipeline_schema_store.rs`): `run_id`, `tokens_prompt`,
  `tokens_completion`, `cost_estimate`, somados incrementalmente a cada
  chamada (não recalculado no fim — mesma razão do `RunLogger` persistir
  na emissão, não no forwarding).
- Frame `hardware_stats` do WebSocket de progresso ganha um vizinho
  `llm_stats: {tokens, cost}` opcional, mesmo padrão "chave nova,
  frontend ignora se não reconhecer" que `hardware_stats` já estabeleceu.
- `RunHistoryPanel.tsx` — coluna nova "Custo LLM" ao lado de linhas/
  duração já mostradas, só quando o run teve um node `llm`.

**Critério de pronto:** rodar um pipeline com 2-3 chamadas LLM mock,
custo agregado bate com a soma manual, aparece no histórico.

---

## Marco L3 — Cache de resposta

**Objetivo:** não pagar/esperar de novo pela mesma chamada.

- `LlmNodeSpec` ganha `cache: Option<LlmCacheSpec> { backend: "redis",
  url: String, ttl_seconds: u64 }`.
- Chave de cache: `sha256(model + prompt + max_tokens + temperature)`.
- `nexus-connector-redis` hoje só faz Streams (`XADD`/`XREAD`) — precisa
  de um modo KV simples (`GET`/`SETEX`) que não existe ainda. Extensão
  pequena e isolada do modo Streams existente (feature própria dentro do
  mesmo crate, ou um struct `RedisKvClient` ao lado do `RedisSource`/
  `RedisSink` atuais — não misturar responsabilidade).
- `apply_llm_stage` checa o cache antes de chamar `call_llm`; cache miss
  grava depois de receber a resposta.

**Critério de pronto:** rodar o mesmo pipeline duas vezes, segunda vez
com latência ~0 e zero tokens novos contabilizados (tudo veio do cache),
testado contra Redis real via testcontainers (mesmo padrão de todo
conector com estado externo deste repo).

---

## Marco L4 — Versionamento de prompt

**Objetivo:** trocar o texto de um prompt sem editar o `PipelineSpec` a
mão toda vez, e saber qual versão gerou qual resposta.

- Tabela nova `prompt_templates`: `id`, `name`, `version` (auto-
  incrementado a cada update), `template`, `created_at`. Endpoint
  `POST /prompts` (cria versão nova, nunca sobrescreve — mesmo princípio
  de imutabilidade que licenças/checkpoints já seguem neste repo).
- `LlmNodeSpec.prompt_template` vira `LlmNodeSpec.prompt: PromptRef {
  name: String, version: Option<u32> }` — `None` = "última versão",
  setado explicitamente = pin, mesmo padrão do `revision` fixo do
  Hugging Face Hub em `EmbeddingModelSpec` (`ARCHITECTURE.md §8`, "nunca
  latest implícito pra reprodutibilidade" — mesmo racional aqui).
- Cada linha de log do L1 passa a incluir `prompt_name`/`prompt_version`
  usados naquela chamada — fecha o ciclo de "qual versão de prompt gerou
  essa resposta".
- UI: painel novo `PromptLibrary.tsx` (lista/cria/edita, mostra
  histórico de versão) — reaproveita o padrão de formulário que
  `SchemaForm.tsx` já resolve pra outras configs, só que aqui o "schema"
  é fixo (nome + template), não dinâmico por conector.

**Critério de pronto:** duas versões do mesmo prompt, pipeline rodando
com `version: None` pega a mais nova automaticamente; rodando com
`version: 1` fixo continua na versão antiga mesmo depois de criar a v2.

---

## Marco L5 — Linhagem row→geração (diferencial #1)

**Objetivo:** "essa geração veio de qual linha/chunk/fonte".

Esse é o marco mais arquiteturalmente novo — **não é** só mais um node,
é uma aresta nova no grafo de linhagem que não existe hoje (o grafo liga
pipeline→recurso e coluna→coluna, nunca linha específica→geração).

- Tabela nova `llm_generations`: `id`, `run_id`, `partition_id`,
  `source_row_key` (valor da `primary_key` da linha de origem, quando
  existir), `prompt_name`+`prompt_version` (do L4), `model`,
  `context_resource_ids` (array — quais recursos/linhas de vector DB
  foram usados como contexto RAG, quando aplicável), `created_at`. Sem
  guardar prompt/resposta completos por padrão (mesma cautela de dado
  sensível do L1) — só o *identificador* da geração; conteúdo completo
  fica só no log de run (que já tem retenção mais curta/local).
- `lineage.rs` ganha um novo tipo de nó (`LineageNodeKind::Generation`)
  e uma aresta `Resource -> Generation` — `GET /lineage/{id}` passa a
  incluir isso quando o pipeline tiver um node `llm` com `context_from`
  apontando pra outro node (o node que fez a busca vetorial).
- **Decisão de design em aberto, não resolvida aqui:** hoje o pipeline
  do NexusFlow é orientado a *batch* (processa um lote de linhas por
  vez), não a "responder uma pergunta". RAG de verdade é
  "pergunta → busca vetorial → contexto → LLM → resposta", um fluxo de
  *request-response*, não de streaming em lote. Duas opções:
  1. **Node `rag_query` dentro do pipeline batch** — funciona pra RAG em
     lote (ex.: gerar resposta pra cada linha de um dataset de perguntas
     salvo), não pra uma API de pergunta-resposta ao vivo.
  2. **Endpoint novo `POST /rag/query`** fora do modelo de `PipelineSpec`
     — usa a config de um pipeline salvo (fonte vetorial + node `llm`)
     mas executa sob demanda, uma pergunta por vez, sem passar pelo
     scheduler/engine de batch. Mais parecido com o que "RAG" realmente
     significa na prática.
  **Recomendação:** começar pela opção 2 — é o caso de uso real
  (aplicação pergunta algo, quer resposta com linhagem, na hora), e
  opção 1 fica como variante batch depois se aparecer demanda real pra
  "gerar resposta pra uma lista grande de perguntas de uma vez".

**Critério de pronto:** `POST /rag/query` com uma pergunta real, resposta
volta com `generation_id`, `GET /lineage/generation/{id}` mostra os
chunks/linhas de origem usados como contexto.

---

## Marco L6 — RAG reativo via CDC (diferencial #2) — BLOQUEADO

**Objetivo:** mudança na fonte dispara re-embed automático das linhas
afetadas, mantendo o índice vetorial fresco.

**Bloqueio real, confirmado no código (`runner.rs`, 2026-09-06):** hoje
`embedding` é **rejeitado explicitamente** no caminho passthrough
("embedding stage is not supported on the no-transform passthrough
path; add a transform node to use embeddings") — só funciona com um
node `transform`. E `ARCHITECTURE.md §7` já documenta que CDC + node de
`transform` passa por `drain_sources` (materializa tudo antes de
processar), o que nunca termina pra um source CDC em volume realista
(WAL/binlog não têm fim natural). **As duas limitações juntas bloqueiam
esse marco por completo** — não dá pra ter `postgres-cdc` alimentando um
node `embedding` hoje, de nenhuma forma, sem antes resolver uma das
duas.

**Caminho pra destravar** (não implementado, só o desenho):
- **Opção A (mais direta):** estender o caminho *passthrough* (que já
  processa CDC corretamente, streaming de verdade, sem `drain_sources`)
  pra também aplicar `embedding` por linha — hoje o passthrough só faz
  `Source → Sink` direto, sem estágio de transformação nenhum no meio.
  Precisa: `run_partition` (`nexus-core::pipeline`) ganhar um hook
  opcional de transformação por-linha (aplicar embedding num
  `RecordBatch` pequeno por vez, não no dataset inteiro) — mudança real
  no engine central, não trivial, mas resolveria também o item já
  listado em `ROADMAP.md` "Débitos conhecidos" (CDC+transform) de
  quebra.
- **Opção B (mais isolada, menos poder):** um node novo `reactive_embed`
  específico pra esse caso, que não passa pelo `PipelineEngine` genérico
  — um loop dedicado: consome eventos CDC diretamente (sem passar pelo
  runner batch), re-embeda e escreve no sink vetorial. Menos
  reaproveitamento do engine existente, mas não exige tocar no núcleo de
  streaming/checkpoint que hoje é usado por todo o resto do sistema.

**Recomendação:** não começar este marco sem antes resolver a Opção A
(ou decidir formalmente pela B) — é decisão de arquitetura de verdade,
não escolha de implementação de detalhe. Revisitar depois do L5 estar
funcionando (RAG "manual" via `/rag/query`), quando já tiver o node
`llm`/geração/linhagem provados em produção.

---

## Marco L7 — Avaliação sistemática (golden dataset)

**Objetivo:** golden question + resposta esperada → score, regressão
automática quando prompt/modelo muda.

- Mesmo padrão de storage do `dbt_test_result_store.rs`, generalizado:
  tabela `llm_eval_results` (`eval_name`, `run_id`, `prompt_version`,
  `score`, `passed: bool`, `timestamp`).
- Scoring: comparação direta (string match/similaridade) OU delegando a
  outra chamada LLM como "juiz" (reaproveita `call_llm` do L1 —
  `prompt: "A resposta X responde a pergunta Y? Score de 0 a 10."`).
- `QualityPanel.tsx` (já mistura resultado de teste "nativo" vs "dbt",
  ver `docs/MLOPS_LLMOPS_PLAN.md`) ganha uma terceira origem: "LLM eval".
- Roda como parte do `run`, no mesmo lugar que `runner.rs` já chama
  `column_lineage`/quality checks — depois do node `llm`, antes de
  fechar o run.

**Critério de pronto:** golden dataset de 5 perguntas, trocar a versão
do prompt (L4) muda o score médio de forma mensurável, resultado visível
no `QualityPanel.tsx`.

---

## Marco L8 — Empacotamento enterprise

**Objetivo:** decidir o que fica OSS e o que vira pago, usando o
mecanismo que `docs/MLOPS_LLMOPS_PLAN.md` já descreveu (reaproveitar
`ConnectorDescriptor`/`requires_license`, nova variante
`ConnectorCapability::Capability` pra registrar algo que não é I/O).

**Proposta de corte** (a validar com o usuário antes de implementar):
- **OSS:** L1-L4 (node `llm`, custo/tokens, cache, versionamento de
  prompt) — capacidade básica de LLMOps fica no núcleo aberto, mesmo
  espírito do `LICENSING.md` ("núcleo aberto, conectores premium
  fechados" já cobre a mecânica de dados; a mecânica de LLM segue o
  mesmo princípio).
- **Enterprise:** L5 (linhagem row→geração) e L6 (RAG reativo, quando
  destravado) — são os dois diferenciais reais identificados em
  `docs/MLOPS_LLMOPS_PLAN.md`, o valor que justifica cobrar. L7
  (avaliação sistemática) fica OSS ou paga a decidir — tem valor mas não
  é o diferencial central.
- Registro: `submit_enterprise_connector!("llm-lineage-tracking",
  ConnectorCapability::Capability, LlmLineageConfig)` — mesmo crate
  privado `nexus-connectors-enterprise`, sem infraestrutura de license
  nova.

**Critério de pronto:** binário OSS sem license instalada não expõe
`/lineage/generation/{id}` (ou expõe com erro claro de license
ausente); binário enterprise com license cobrindo `llm-lineage-tracking`
expõe normalmente — mesmo teste de padrão que já existe pra conector
pago (`check_connector_license`, ver `connectors.rs`'s testes).

---

## Resumo de dependências entre marcos

```
L1 (node llm + tracing) ──┬──> L2 (custo/tokens)
                          ├──> L3 (cache)
                          └──> L4 (versionamento de prompt) ──> L5 (linhagem row→geração)
                                                                  │
                                                                  v
                                                        L6 (RAG reativo) — bloqueado
                                                          por limitação real do engine,
                                                          não por L5

L7 (avaliação sistemática) depende de L1 (reaproveita call_llm) e L4
(precisa saber qual versão de prompt foi avaliada).

L8 (empacotamento enterprise) só depois de L5 estar funcionando —
precisa ter algo real pra gatear por license, não um esqueleto vazio.
```

## Referências

- `docs/MLOPS_LLMOPS_PLAN.md` — pilares, priorização geral, diferencial,
  mecanismo de empacotamento enterprise (contexto deste documento).
- `ARCHITECTURE.md §7` — limitação real de CDC+transform que bloqueia o
  Marco L6.
- `ARCHITECTURE.md §8` — pipeline de embeddings, molde técnico pro node
  `llm`.
- `ARCHITECTURE.md §15` — `RunLogStore`/`RunLogger`, base do tracing do
  Marco L1.
- `crates/nexus-server/src/runner.rs` — `apply_embedding_stage` (linha
  ~1220), molde direto pro `apply_llm_stage` do Marco L1; linhas 303/754
  confirmam a restrição de `embedding` fora do caminho passthrough que
  bloqueia o Marco L6.
