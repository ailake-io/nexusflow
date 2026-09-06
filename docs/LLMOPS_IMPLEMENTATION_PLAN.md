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
Marco L6 — RAG reativo via CDC (diferencial #2), precisa de um fix real
           no `run_partition` do engine — ver desenho concreto na seção
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

## Marco L6 — RAG reativo via CDC (diferencial #2)

**Objetivo:** mudança na fonte dispara re-embed automático das linhas
afetadas, mantendo o índice vetorial fresco.

**Bloqueio de hoje, confirmado no código (`runner.rs`, 2026-09-06):**
`embedding` é **rejeitado explicitamente** no caminho passthrough
("embedding stage is not supported on the no-transform passthrough
path; add a transform node to use embeddings") — só funciona com um
node `transform`, e `ARCHITECTURE.md §7` já documenta que CDC + node de
`transform` passa por `drain_sources` (materializa tudo, nunca termina
pra um source CDC em volume realista). **Mas isso é encanamento, não
limitação de arquitetura** — ver desenho concreto abaixo, verificado
contra o código real, não uma decisão em aberto.

### Como destravar — desenho concreto, verificado contra o código real

**Achado que simplifica tudo:** a restrição não é técnica de verdade —
é só encanamento faltando. Confirmado lendo o código (2026-09-06):

- `nexus_ai::embedding::apply_embedding(batch: &RecordBatch, spec,
  backend) -> Result<RecordBatch, EmbeddingError>` (`nexus-ai/src/
  embedding/pipeline.rs:111`) já opera **um `RecordBatch` por vez** —
  não depende do dataset inteiro, nunca precisou de `drain_sources`.
  `apply_embedding_stage` (`runner.rs:1220`) só chama isso num loop
  `for batch in inputs`.
- `PipelineEngine::run_partition` (`nexus-core/src/pipeline.rs:114`) —
  o caminho passthrough que CDC já usa corretamente — tem uma task
  leitora (`source.read_batches()` → `tx.send(batch)`) e uma task
  escritora (`rx.recv()` → **direto** `sink.write_batch(batch)`,
  `pipeline.rs:152-158`). **Não existe nenhum estágio de transformação
  no meio** — é por isso que passthrough nunca precisou de
  `drain_sources`: ele nunca fez nada além de repassar o batch adiante.

Ou seja: `embedding` já é compatível com processamento em streaming por
natureza (função pura por batch); só nunca foi conectado no caminho que
já faz streaming de verdade. A opção B do parágrafo anterior (node
dedicado fora do engine) não é necessária — dá pra resolver dentro do
próprio `PipelineEngine`, sem duplicar lógica de streaming/checkpoint.

**Mudança concreta (3 pontos, em ordem):**

1. **`nexus-core::pipeline`** — `run_partition` ganha um parâmetro novo
   opcional, um closure genérico (nexus-core não pode saber de
   `nexus-ai` — regra de camada já estabelecida, mesma razão de
   `RunLogStore` viver só em `nexus-server`):
   ```rust
   pub type BatchTransform =
       Box<dyn Fn(RecordBatch) -> BoxFuture<'static, Result<RecordBatch, NexusError>> + Send + Sync>;

   pub async fn run_partition(
       &self,
       handle: PartitionHandle,
       progress: Option<ProgressSender>,
       batch_transform: Option<BatchTransform>,   // novo
   ) -> Result<PartitionStats, NexusError> {
   ```
   Na task escritora (`pipeline.rs:152`), antes de `sink.write_batch(batch)`:
   ```rust
   let batch = match &batch_transform {
       Some(f) => f(batch).await?,
       None => batch,
   };
   ```
   Um batch de entrada pode virar um batch com **mais linhas** (chunking
   expande 1 linha em N chunks) — sem problema, o resto do loop
   (contagem de rows/bytes escritos, checkpoint) já opera sobre o batch
   que efetivamente foi escrito, não assume 1:1 com o que foi lido.

2. **`nexus-server::runner`** — em `run_passthrough_pipeline`, remover a
   rejeição atual (`runner.rs:754-757`) e, quando `spec.embedding.is_some()`:
   carregar o backend uma vez (`nexus_ai::embedding::
   load_embedding_backend`, mesma chamada que `apply_embedding_stage` já
   faz) e construir o `BatchTransform` fechando sobre `backend`+`spec`,
   chamando `nexus_ai::embedding::apply_embedding` por dentro. Passar
   esse closure pra `run_partition`. Sem essa flag, comportamento
   idêntico a hoje (closure `None`).

3. **Nada muda em `ARCHITECTURE.md §7`'s CDC** — os 3 conectores CDC
   nativos já entregam pro passthrough exatamente como fazem hoje; o
   `resume_state`/checkpoint por partição já funciona igual, porque a
   transformação acontece **depois** da leitura e **antes** da escrita,
   sem tocar no mecanismo de posição/resume do source.

**Por que isso não foi feito assim desde o início:** `embedding` nasceu
pensado pra combinar com `transform` (SQL, fan-in/fan-out — caso de uso
original, `ARCHITECTURE.md §8`), e a proibição no passthrough foi
provavelmente só "não testamos essa combinação ainda" virando um erro
explícito em vez de um bug silencioso — não uma limitação de design
proposital documentada em lugar nenhum.

**Critério de pronto:** pipeline `postgres-cdc → embedding → lancedb`
(sem node `transform`) processando um WAL ao vivo, streaming de verdade
(sem esperar o WAL "acabar", que nunca acontece) — mesmo teste de
integração real que os outros conectores CDC já usam
(testcontainers), agora com uma asserção a mais: linha nova no Postgres
aparece como embedding novo no LanceDB em poucos segundos, sem reiniciar
o pipeline.

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
                                                        L6 (RAG reativo) — precisa do
                                                          fix de encanamento no
                                                          run_partition (ver seção
                                                          própria), não depende de L5

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
