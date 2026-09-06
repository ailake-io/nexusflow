# 🤖 MLOps e LLMOps no NexusFlow — levantamento e próximos passos

> **Status: levantamento/ideação (2026-09-05), nada implementado ainda.**
> Este documento registra uma discussão exploratória sobre estender o
> NexusFlow (hoje um framework de movimentação/transformação de dados) em
> direção a MLOps (tracking/registry de modelo) e LLMOps (observabilidade de
> chamadas LLM/RAG). Nenhum destes itens está no `ROADMAP.md` como fase
> comprometida — isso acontece só quando/se o usuário decidir priorizar
> algum item daqui.

## Por que isso é barato de considerar

O NexusFlow já tem, hoje, boa parte da infraestrutura que MLOps/LLMOps
precisam — não como coincidência, mas porque "rodar uma execução,
registrar métricas, versionar uma config, alertar em falha" é o mesmo
problema estrutural que "rodar um pipeline de dados" já resolve:

- **Execução rastreada com métricas** — `pipeline_runs` + `RunLogStore`
  (`ARCHITECTURE.md §15`) já registram início/fim, linhas processadas,
  erro, e uma narração textual por run. É estruturalmente o mesmo modelo
  de dado do "Tracking" do MLflow (run → métricas → params).
- **Pipeline de embeddings já existe** — chunking (fixed-size, recursive,
  semantic) + inferência ONNX/API + 6 sinks vetoriais (`ARCHITECTURE.md
  §8`). É a metade "R" de RAG já pronta.
- **Testes com resultado estruturado já existem** — `dbt_test_result_store.rs`
  guarda nome do teste + pass/fail + timestamp; `QualityPanel.tsx` já
  pensa em unificar teste "nativo" vs "dbt" na mesma UI (ver
  `docs/PLUGIN_STORE_PLAN.md`-style de reuso). Um "teste de LLM" (pergunta
  golden → resposta esperada → score) cabe na mesma forma.
- **Object store genérico já existe** — `csv`/`parquet` já leem/escrevem
  em `s3://`/`gs://`/`az://` via `object_store`. Um artefato de modelo é
  só mais um blob.
- **Conector Redis já existe** — cache de resposta de LLM por hash de
  prompt é `GET`/`SET` num Stream já suportado (ou uma extensão pequena
  pro modo KV, hoje não implementado).
- **Progresso em tempo real via WebSocket** — já intercala um frame
  `hardware_stats` a cada 2s no mesmo canal do progresso de linhas/MB/s
  (`CLAUDE.md §6`). Custo/tokens de LLM cabem no mesmo mecanismo.

## LLMOps — pilares, o que cada um precisa, custo relativo

Ordenado por esforço crescente (do mais barato, que só reaproveita infra
existente, ao mais caro, que é domínio novo):

### 1. Tracing de chamadas LLM — **mais barato**
Node novo `llm` (irmão do node `embedding` já existente, mesmo backend
`api` compatível com OpenAI/vLLM/Ollama que `nexus-ai` já tem). Cada
chamada loga prompt/resposta/tokens/latência como linha estruturada via
`RunLogger` — mesmo mecanismo do `ARCHITECTURE.md §15`, zero storage
novo.

### 2. Custo/tokens — quase de graça junto com o item 1
Somar `tokens_used`/`cost_estimate` (tokens × preço por modelo,
configurável) no mesmo frame de progresso WebSocket que já leva
`hardware_stats`. Agregado no histórico de runs (`PipelineSummary`) do
mesmo jeito que linhas/bytes já são.

### 3. Cache semântico/exato de resposta
Cache por hash do prompt+params. O conector `redis` já existe no
workspace (hoje só modo Streams) — um modo KV simples (`GET`/`SETEX`)
seria extensão pequena, não conector novo.

### 4. Versionamento de prompt
Precisa de abstração nova: um "catálogo de prompt" (nome + versão +
template + variáveis), registrado de forma parecida com o
`ConnectorRegistry` (`nexus-core::registry`, macro `inventory`) — só que
o catálogo aqui seria dados (tabela nova), não código Rust registrado em
compile-time, já que prompt muda em runtime sem precisar de rebuild.

### 5. Avaliação de RAG (recall@k, relevância de chunk)
Reaproveita o pipeline de embedding + os 6 conectores vetoriais já
implementados — roda um conjunto de queries conhecidas contra o vector
store, compara contra ground truth. A lógica de scoring em si é nova.

### 6. Avaliação sistemática (golden dataset, LLM-as-judge, regressão)
Mesmo padrão de storage do item de teste dbt (`dbt_test_result_store.rs`)
generalizado: teste = pergunta golden + resposta esperada + score
(calculado por comparação direta ou delegando a outra chamada LLM como
"juiz", reaproveitando o node `llm` do item 1). Roda como parte do run,
mesmo lugar que `runner.rs` já chama `column_lineage`/quality checks
(ver o plano de linhagem/qualidade desta mesma sessão).

### 7. Guardrails (PII, jailbreak, filtro de conteúdo) — **mais caro**
Node novo tipo "transform" entre chamadas LLM, delegando a um
classificador (regra local ou outro modelo). Domínio genuinamente novo,
nenhuma infra hoje cobre isso.

### 8. Feedback loop humano (👍/👎 ligado a uma geração específica)
Endpoint novo (`POST /pipelines/{id}/runs/{run_id}/feedback` ou similar)
+ tabela nova, pra depois virar dataset de avaliação/fine-tune. Esforço
médio — é CRUD simples, mas é uma capacidade que não existe em nenhuma
forma hoje (o sistema não tem conceito de "geração individual dentro de
um run" pra anexar feedback).

## Diferencial real (não é só copiar Langfuse/LangSmith/Helicone)

Ferramentas de observabilidade de LLM (Langfuse, LangSmith, Helicone)
observam a chamada de LLM **de fora** — instrumentam o código do app,
mas não sabem de onde o dado que virou contexto/RAG veio. O NexusFlow já
é dono do pipeline inteiro **antes** da chamada acontecer (fonte → chunk
→ embed → vector DB). Isso abre duas capacidades que nenhuma ferramenta
de observabilidade pura consegue replicar sem virar também uma
ferramenta de dados:

### 1. Linhagem completa até a geração — o diferencial mais forte

"Essa alucinação veio do chunk #47 do documento X, ingerido da tabela
Postgres Y em 2026-09-01, com o modelo de embedding Z v3" — rastreando
do dado bruto até a resposta específica do LLM. O grafo de linhagem
cross-pipeline já existe (`lineage.rs`) e a linhagem de coluna via
DataFusion também (`nexus-core::transform`) — estender mais um salto
(linha do vector DB → geração do LLM, node `llm` do item 1 acima) dá
continuidade a algo que já existe, não é arquitetura nova do zero.
Langfuse/LangSmith param na resposta do vector DB; não sabem nem que
pipeline gerou aquele chunk, muito menos a tabela de origem.

**O que precisa de verdade:** um mecanismo de rastreamento ligando linha
específica → geração específica — isso é uma junção nova no grafo de
linhagem (hoje o grafo liga pipeline→recurso e coluna→coluna, não
linha→geração de LLM), não é "só adicionar um node".

### 2. RAG reativo via CDC que já existe

A maioria dos sistemas RAG fica com índice desatualizado até alguém
rodar um re-embed manual. O NexusFlow já tem CDC nativo (Postgres WAL,
MongoDB Change Streams, MySQL binlog, `ARCHITECTURE.md §7`) — um evento
de mudança na fonte poderia disparar re-chunk/re-embed automático só das
linhas que mudaram, com linhagem completa de "o que disparou essa
atualização" (reaproveitando o item 1 acima). Resolve staleness de RAG,
problema real que nenhuma ferramenta de observabilidade resolve porque
não são donas da ingestão.

**Fio condutor:** o moat aqui não é "observar chamada de LLM melhor", é
"ser dono do dado desde a origem" — algo que ferramenta que só vende
observabilidade não consegue replicar sem virar concorrente direto do
próprio NexusFlow em movimentação de dados.

## Como empacotar isso como enterprise (mesmo mecanismo dos conectores)

Não precisa de infra de licenciamento nova — o registry de conector já é
agnóstico ao que a "coisa" faz. `ConnectorDescriptor` com
`requires_license: Some(slug)` (`nexus-core::registry`) só guarda um
nome + trava por license; não exige que seja de fato um `Source`/`Sink`
de I/O. Dá pra registrar "linhagem até a geração" ou "RAG reativo via
CDC" como mais uma entrada no catálogo:

```rust
submit_enterprise_connector!(
    "llm-lineage-tracking",
    ConnectorCapability::Capability,  // variante nova, não é I/O
    LlmLineageConfig
)
```

Com isso, de graça, sem construir UI/licenciamento novo:
- Aparece em `GET /connectors` com `licensed: bool`, igual qualquer
  conector pago.
- Cadeado no `ConnectorPalette.tsx` do mesmo jeito que Salesforce/
  Snowflake hoje.
- Aba **Store** já lista, categoriza (ver `ENTERPRISE_CATALOG` do
  `Store.tsx`), mostra bloqueado/liberado sem mudança de código.
- `check_connector_license` (`connectors.rs`) já barra o uso sem
  license, no mesmo ponto que já barra conector pago.

**Trabalho real:** só a lógica em si (linhagem row→geração, ou trigger
de re-embed via CDC) — vive num crate no repo privado
`nexus-connectors-enterprise`, exatamente como Salesforce/SAP vivem
hoje. **Única mudança estrutural:** adicionar uma variante em
`ConnectorCapability` (hoje só `AdbcNative`/`ArrowFlight`/`Bridged`)
pra cobrir "capacidade" em vez de "fonte/destino de dado" — cosmético,
não muda o mecanismo de license.

**Por que isso vende bem:** o cliente que já paga por conector
enterprise (Salesforce, Snowflake) vê a linhagem row→geração-LLM na
mesma tela, mesmo fluxo de compra, sem precisar aprender/adotar outro
produto separado. Reforça o pacote existente em vez de virar upsell de
ferramenta nova.

## MLOps — o que dá pra ter parecido com MLflow

MLflow tem 4 componentes: Tracking, Model Registry, Projects, Serving.
Nem tudo faz sentido pro NexusFlow:

- **Tracking** — viável e barato: estender `pipeline_runs`/`RunLogStore`
  pra aceitar métrica arbitrária (nome + valor + step, tipo
  `log_metric("loss", 0.23, step=10)` do MLflow) em vez de só
  linhas/bytes/erro. Um node de "treino" (shell out pra script
  Python/R, mesmo padrão do `python_transform` já implementado) reportaria
  métricas por época durante a execução.
- **Model Registry** — feature nova real, mas reaproveita o `object_store`
  já usado por csv/parquet como backing de blob. Precisa de: tabela nova
  (nome do modelo, versão, stage `staging`/`production`/`archived`,
  path do artefato, metadata), API CRUD, UI de listagem/promoção.
- **Projects** (empacotamento reproduzível de código de treino) — baixo
  valor agregado pro NexusFlow especificamente; um usuário MLflow já usa
  isso fora do NexusFlow hoje, não há necessidade de duplicar.
- **Serving** — **fora de escopo, de propósito.** Servir modelo em
  produção (inferência de baixa latência) é outra categoria de produto
  (BentoML, Seldon, KServe, Triton) — não é "mover e transformar dados",
  é o oposto do que o NexusFlow faz hoje. Colocar isso aqui seria
  scope creep sério.

## Priorização sugerida

Ordem de custo crescente, cada item reaproveitando o anterior:

```
1. LLMOps #1 (tracing)         — reaproveita RunLogStore
2. LLMOps #2 (custo/tokens)    — reaproveita o frame WebSocket de #1
3. LLMOps #3 (cache)           — reaproveita o conector redis
4. MLOps Tracking              — estende pipeline_runs (paralelo aos 3 acima)
5. LLMOps #4 (prompt versioning)   — abstração nova, mas pequena
6. LLMOps #6 (avaliação sistemática) — reaproveita o padrão de dbt test
7. LLMOps #5 (avaliação de RAG)      — lógica de scoring nova
8. MLOps Model Registry         — storage + API + UI novos
9. LLMOps #8 (feedback humano)  — CRUD novo
10. LLMOps #7 (guardrails)      — domínio novo, sem infra reaproveitável
```

**Não fazer:** MLflow Projects (baixo valor), Model Serving (fora de
escopo de produto — outra categoria de ferramenta).

## Decisão de posicionamento (não técnica, mas real)

Fazer os itens 1-4 acima é incremental — o NexusFlow continua sendo
"framework de movimentação/transformação de dados" com mais um tipo de
node. Fazer os itens 5+ bem feito (principalmente avaliação sistemática +
Model Registry) começa a posicionar o produto também como ferramenta de
ML/LLM experiment tracking — atrai um público novo (cientista de dados
rodando treino/avaliação) diferente do público atual (engenheiro de dados
montando pipeline ETL). Essa é uma decisão de produto a se tomar antes de
ir além do item 4, não uma decisão puramente técnica.

## Referências

- `ARCHITECTURE.md §8` — pipeline de embeddings (`nexus-ai`), base pro
  node `llm`.
- `ARCHITECTURE.md §15` — `RunLogStore`/`RunLogger`, base pro tracing de
  chamadas LLM (item 1).
- `docs/GETTING_STARTED.md` §2 (Embeddings) — exemplo de config do node
  `embedding` hoje, molde pro node `llm` novo.
- `crates/nexus-server/src/dbt_test_result_store.rs` — padrão de storage
  de resultado de teste, base pra avaliação sistemática (item 6).
- Conector `redis` (`crates/nexus-connectors/nexus-connector-redis`) —
  base pro cache (item 3).
