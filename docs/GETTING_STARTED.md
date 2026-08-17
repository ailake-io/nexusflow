# Primeiros passos com o NexusFlow

Guia prático de instalação e uso — do zero até rodar seu primeiro pipeline. Para arquitetura interna, ver [`ARCHITECTURE.md`](../ARCHITECTURE.md); para a stack completa, [`CLAUDE.md`](../CLAUDE.md).

## 1. Instalação

Escolha uma das opções abaixo. Todas sobem o mesmo binário: um único processo servindo API REST + WebSocket + UI web em `http://localhost:8080`.

### Docker (mais simples)

Imagem publicada no GHCR (já com todos os 24 conectores):

```bash
docker run -d --name nexusflow -p 8080:8080 \
  -e NEXUS_JWT_SECRET="$(openssl rand -hex 32)" \
  -e NEXUS_ENCRYPTION_KEY="$(openssl rand -hex 32)" \
  -e NEXUS_ADMIN_USERNAME=admin \
  -e NEXUS_ADMIN_PASSWORD="troque-isto" \
  ghcr.io/ailake-io/nexusflow:latest
```

Build local (imagem por padrão só liga postgres/sqlite, igual ao binário nativo — ver seção 2 abaixo):

```bash
docker build -t nexusflow .
```

Com conectores extras no build local:

```bash
docker build --build-arg FEATURES=embed-ui,connectors-all -t nexusflow:full .
```

Não validado como build Docker completo nesta sessão (cada conector foi validado via `cargo build` direto, não através do Dockerfile) — se faltar alguma lib no runtime (ex. `zlib1g` pro rdkafka do kafka), adicione no `apt-get install` do estágio `runtime` do `Dockerfile`.

Perfil com base CUDA (`--gpus all`) — usa a mesma imagem numa base `nvidia/cuda`; as features `cuda`/`metal` registram o execution provider ONNX correto, mas **não foram validadas em hardware real** (fallback silencioso pra CPU se driver/GPU não estiver presente):

```bash
docker build --build-arg RUNTIME_IMAGE=nvidia/cuda:12.4.1-runtime-ubuntu22.04 -t nexusflow:cuda .
docker run --gpus all -d -p 8080:8080 -e NEXUS_JWT_SECRET=... -e NEXUS_ENCRYPTION_KEY=... nexusflow:cuda
```

### Script de instalação (Linux/macOS)

```bash
curl -fsSL https://raw.githubusercontent.com/ailake-io/nexusflow/develop/scripts/install.sh | sh
```

Baixa o binário + drivers ADBC pra `~/.local/share/nexusflow` e cria `~/.local/bin/nexusflow`. Precisa de um [release](https://github.com/ailake-io/nexusflow/releases) publicado — ver `.github/workflows/release.yml`. O binário do release já vem com **todos** os 24 conectores linkados (`embed-ui,connectors-all`, não só postgres/sqlite — ver seção 2 abaixo); pra `odbc`/`kafka` funcionarem, precisa de `unixodbc`/`libsasl2` no sistema (o instalador avisa no final se faltar).

### Pacotes nativos (Linux)

```bash
./scripts/package-deb.sh        # nexusflow_<versão>_amd64.deb
./scripts/package-appimage.sh   # NexusFlow-<versão>-x86_64.AppImage
./scripts/package-rpm.sh        # precisa de rpmbuild instalado
```

Mesma coisa: todos os conectores já vêm linkados; o `.deb` declara `unixodbc`/`libsasl2-2` como `Depends`, AppImage/rpm exigem essas libs já presentes no sistema alvo.

Windows (`.msi`/winget) e macOS (Homebrew/`.dmg`) têm specs em `packaging/windows/` e `packaging/macos/`, mas ainda não foram validados em máquina real — ver os comentários em cada arquivo.

### Build a partir do source

```bash
npm --prefix frontend ci && npm --prefix frontend run build
cargo build --release -p nexusflow --features embed-ui
./scripts/build-adbc-postgresql-driver.sh
./scripts/build-adbc-sqlite-driver.sh

export ADBC_DRIVER_POSTGRESQL_PATH="$(pwd)/target/adbc/libadbc_driver_postgresql.so"
export ADBC_DRIVER_SQLITE_PATH="$(pwd)/target/adbc/libadbc_driver_sqlite.so"
export NEXUS_JWT_SECRET="$(openssl rand -hex 32)"
export NEXUS_ENCRYPTION_KEY="$(openssl rand -hex 32)"
export NEXUS_ADMIN_USERNAME=admin
export NEXUS_ADMIN_PASSWORD="troque-isto"

./target/release/nexusflow
```

`--features embed-ui` exige que `frontend/dist` já exista (o `#[derive(RustEmbed)]` lê a pasta em tempo de compilação) — por isso o `npm run build` vem antes do `cargo build`.

## 2. Habilitando conectores

Isso só se aplica a quem builda a partir do source (seção 1, "Build a partir do source") — os binários pré-buildados (script de instalação, `.deb`/AppImage/rpm) e a imagem Docker publicada no GHCR já vêm com `connectors-all` ligado, ver seção 1.

Por padrão um `cargo build` sem flags só liga `postgres` e `sqlite`. A feature `connectors-all` habilita as outras **22 entradas de conector** no catálogo (24 nomes no total, pois a feature `rest` registra tanto `rest` quanto `webhook`): mongodb, kafka, rest, webhook, odbc, milvus, qdrant, lancedb, pgvector, pinecone, chromadb, deltalake, iceberg, parquet, ailake, csv e os 6 CDCs nativos (postgres-cdc, mongodb-cdc, mysql-cdc, deltalake-cdc, iceberg-cdc, ailake-cdc). Cada um só entra no binário se sua feature for pedida:

```bash
# um conector específico
cargo build --release -p nexusflow --features embed-ui,pgvector

# todos de uma vez
cargo build --release -p nexusflow --features embed-ui,connectors-all
```

`kafka` e `odbc` precisam de dependência nativa (`librdkafka` / unixODBC vendorizado) e compilam mais devagar; `milvus`/`lancedb` precisam de `protoc` no PATH. O catálogo servido em `GET /connectors` reflete exatamente o que foi compilado — a UI nunca mostra um conector que não está linkado no binário.

### Embeddings (chunking + ONNX)

Para gerar embeddings no pipeline, adicione um nó `embedding` ao spec (ou arraste o node **+ Embedding** no Canvas — ambos editam o mesmo campo) — ele roda **antes** do transform SQL, expandindo cada linha da source em chunks e adicionando uma coluna `FixedSizeList<Float32>` com os vetores:

```json
{
  "pipeline_id": "rag-pipeline",
  "sources": [{"connector": "postgres", "config": {"uri": "postgres://user:pw@host/db", "table": "docs", "primary_key": "id"}}],
  "transform": {"sql": "SELECT id, body, embedding FROM source0"},
  "sinks": [{"connector": "lancedb", "config": {"uri": "/tmp/vectors", "table": "docs", "primary_key": "id", "embedding_column": "embedding", "dimension": 384}}],
  "embedding": {
    "source_column": "body",
    "output_column": "embedding",
    "dimension": 384,
    "model": {
      "backend": "onnx",
      "repo": "sentence-transformers/all-MiniLM-L6-v2",
      "revision": "main",
      "filename": "model.onnx",
      "tokenizer_filename": "tokenizer.json",
      "max_length": 128
    },
    "chunking": {"strategy": "fixed_window", "chunk_size": 256, "overlap": 32}
  }
}
```

A feature Cargo `embeddings` (incluída em `connectors-all`) liga o crate `nexus-ai` e suas dependências ONNX/HF Hub. Sem ela, um spec com `embedding` retorna erro claro.

> **Reprodutibilidade do modelo:** o campo `revision` do backend ONNX fixa a tag/branch/commit do Hugging Face (ex.: `"main"` ou um hash de commit). Use um hash de commit ou tag explícita para garantir que o mesmo peso seja usado em todos os ambientes.

### Limitações conhecidas de sinks

- **Iceberg** — o sink é *append-only* na versão atual do `iceberg-rust` (0.10.0): não há commit de *equality-delete*/`upsert` na API pública. Para evitar duplicatas em reexecuções, configure `primary_key` no sink — linhas com chave já existente são descartadas antes do `fast_append`. CDC deletes são rejeitados explicitamente com erro claro.
- **Parquet** — implementa CDC upsert/delete como reescrita completa do arquivo; é correto, mas `O(tamanho da tabela)` por batch. A reescrita usa arquivo temporário + `rename` atômico para evitar perda do arquivo original em caso de crash.
- **Kafka source** — `enable.auto.commit` está desligado; offsets são commitados manualmente ao final de cada `read_batches`, alinhados com o checkpoint do pipeline.

## 3. Variáveis de ambiente

| Variável | Obrigatória? | Padrão | Descrição |
|---|---|---|---|
| `NEXUS_JWT_SECRET` | sim | — | Segredo pra assinar/validar JWT. Sem ela o processo não sobe. |
| `NEXUS_ENCRYPTION_KEY` | sim | — | 64 caracteres hex (32 bytes) — chave AES-256-GCM que criptografa credenciais de conector em repouso. Gere com `openssl rand -hex 32`. |
| `NEXUS_CHECKPOINT_DB` | não | `sqlite://nexusflow.db` | Onde ficam os checkpoints de pipeline (retomada por partição). Aceita `postgres://`/`postgresql://` também — ver §4 abaixo. |
| `NEXUS_AUTH_DB` | não | `sqlite://nexusflow-auth.db` | Usuários e papéis (RBAC). Aceita `postgres://`/`postgresql://` também. |
| `NEXUS_PIPELINES_DB` | não | `sqlite://nexusflow-pipelines.db` | Definições de pipeline e histórico de execuções. Aceita `postgres://`/`postgresql://` também. |
| `NEXUS_ADMIN_USERNAME` / `NEXUS_ADMIN_PASSWORD` | não | — | Se as duas estiverem setadas e a tabela de usuários estiver vazia, cria a conta Admin inicial (idempotente — não roda de novo depois). |
| `NEXUS_SLACK_WEBHOOK_URL` | não | — | Sem ela, falhas de pipeline não disparam alerta no Slack. |
| `NEXUS_OTLP_ENDPOINT` | não | — | Endpoint OTLP/HTTP pra exportar traces. Sem ela, traces ficam só como log JSON local; métricas Prometheus em `/metrics` funcionam de qualquer jeito. |
| `NEXUS_ALLOW_INTERNAL_HOSTS` | não | `false` | Quando `true`, permite URLs de conectores apontando para `localhost`, `127.0.0.1` e IPs de LAN privados. Útil para testes locais; em produção mantenha `false` para mitigar SSRF. |
| `ADBC_DRIVER_POSTGRESQL_PATH` / `ADBC_DRIVER_SQLITE_PATH` | sim (se usar postgres/sqlite) | — | Caminho pro `.so`/`.dylib` do driver ADBC — não existe distribuição via crates.io, tem que buildar com `scripts/build-adbc-*-driver.sh`. |

### Metadados em Postgres (multi-réplica / k8s)

Por padrão os 3 metadados acima (`NEXUS_CHECKPOINT_DB`/`NEXUS_AUTH_DB`/`NEXUS_PIPELINES_DB`) usam SQLite — suficiente pra rodar single-node (`ARCHITECTURE.md §6`). Apontando as três pra um Postgres em vez disso (`postgres://user:pass@host:5432/db`), o backend troca automaticamente pelo scheme da URL — sem flag nem env var extra:

```bash
export NEXUS_CHECKPOINT_DB=postgres://user:pass@host:5432/nexusflow
export NEXUS_AUTH_DB=postgres://user:pass@host:5432/nexusflow
export NEXUS_PIPELINES_DB=postgres://user:pass@host:5432/nexusflow
```

As três podem apontar pro mesmo banco Postgres (tabelas não colidem) ou bancos separados. Isso é o pré-requisito real pra rodar mais de uma réplica atrás do mesmo Service em k8s: SQLite não pode ser compartilhado com segurança entre réplicas (não use volume `ReadWriteMany` com ele — é receita de `database is locked`). Com Postgres, o scheduler de cron também coordena via `pg_try_advisory_lock` — só uma réplica dispara cada pipeline agendado por tick, mesmo com N réplicas lendo o mesmo Postgres (em SQLite essa coordenação não existe, mas também não faz sentido — só uma réplica é segura com SQLite de qualquer forma).

**Migrando dados existentes de SQLite pra Postgres**: o binário `migrate-metadata` copia usuários, pipelines salvos, histórico de runs e checkpoints preservando os IDs originais (idempotente — rodar de novo só pula o que já foi migrado):

```bash
cargo run --release -p nexus-server --bin migrate-metadata --features postgres -- \
  --auth-sqlite sqlite://nexusflow-auth.db --auth-postgres postgres://user:pass@host/db \
  --pipelines-sqlite sqlite://nexusflow-pipelines.db --pipelines-postgres postgres://user:pass@host/db \
  --checkpoint-sqlite sqlite://nexusflow.db --checkpoint-postgres postgres://user:pass@host/db
```

`spec_ciphertext` é copiado byte a byte, não re-criptografado — o servidor apontado pro Postgres migrado precisa rodar com o **mesmo** `NEXUS_ENCRYPTION_KEY` de antes, senão os specs migrados falham ao decriptar no primeiro load.

Manifests de referência (Deployment/Service/PVC/HPA/ConfigMap/Secret) em
`packaging/kubernetes/` (`kubectl apply -k packaging/kubernetes/`), stack file
de Docker Swarm em `packaging/swarm/` (`docker stack deploy`) — ver o `README.md`
de cada um. Não são Helm chart nem testados num cluster gerenciado real, são
ponto de partida validado offline.

## 4. Primeiro acesso

1. Abra `http://localhost:8080` — a UI é servida pelo próprio binário (`embed-ui`).
2. Login com o usuário Admin bootstrapado (`NEXUS_ADMIN_USERNAME`/`NEXUS_ADMIN_PASSWORD`).
3. No canvas, arraste um node de source e um de sink da lista de conectores (vem de `GET /connectors`, dinâmica).
4. Preencha a config de cada node no painel lateral — campos de formulário reais (texto, número, enum, listas), gerados a partir do schema que cada conector expõe, não um JSON pra escrever à mão. Nunca fica em plain text depois de salvo — criptografado com `NEXUS_ENCRYPTION_KEY`.
5. Opcional: adicione um node de transform (SQL via DataFusion) entre source e sink, ou um node `dbt` depois do(s) sink(s) pra rodar ELT pós-carga. **Sem transform, o runner só suporta `postgres → postgres`; cross-connector ou outros conectores exigem um nó transform.**
6. Clique **Save** pra persistir o pipeline (cria na primeira vez, atualiza nas seguintes) — sem isso ele só existe nessa aba do navegador e o scheduler (próximo item) não tem o que agendar. Opcional: preencha o campo **schedule** (cron) pra rodar automaticamente, sem precisar clicar Run de novo.
7. Rode manualmente e acompanhe linhas/s, MB/s e logs em tempo real no painel de execução (WebSocket), ou deixe o schedule disparar sozinho.
8. Na aba **Pipelines**: veja tudo que já foi salvo, clique **Edit** pra recarregar um pipeline de volta no canvas (inclusive configs de conector), **Histórico** pra ver todas as execuções (não só a última) com duração calculada, linhas gravadas, erro completo em caso de falha e um botão **Logs** por execução (funciona pra qualquer run, inclusive um disparado pelo scheduler que ninguém acompanhou ao vivo), ou **Delete** pra remover. Na aba **Status**: visão rápida de todos os pipelines com um flag por linha — verde (sucesso), amarelo (em execução), vermelho (falha), cinza (nunca rodou).

### Via API direto

```bash
# login
TOKEN=$(curl -s -X POST http://localhost:8080/auth/login \
  -H 'content-type: application/json' \
  -d '{"username":"admin","password":"troque-isto"}' | jq -r .token)

# catálogo de conectores disponíveis nesse binário
curl -s http://localhost:8080/connectors -H "authorization: Bearer $TOKEN"

# criar um pipeline (schedule é opcional — sem ele só roda via /run manual)
# NOTA: sem nó transform, apenas postgres → postgres é suportado.
# Cross-connector ou outros conectores exigem um nó transform (exemplo abaixo).
curl -s -X POST http://localhost:8080/pipelines \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{
    "pipeline_id": "meu-pipeline",
    "sources": [{"connector": "postgres", "config": {"uri": "postgres://user:pw@host/db", "table": "events", "primary_key": "id"}}],
    "sinks": [{"connector": "postgres", "config": {"uri": "postgres://user:pw@host/db", "table": "events_copy", "primary_key": "id"}}],
    "schedule": "0 */6 * * *"
  }'

# exemplo cross-connector (postgres → sqlite) — exige um nó transform
curl -s -X POST http://localhost:8080/pipelines \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{
    "pipeline_id": "meu-pipeline-sqlite",
    "sources": [{"connector": "postgres", "config": {"uri": "postgres://user:pw@host/db", "table": "events", "primary_key": "id"}}],
    "transform": {"sql": "SELECT * FROM source0"},
    "sinks": [{"connector": "sqlite", "config": {"path": "/tmp/out.db", "table": "events", "primary_key": "id"}}]
  }'

# atualizar (mesmo body, PUT em vez de POST)
curl -s -X PUT http://localhost:8080/pipelines/meu-pipeline \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"pipeline_id": "meu-pipeline", "sources": [...], "sinks": [...], "schedule": "0 */6 * * *"}'

# rodar manualmente (o scheduler acima dispara sozinho, esse endpoint é só pra forçar fora do horário)
# retorna 202 Accepted imediatamente com {"run_id": N} — a execução acontece em background;
# acompanhe o progresso ao vivo no WebSocket /pipelines/{id}/runs/{run_id}/progress ou no histórico abaixo
curl -s -X POST http://localhost:8080/pipelines/meu-pipeline/run \
  -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"pipeline_id": "meu-pipeline"}'

# histórico de execuções (inclui as disparadas pelo scheduler, indistinguíveis de um run manual)
curl -s http://localhost:8080/pipelines/meu-pipeline/runs -H "authorization: Bearer $TOKEN"

# log de execução de um run específico (id vindo da resposta acima ou do POST /run) —
# funciona pra um run já terminado ou disparado pelo scheduler sem ninguém olhando o
# WebSocket ao vivo, já que fica persistido (ARCHITECTURE.md §15)
curl -s http://localhost:8080/pipelines/meu-pipeline/runs/1/logs -H "authorization: Bearer $TOKEN"

# spec completo, configs de conector inclusas — só pra recarregar/editar, exige Write
curl -s http://localhost:8080/pipelines/meu-pipeline/spec -H "authorization: Bearer $TOKEN"

# preview: primeiras linhas de um node source/sink do pipeline, sem rodar o pipeline inteiro
curl -s "http://localhost:8080/pipelines/meu-pipeline/preview?node=source0&limit=20" \
  -H "authorization: Bearer $TOKEN"
```

Papéis RBAC (`Read < Execute < Write < Admin`): criar/editar pipeline (`POST`/`PUT`/`DELETE /pipelines`, e `GET /pipelines/{id}/spec`) exige `Write`; rodar exige `Execute`; listar catálogo/histórico/resumo (`GET /pipelines`) exige `Read`.

## 5. ELT (ou ETL real) com dbt (opcional)

Precisa do build com a feature `dbt` (`cargo build --features embed-ui,dbt`) e do CLI `dbt` (dbt-fusion) no `PATH` do processo em runtime — não é instalado automaticamente. Um pipeline com um node `dbt` roda `dbt run`/`build`/`test` contra o warehouse de destino **depois** que a carga bruta termina (ELT clássico, não transforma os `RecordBatch` do pipeline em si). Resultado (models/tests passados, lineage) aparece no histórico da execução e no painel da UI.

Se `dbt.output` estiver setado no spec (aponta pro model/tabela que o dbt acabou de gerar), o pipeline vira ETL de verdade: depois do `dbt run`/`build`, o backend lê esse resultado de volta e grava em `post_dbt_sinks` — tudo no mesmo `run`, sem precisar montar um segundo pipeline manualmente pra "buscar o que o dbt gerou". Configuração hoje só via API/JSON (sem UI dedicada no Canvas ainda):

```json
{
  "pipeline_id": "etl-com-dbt",
  "sources": [{"connector": "postgres", "config": {"uri": "...", "table": "raw_events", "primary_key": "id"}}],
  "sinks": [{"connector": "postgres", "config": {"uri": "...", "table": "staging_events", "primary_key": "id"}}],
  "dbt": {
    "project_dir": "meu_projeto_dbt",
    "command": "run",
    "output": {"connector": "postgres", "config": {"uri": "...", "table": "transformed_events", "primary_key": "id"}}
  },
  "post_dbt_sinks": [
    {"connector": "postgres", "config": {"uri": "...", "table": "final_events", "primary_key": "id"}}
  ]
}
```

## 6. Observabilidade

- `GET /health` — liveness, sem auth.
- `GET /metrics` — Prometheus, sem auth (segurança via segmentação de rede, não token — scrapers não carregam JWT).
- Logs estruturados em JSON no stdout; setar `NEXUS_OTLP_ENDPOINT` pra exportar traces também.

## Leitura relacionada

| Arquivo | Conteúdo |
|---|---|
| [`USER_GUIDE.md`](./USER_GUIDE.md) | Referência completa: config exata de cada um dos 24 conectores, transform SQL, embeddings, dbt ELT/ETL, preview, agendamento |
| [`ARCHITECTURE.md`](../ARCHITECTURE.md) | Roteador de conectores, streaming/backpressure, checkpointing |
| [`ROADMAP.md`](../ROADMAP.md) | Fases e critério de "pronto" |
| [`CONTRIBUTING.md`](../CONTRIBUTING.md) | Como contribuir |
