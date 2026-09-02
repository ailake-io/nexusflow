# Guia de teste — Conectores OSS

Passo a passo pra testar, manualmente, cada um dos 31 conectores OSS contra
infra real (Docker local sempre que existe imagem oficial, arquivo local
quando o conector é embutido). Os 40 conectores enterprise estão no
`docs/CONNECTOR_TESTING_GUIDE.md` do repo privado `nexus-connectors-enterprise`.

Cada conector aqui só lista os campos **mínimos** pra montar um teste — a
lista completa de campos (obrigatórios e opcionais, com descrição) está em
`docs/CONNECTOR_FIELD_REFERENCE.md`, gerado do schema real. Use os dois
juntos: este doc diz *o que subir e clicar*, aquele diz *o que cada campo
significa*.

**Postgres, MySQL e MongoDB (batch e CDC) já têm um guia dedicado, mais
fundo que este** — 100k linhas seedadas, docker-compose pronto, passo a
passo de UI completo — no repo `nexusflow-db-testbeds` (fora deste repo de
propósito, é só infra de teste). Não duplicado aqui; ver a seção 1 pra como
apontar pra ele.

---

## 0. Antes de tudo

### 0.1. Subir o NexusFlow

Se você já tem um container rodando (ex.: `nexus-test/docker-compose.yml`
deste mesmo diretório de trabalho, ou o setup do `nexusflow-db-testbeds`),
reuse-o. Senão, o jeito mais rápido pra um teste isolado, sem metadados
compartilhados com nada:

```bash
docker volume create nexusflow_data
docker run --rm -v nexusflow_data:/data alpine chown -R 1001:1001 /data

docker run -d --name nexusflow-test --network host \
  -e NEXUS_JWT_SECRET="$(openssl rand -hex 32)" \
  -e NEXUS_ENCRYPTION_KEY="$(openssl rand -hex 32)" \
  -e NEXUS_ADMIN_USERNAME=admin \
  -e NEXUS_ADMIN_PASSWORD='troque-esta-senha' \
  -e NEXUS_CHECKPOINT_DB='sqlite:///data/nexusflow.db' \
  -e NEXUS_AUTH_DB='sqlite:///data/nexusflow-auth.db' \
  -e NEXUS_PIPELINES_DB='sqlite:///data/nexusflow-pipelines.db' \
  -v nexusflow_data:/data \
  -v /tmp/nexusflow-out:/data/out \
  thiagolange/nexusflow:latest
```

`--network host` é o que deixa o container alcançar `localhost:<porta>` de
qualquer outro container/serviço subido separadamente no host (Kafka,
Qdrant, etc.) — sem isso, `localhost` de dentro do container não é o
`localhost` da sua máquina. Abra `http://localhost:8080`, entre com
`admin` / a senha que você definiu.

### 0.2. Como montar e rodar um pipeline (genérico, vale pra qualquer conector abaixo)

1. **Canvas** → arraste o conector desejado pro canvas (fonte por padrão).
2. Clique no node, preencha os campos do formulário à direita (rótulo =
   nome do campo; `*` = obrigatório). Pra variante CDC de um conector que
   tem as duas (postgres/mysql/mongodb), use o seletor **Modo** no próprio
   node.
3. Arraste um **segundo** conector (o destino), mude **Papel** pra
   `destino`, preencha os campos dele — se você só quer testar leitura
   (source), pule pro atalho da seção 0.3 em vez disso.
4. Clique **+ Transform** (obrigatório em praticamente todo teste — ver
   aviso abaixo) e escreva `SELECT * FROM source0` (ou colunas
   específicas).
5. Conecte fonte → Transform → destino.
6. Preencha **pipeline_id**, clique **Salvar**, depois **Executar**.
7. Acompanhe em **Status** (WebSocket, % ao vivo) ou **Pipelines** →
   histórico de execuções.

> **Por que o Transform é quase sempre obrigatório:** sem ele, o
> NexusFlow tenta o caminho "linear" direto (só Postgres→Postgres batch
> puro é suportado sem Transform). Qualquer outra combinação falha em
> runtime com `unsupported connector: the partitioned (no-transform) path
> only supports 'postgres'`. `SELECT * FROM source0` força o caminho
> genérico, que suporta qualquer conector — e é obrigatório de verdade
> (não só estilo) pra CDC, porque preserva a coluna `__opcode`.

Cenário **CDC**: o run fica `running` indefinidamente (é streaming) — não é
erro. Gere mudanças na fonte enquanto o pipeline roda e confira o
resultado no destino.

### 0.3. Atalho pra só testar uma fonte (sem sink/Transform)

Pra confirmar que um **source** conecta e lê direito, sem montar pipeline
completo: arraste só o conector, preencha os campos, e use o botão
**Visualizar** (preview) no node — mostra as primeiras linhas reais.
Equivale a `POST /connectors/preview` — não precisa Salvar nem Executar.
Não serve pra sinks (nada a visualizar num destino vazio) nem pra CDC
(preview não faz streaming contínuo).

### 0.4. Dados de exemplo prontos

`nexus-test/testdata/` (fora deste repo) já tem `nf_small.csv` (3 linhas)
e `events_100k.csv`/`.txt`/`.parquet` (100k linhas, colunas
`id`/`nome`/`valor`/`data`) — monte o container com
`-v .../testdata:/data/testdata:ro` pra usar como fonte sem precisar gerar
nada.

---

## 1. Postgres, MySQL, MongoDB (batch + CDC)

Guia dedicado e completo (100k linhas seedadas, docker-compose com/sem
CDC, passo a passo de UI campo a campo) em `nexusflow-db-testbeds/README.md`
— cobre `postgres`, `postgres-cdc`, `mysql`, `mysql-cdc`, `mongodb`,
`mongodb-cdc`. Não repetido aqui.

---

## 2. Bancos e engines SQL — Docker local

| Conector | Subir | Config mínima | Verificar |
|---|---|---|---|
| `sqlite` | Nada — embutido, sem container. `file_path: /data/out/teste.db` (ou `:memory:`). | `file_path=/data/out/teste.db`, `table=events`, `primary_key=id` | Sink: `sqlite3 /caminho/teste.db "SELECT count(*) FROM events;"` no host (se montado como volume). |
| `duckdb` | Nada — embutido. `path: /data/out/teste.duckdb` (ou `:memory:`). | `path=/data/out/teste.duckdb`, `table=events`, `primary_key=id` | Sink: `duckdb /caminho/teste.duckdb "SELECT count(*) FROM events;"` no host. |
| `clickhouse` | `docker run -d --name ch -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server` | `host=localhost`, `port=8123`, `database=default`, `table=events` (crie a tabela antes: `docker exec ch clickhouse-client -q "CREATE TABLE default.events (id Int64, name String) ENGINE=MergeTree ORDER BY id"`) | `docker exec ch clickhouse-client -q "SELECT count() FROM default.events"` |
| `pgvector` | `docker run -d --name pgv -p 5432:5432 -e POSTGRES_PASSWORD=nexusflow pgvector/pgvector:pg16` — depois `CREATE EXTENSION vector; CREATE TABLE events (id bigint PRIMARY KEY, embedding vector(384));` | `host=localhost`, `username=postgres`, `password=nexusflow`, `database=postgres`, `table=events`, `primary_key=id`, `embedding_column=embedding`, `dimension=384` | `psql` → `SELECT count(*) FROM events;` |
| `odbc` | Precisa de um driver ODBC já registrado no host/container rodando o NexusFlow (unixODBC) — não é algo que sobe num `docker run` isolado. Mais simples: registrar o driver Postgres Unicode e apontar pro mesmo container do teste de `pgvector`/Postgres acima. | `driver={PostgreSQL Unicode}`, `server=localhost`, `username=postgres`, `password=nexusflow`, `table=events`, `primary_key=id` | Preview do source deve retornar linhas. |

## 3. Bancos vetoriais — Docker local

| Conector | Subir | Config mínima | Verificar |
|---|---|---|---|
| `qdrant` | `docker run -d --name qdrant -p 6333:6333 -p 6334:6334 qdrant/qdrant` | `host=localhost`, `port=6334`, `collection_name=events`, `primary_key=id`, `embedding_column=embedding`, `dimension=384` — crie a collection antes via API do Qdrant (`PUT /collections/events` com `vectors.size=384`) | `curl localhost:6333/collections/events` → `points_count` bate com as linhas gravadas. |
| `chromadb` | `docker run -d --name chroma -p 8000:8000 chromadb/chroma` — crie a collection antes (`POST /api/v2/.../collections`, nome `events`) | `host=localhost`, `port=8000`, `collection=events`, `primary_key=id`, `embedding_column=embedding`, `dimension=384` | `curl localhost:8000/api/v2/.../collections/events` |
| `lancedb` | Nada — embutido, local. `path: /data/out/lancedb` | `path=/data/out/lancedb`, `table_name=events`, `primary_key=id`, `embedding_column=embedding`, `dimension=384` | Reabra a tabela com um script Python `lancedb.connect(path).open_table("events").count_rows()`, ou confira via novo pipeline `lancedb` fonte → `csv` destino. |
| `milvus` | **3 containers** (etcd + minio + milvus — Milvus não roda sozinho num único container nesta versão): <br>`docker network create milvus-test` <br>`docker run -d --name etcd --network milvus-test quay.io/coreos/etcd:v3.5.18 etcd -advertise-client-urls=http://etcd:2379 -listen-client-urls=http://0.0.0.0:2379 --data-dir /etcd` <br>`docker run -d --name minio --network milvus-test -e MINIO_ACCESS_KEY=minioadmin -e MINIO_SECRET_KEY=minioadmin minio/minio server /minio_data` <br>`docker run -d --name milvus --network milvus-test -p 19530:19530 -e ETCD_ENDPOINTS=etcd:2379 -e MINIO_ADDRESS=minio:9000 milvusdb/milvus:latest milvus run standalone` | `host=localhost`, `port=19530`, `collection_name=events`, `primary_key=id` (Int64), `embedding_column=embedding`, `dimension=384` — crie a collection+índice antes via SDK/cliente Milvus | Query via `pymilvus` ou novo pipeline `milvus` fonte → `csv` destino. |
| `pinecone` | Sem opção local — API cloud only. Crie um index de teste grátis (tier free) em https://app.pinecone.io, dimensão 384. | `api_key=<sua key>`, `index_name=<nome do index>`, `primary_key=id`, `embedding_column=embedding`, `dimension=384` | Console do Pinecone mostra `vector count` do index. |

## 4. Filas e streaming — Docker local

| Conector | Subir | Config mínima | Verificar |
|---|---|---|---|
| `kafka` | `docker run -d --name kafka -p 9092:9092 apache/kafka:latest` (imagem oficial Apache, KRaft, sem Zookeeper) | `bootstrap_servers=localhost:9092`, `topic=events`, `group_id=nexus-test`, `fields`: preencha ou deixe vazio pra inferir do payload JSON | Publique uma mensagem JSON no tópico (`kafka-console-producer`) antes de rodar; confira no destino. |
| `mqtt` | `docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto` (pode precisar de `mosquitto.conf` com `listener 1883` + `allow_anonymous true` montado, a imagem base não aceita anônimo por padrão) | `broker_url=mqtt://localhost:1883`, `client_id=nexus-test`, `topic_filter=sensors/#`, `fields` | `mosquitto_pub -h localhost -t sensors/temp -m '{"id":1,"value":22.5}'` antes de rodar. |
| `nats` | `docker run -d --name nats -p 4222:4222 nats:latest` | `server_url=nats://localhost:4222`, `subject=events`, `fields` | `nats pub events '{"id":1}'` (CLI `nats`) antes de rodar. |
| `rabbitmq` | `docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management` | `url=amqp://guest:guest@localhost:5672/%2f`, `queue=events`, `fields` | UI de management em `localhost:15672` mostra mensagens na fila antes/depois. |
| `redis` | `docker run -d --name redis -p 6379:6379 redis:7` | `url=redis://localhost:6379`, `stream_key=events`, `fields` | `redis-cli XADD events '*' id 1 value teste` antes; `redis-cli XLEN events` depois. |

## 5. Data lake / arquivo local — sem container

Todos embutidos (sem servidor externo) — só precisam de um diretório
gravável.

| Conector | Config mínima | Verificar |
|---|---|---|
| `csv` | `path=/data/out/teste.csv`, `has_header=true`, `primary_key=id` (sink) — use `nexus-test/testdata/nf_small.csv` como fonte pronta | `cat`/`wc -l` no arquivo de saída. |
| `parquet` | `path=/data/out/teste.parquet`, `primary_key=id` | `parquet-tools cat` ou reabra via novo pipeline `parquet` fonte → `csv` destino. |
| `deltalake` | `path=/data/warehouse`, `table_name=events`, `primary_key=id` | Reler via novo pipeline `deltalake` fonte → `csv` destino, ou `deltalake` Python (`DeltaTable(path).to_pandas()`). |
| `iceberg` | `catalog_path=/data/warehouse/catalog.db`, `warehouse_path=/data/warehouse`, `namespace_name=default`, `table_name=events` | Mesma ideia — reler via pipeline reverso. |
| `ailake` | `warehouse=/data/warehouse/ailake`, `namespace=default`, `table=events`, `primary_key=id`, `embedding_column=embedding`, `dimension=384` | Reler via pipeline reverso. |

CDC dessas 3 (`deltalake-cdc`/`iceberg-cdc`/`ailake-cdc`): mesma
infraestrutura acima; a fonte lê a partir de uma versão/snapshot
(`starting_version`/`starting_snapshot_id`, vazio = desde o início).
Gere uma segunda escrita na tabela (rode o pipeline batch de novo com
dado diferente) e confirme que o CDC captura só o delta.

## 6. APIs genéricas — sem container dedicado

| Conector | Como testar | Config mínima | Verificar |
|---|---|---|---|
| `rest` | Aponte pra qualquer API JSON pública de teste, ex. `https://jsonplaceholder.typicode.com` | `base_url=https://jsonplaceholder.typicode.com`, `path=/posts`, `method=GET` | Preview deve retornar as linhas do array JSON. |
| `webhook` | Suba um receptor local só pra ver o payload chegar: `docker run -d --name httpbin -p 8081:80 kennethreitz/httpbin` (ou use https://webhook.site pra uma URL temporária hospedada) | `base_url=http://localhost:8081`, `path=/post`, `method=POST` | `docker logs httpbin` mostra o corpo recebido, ou o histórico em webhook.site. |

---

## 7. Coisas que quebram (checklist antes de reportar falha)

- **Timeout de conexão** (`ADBC_DRIVER_POSTGRESQL_PATH not set` ou parecido em conectores nativos): drivers ADBC reais precisam ser buildados/apontados via env var — não é bug do conector, é setup do binário.
- Sink sem tabela/collection/index pré-criada: a maioria dos conectores vetoriais/warehouse **não cria índice/collection sozinho** (só a tabela em `postgres`/`csv`/`sqlite` é criada automaticamente) — crie antes de rodar.
- `unsupported connector: the partitioned (no-transform) path only supports 'postgres'`: faltou o node **+ Transform** — ver seção 0.2.
- Pipeline CDC sem `SELECT *` no Transform: perde a coluna `__opcode`, todo INSERT/UPDATE/DELETE vira upsert cego.
- Sink `csv` com "Permission denied": o volume `/data/out` no host precisa pertencer ao uid 1001 (o container roda como usuário não-root) — `docker run --rm -v /caminho:/fix alpine chown -R 1001:1001 /fix`.
