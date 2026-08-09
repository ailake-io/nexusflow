# CDC reference environment (Marco 4)

Ambiente de exemplo/teste pra validar o modo `Debezium` do
`nexus-connector-kafka` (ver `ARCHITECTURE.md §7`, `IMPLEMENTATION_PLAN.md`
Marco 4). **Não é infra de produção** — sem persistência, sem TLS, sem auth.

**Caminho alternativo, não mais o único**: desde a Fase 18 (`ROADMAP.md`),
CDC nativo (`postgres-cdc`/`mongodb-cdc`/`mysql-cdc`) não depende de
Kafka/Debezium/Zookeeper — recomendado por padrão, mais leve. Esse stack
Debezium+Kafka continua útil pra quem já opera essa infra ou quer centralizar
CDC de vários bancos por um único broker.

## Subir o stack

```bash
docker compose -f docs/cdc-reference/docker-compose.yml up -d
```

Sobe: Zookeeper, Kafka, Postgres (`wal_level=logical` já configurado pela
imagem `debezium/postgres`) e Kafka Connect com o plugin Debezium Postgres
embutido.

Kafka expõe dois listeners: `INTERNAL` (`kafka:9092`, usado pelo Connect
dentro da rede do compose) e `EXTERNAL` (`localhost:9093`, publicado pra
host). Um cliente rodando no host (ex.: `nexus-connector-kafka` local) usa
sempre a porta `9093` — a `9092` só resolve dentro da rede do compose.

## Registrar o conector Postgres no Debezium

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @docs/cdc-reference/register-postgres-connector.json \
  http://localhost:8083/connectors
```

Cada tabela `public.<tabela>` do banco `nexus` passa a publicar eventos no
tópico Kafka `nexus.public.<tabela>`.

## Gerar eventos

```bash
psql -h localhost -U nexus -d nexus -c "create table public.orders (id int primary key, status text);"
# REPLICA IDENTITY DEFAULT only carries primary-key columns in the old row
# image — UPDATE/DELETE's "before" would be missing every other column.
# FULL captures the whole row, which is what the __opcode/before contract
# below assumes.
psql -h localhost -U nexus -d nexus -c "alter table public.orders replica identity full;"
psql -h localhost -U nexus -d nexus -c "insert into public.orders values (1, 'pending');"
psql -h localhost -U nexus -d nexus -c "update public.orders set status = 'paid' where id = 1;"
psql -h localhost -U nexus -d nexus -c "delete from public.orders where id = 1;"
```

## Consumir com o nexus-connector-kafka

Config do node Kafka com `envelope: debezium`:

```json
{
  "bootstrap_servers": "localhost:9093",
  "topic": "nexus.public.orders",
  "group_id": "nexus-pipeline-1",
  "envelope": "debezium",
  "fields": [
    { "name": "id", "data_type": "int64", "nullable": false },
    { "name": "status", "data_type": "utf8", "nullable": true }
  ]
}
```

O `RecordBatch` resultante ganha uma coluna extra `__opcode` (`"I"`/`"U"`/`"D"`),
derivada do campo `payload.op` do envelope Debezium (`c`/`r` -> Insert,
`u` -> Update, `d` -> Delete) — nunca como side-channel separado, por
`ARCHITECTURE.md §5`. Pra `op = d`, os valores de campo vêm de `payload.before`
(o `after` é `null`); pros demais, de `payload.after`.

`tombstones.on.delete: false` no connector evita a mensagem de tombstone
(`value = null`) que o Kafka Connect emitiria após cada delete — o
`nexus-connector-kafka` já ignora mensagens sem payload, mas isso pouparia um
poll inútil por delete.

## Resume por partição

Offset de consumo é rastreado por partição Kafka (`nexus-server`'s
`checkpoint_store`, já genérico por `(pipeline_id, partition_id)` desde
Marco 1). Uma falha no meio do pipeline retoma cada partição do último
offset commitado, não do início do tópico.
