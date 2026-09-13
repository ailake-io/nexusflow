# Conectando `kafka` a serviços gerenciados (Confluent Cloud, Azure Event Hubs)

`nexus-connector-kafka` (`crates/nexus-connectors/nexus-connector-kafka`)
já é wire-compatible com Kafka de verdade — **não precisa de conector
novo** pra falar com Confluent Cloud ou Azure Event Hubs, só a
configuração de conexão certa. Confirmado lendo o `config.rs` real do
conector (`KafkaSecurityProtocol`/`KafkaSaslMechanism`), não suposição.

Campos relevantes do config do `kafka` (`crates/nexus-connectors/
nexus-connector-kafka/src/config.rs`):
- `bootstrap_servers` — string `host:port` (ou lista separada por
  vírgula).
- `security_protocol` — `plaintext` | `sasl_plaintext` | `ssl` | `sasl_ssl`
  (serde `rename_all = "snake_case"` na leitura do JSON — não confundir com
  os literais `PLAINTEXT`/`SASL_SSL` que `as_str()` manda pro librdkafka
  internamente, esses não são o que a API aceita como entrada).
- `sasl_mechanism` — `plain` | `scram_sha256` | `scram_sha512` (mesmo
  motivo: snake_case, sem hífen).
- `sasl_username` / `sasl_password`.
- `topic`, `group_id`, `fields` — obrigatórios, mesmos de sempre.

`kafka` agora tem sink além de source (mesma config, feature `producer` de `nexus-connector-kafka`) — os exemplos abaixo valem igual para **publicar** num tópico do Confluent Cloud/Event Hubs, só que como sink em vez de source (`group_id` é ignorado pelo producer, mas continua obrigatório no schema do config).

## Confluent Cloud

Endpoint real: `pkc-xxxxx.<região>.aws.confluent.cloud:9092` (copiar
do painel do cluster). Auth via API Key/Secret do cluster.

```json
{
  "bootstrap_servers": "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
  "security_protocol": "sasl_ssl",
  "sasl_mechanism": "plain",
  "sasl_username": "<API_KEY>",
  "sasl_password": "<API_SECRET>",
  "topic": "events",
  "group_id": "nexus-consumer",
  "fields": [{"name": "id", "data_type": "int64", "nullable": false}]
}
```

Schema Registry (Avro/Protobuf) é add-on separado do Confluent Cloud —
só relevante se um conector futuro precisar de deserialização
schema-aware; o `fields` explícito do `kafka` já cobre o caso comum
(payload JSON).

## Azure Event Hubs

Event Hubs expõe um endpoint Kafka-compatível — mesmo protocolo, sem
mudar código nenhum. Confirmado: esse endpoint só aceita a connection
string completa como senha, **não aceita SAS token separado**, e o
usuário SASL é literalmente a string `"$ConnectionString"` (não um
usuário real).

```json
{
  "bootstrap_servers": "<namespace>.servicebus.windows.net:9093",
  "security_protocol": "sasl_ssl",
  "sasl_mechanism": "plain",
  "sasl_username": "$ConnectionString",
  "sasl_password": "Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...",
  "topic": "<nome do Event Hub>",
  "group_id": "nexus-consumer",
  "fields": [{"name": "id", "data_type": "int64", "nullable": false}]
}
```

O nome do "tópico" Kafka é o próprio nome do Event Hub dentro do
namespace.

## Fontes

- [Confluent — SASL/PLAIN](https://docs.confluent.io/platform/current/security/authentication/sasl/plain/overview.html)
- [Confluent Cloud — usando ferramentas Kafka](https://docs.confluent.io/kafka/operations-tools/use-kafka-tools-ccloud.html)
- [Azure Event Hubs — Kafka overview](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview)
