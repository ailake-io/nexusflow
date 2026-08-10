# NexusFlow no Docker Swarm

Stack file de referência — validado offline (`docker compose config`), não
testado num swarm multi-node real.

## Pré-requisito: Postgres

Mesmo requisito do Kubernetes (`packaging/kubernetes/README.md`): com
`replicas: 2`, `NEXUS_CHECKPOINT_DB`/`NEXUS_AUTH_DB`/`NEXUS_PIPELINES_DB` têm
que apontar pra um Postgres compartilhado, não o SQLite padrão. Ver
`GETTING_STARTED.md §3` e `ARCHITECTURE.md §14`.

## Segredos: sem `docker secret` nativo aqui

Swarm secrets montam como arquivo em `/run/secrets/<nome>` — o `nexus-server`
só lê config via variável de ambiente (sem convenção `_FILE`), então o
secret store nativo do Swarm não se encaixa direto. Este stack usa
`${VAR}` (substituição padrão do compose) lido do shell/`.env` no momento do
deploy — injete os valores reais aí (do seu CI/CD ou secret manager), nunca
num `.env` commitado.

## Uso

```bash
docker swarm init   # se ainda não for um swarm

export NEXUS_JWT_SECRET=$(openssl rand -hex 32)
export NEXUS_ENCRYPTION_KEY=$(openssl rand -hex 32)
export NEXUS_CHECKPOINT_DB=postgres://user:pass@host:5432/nexusflow
export NEXUS_AUTH_DB=postgres://user:pass@host:5432/nexusflow
export NEXUS_PIPELINES_DB=postgres://user:pass@host:5432/nexusflow

docker stack deploy -c packaging/swarm/docker-stack.yml nexusflow
```

## Limitações conhecidas

Mesma de `packaging/kubernetes/README.md`: escalar pra baixo mata runs de
pipeline em voo naquele container (recuperável via checkpoint, não é perda
de dado). Sem Postgres incluído no stack de propósito — aponte pro seu.
