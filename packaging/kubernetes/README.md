# NexusFlow no Kubernetes

Manifests de referência — não um Helm chart, não testado num cluster gerenciado real
(EKS/GKE/AKS), só validado offline (`kubeconform`) e por revisão manual. Ajuste
recursos/replicas/imagem pro seu ambiente antes de rodar em produção.

## Pré-requisito: Postgres

Com mais de 1 réplica, `NEXUS_CHECKPOINT_DB`/`NEXUS_AUTH_DB`/`NEXUS_PIPELINES_DB`
**precisam** apontar pra um Postgres compartilhado — SQLite (o padrão) não pode ser
compartilhado com segurança entre pods. Ver `GETTING_STARTED.md §3` e
`ARCHITECTURE.md §14` (leader election via `pg_try_advisory_lock`, só uma réplica
dispara cada pipeline agendado por tick).

Este diretório não sobe um Postgres — aponte pro seu próprio (gerenciado ou
self-hosted). O `migrate-metadata` (ver `GETTING_STARTED.md §3`) migra dados
existentes de SQLite se for o caso.

## Uso

1. Gere os segredos reais em vez de editar `secret.yaml` no lugar:
   ```bash
   kubectl create secret generic nexusflow-secrets \
     --from-literal=NEXUS_JWT_SECRET="$(openssl rand -hex 32)" \
     --from-literal=NEXUS_ENCRYPTION_KEY="$(openssl rand -hex 32)" \
     --from-literal=NEXUS_ADMIN_PASSWORD="..." \
     --from-literal=NEXUS_CHECKPOINT_DB="postgres://user:pass@host:5432/nexusflow" \
     --from-literal=NEXUS_AUTH_DB="postgres://user:pass@host:5432/nexusflow" \
     --from-literal=NEXUS_PIPELINES_DB="postgres://user:pass@host:5432/nexusflow"
   ```
   (Ou use Sealed Secrets / External Secrets Operator / Vault — o que seu cluster já usa.)
2. Ajuste `configmap.yaml` (usuário admin, timeouts, SMTP) e a imagem em
   `deployment.yaml` (`ghcr.io/ailake-io/nexusflow:<tag>` — não use `:latest` em
   produção).
3. Aplique:
   ```bash
   kubectl apply -k packaging/kubernetes/
   ```
   Sem PVC/HPA: remova `pvc.yaml`/`hpa.yaml` de `kustomization.yaml`'s `resources`
   antes (o cache de embeddings cai pra `emptyDir`, que já sobrevive a restart no
   mesmo node — só não sobrevive a reagendamento pra outro node).
4. `kubectl port-forward svc/nexusflow 8080:8080` ou exponha via seu Ingress/
   LoadBalancer de preferência — nenhum dos dois está incluído aqui de propósito
   (TLS/hostname são específicos do seu ambiente).

## Limitações conhecidas

- **HPA e runs em andamento**: escalar pra baixo mata runs de pipeline em voo
  daquele pod (`shutdown_signal` faz graceful shutdown do HTTP, mas não espera
  supervisors de run já disparados) — o run é marcado `failed` no próximo boot e
  retoma do último checkpoint por partição, não é perda de dado, mas não é
  "limpo". Ver comentário em `hpa.yaml`.
- **Sem Ingress/TLS incluído** — de propósito, é específico do seu cluster.
- **Sem manifesto de Postgres** — de propósito, use o seu (gerenciado é o
  recomendado; um Postgres rodando dentro do mesmo cluster sem replicação própria
  vira um single point of failure pior que o SQLite que ele substitui).
