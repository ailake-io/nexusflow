# 📜 Modelo de Licenciamento — NexusFlow

NexusFlow é **100% open source**, sob **Apache License 2.0** (ver `LICENSE`).
Não existe edição enterprise, conector pago, license key nem capability
gateada: todo o código vive neste repositório e todo binário publicado
(tarball/`.deb`/`.rpm`/AppImage, `.msi`, Homebrew, imagem Docker) é
compilado com `--features embed-ui,connectors-all` — tudo incluso.

## 1. O que está incluso

- `nexus-core` — traits, modelos Arrow, DAG parser, registry de conectores
- `nexus-server` — API Axum, Auth/RBAC, Scheduler, WebSockets, LLMOps
  (node `llm`, RAG, avaliação, versionamento git + mirror GitHub,
  `GET /lineage/generation/{id}`, RAG reativo CDC+embedding), agente com
  tool-calling, Canvas de Infra
- `nexus-ai` — pipeline de embeddings (chunking, ONNX/ort, API externa)
- `nexus-infra-terraform` — catálogo de módulos Terraform + codegen do
  Canvas de Infra
- `crates/nexus-connectors/` — todos os conectores: SQL/DW, CDC nativo,
  vetoriais/busca, streaming, arquivo/storage e SaaS de negócio
  (Salesforce, HubSpot, Zendesk, ServiceNow, Dynamics 365, NetSuite,
  Workday, Shopify, Stripe, GA4, Google/Meta/LinkedIn/TikTok/X Ads,
  YouTube Analytics)
- Frontend (React Flow canvas)

Por que Apache-2.0 e não MIT: cláusula de patente protege contribuidores e
usuários corporativos — mesmo racional do Arrow, DataFusion e Tokio (stack
que o NexusFlow já depende).

## 2. Histórico

Até a Fase 35 (2026-10-28) o NexusFlow seguia modelo **open-core**:
conectores de SaaS de negócio e o Canvas de Infra viviam num repo privado
(`nexus-connectors-enterprise`) e 3 capabilities de LLMOps eram liberadas
por license key JWT (Ed25519) vendida via Stripe (`nexus-licensing`). A
Fase 32 (2026-09-25) já tinha aberto toda a infraestrutura de dado; a
Fase 35 abriu o resto e removeu o mecanismo de licença inteiro
(`POST/GET /license`, `check_connector_license`, aba Store). Ver
`ROADMAP.md` Fase 35.

## 3. Contribuições

Contribuições externas ficam sob Apache-2.0 automaticamente (seção 5 da
licença cobre isso — nenhum CLA extra necessário). Reavaliar se necessário
CLA formal quando houver contribuições corporativas significativas.
