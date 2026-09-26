# 💰 Conectores Enterprise (candidatos) — NexusFlow

Este doc detalha os candidatos a conector pago citados em `LICENSING.md §2`. Vivem em repo privado separado (`nexus-connectors-enterprise`, binário próprio — ver `LICENSING.md §2`), atrás de license key — nunca entram em `crates/nexus-connectors/` (OSS). Ver `ARCHITECTURE.md` e `ROADMAP.md` (Fase 12).

**Reescrito na Fase 32 (2026-09-25)**: mudança de modelo — toda
**infraestrutura de dado** (SQL/DW, vetorial/busca, streaming, arquivo/
storage) virou OSS, migrada do repo privado pra `crates/nexus-connectors/`
público. Só fica enterprise **SaaS de negócio** (ads/marketing, CRM/ERP/
suporte/RH, pagamento). Isso esvaziou quase inteiramente as seções que
existiam aqui antes (Data Warehouses, Arquivos, Vetorial/busca, Streaming,
CDC avançado) — o repo privado caiu de 38 crates de conector pra 16.

**Status real (auditado via `Cargo.toml` do repo privado, 2026-09-25):**
16 crates de conector restam no repo privado. Restam sem crate por falta
de demanda confirmada: SAP BAPI/IDoc (bloqueio legal — SDK NetWeaver
proprietário, sem licença SAP não redistribuível, ver `ROADMAP.md` item
17), IBM Db2 (excluído por decisão explícita, não falta de demanda), Db2
CDC (mesma exclusão) e OPC-UA (ver seção 3 abaixo).

## 1. SaaS / CRM / ERP / suporte / RH

| Conector | Por quê é pago |
|---|---|
| **Salesforce** ✅ implementado | O conector mais pedido em qualquer ferramenta de integração de dados — prioridade alta |
| **SAP** (BAPI/IDoc/S/4HANA) | ERP mais comum em grandes empresas, integração cara e complexa — alto ticket. Sem crate: SDK NetWeaver da SAP é proprietário, exige licença comercial direta, não redistribuível — bloqueio legal, não técnico (ver `ROADMAP.md` item 17) |
| **HubSpot** ✅ implementado (CRM v3, Batch Upsert API) | CRM popular em empresas médias, bom volume |
| **Workday** ✅ implementado (RaaS, **source-only** — write-path real é SOAP/Integration Services, fora de escopo) | RH/financeiro enterprise, ticket alto |
| **NetSuite** ✅ implementado (SuiteQL + REST Record API) | ERP de média empresa, demanda constante |
| **Dynamics 365** ✅ implementado (Dataverse Web API, OData v4) | Ecossistema Microsoft, correlaciona com clientes que já usam Azure |
| **ServiceNow** ✅ implementado (Table API) | ITSM enterprise, dados de operação |
| **Zendesk** ✅ implementado (Support API v2) | Suporte/CS, volume alto, ticket menor |
| **Shopify** ✅ implementado | E-commerce, alto volume |

## 2. Marketing / Ads / Analytics + pagamento (alto volume, padrão Fivetran/Airbyte)

> **2026-09-10**: os 5 conectores de ads (Google/Meta/LinkedIn/TikTok/X)
> têm crate implementado no repo privado, mas foram **retirados de
> `connectors-all`/`connectors-all-no-embeddings`** (`bin/Cargo.toml`) —
> nenhum deles foi validado contra uma conta de ads real ainda (Google
> Ads chegou mais perto: OAuth configurado, travou no penúltimo passo
> por um atraso de segurança de 6 dias na conta Google usada pro teste,
> não é bug do conector; os outros 4 nem começaram). Não entram em
> nenhum binário publicado (Docker Hub, `.msi`, tarball macOS,
> `.deb`/`.rpm`/AppImage) até passar num teste real, um de cada vez —
> ver `docs/PENDING_REAL_ACCOUNT_VALIDATION.md`. "Implementado" abaixo
> significa "crate existe e compila", não "incluído no build padrão".

| Conector | Por quê é pago |
|---|---|
| **Google Analytics (GA4)** ✅ implementado | Conector mais usado em stacks de marketing analytics |
| **Google Ads** ✅ implementado, ⛔ excluído do build padrão até teste real | Par natural do GA4 |
| **Meta Ads** (Facebook/Instagram) ✅ implementado, ⛔ excluído do build padrão até teste real | Mesma categoria, alto volume de contas pequenas/médias |
| **LinkedIn Ads** ✅ implementado, ⛔ excluído do build padrão até teste real (também precisa de aprovação MDP discricionária do LinkedIn) | Nicho B2B, ticket médio |
| **X Ads** ✅ implementado, ⛔ excluído do build padrão até teste real | Mesma categoria de marketing analytics, volume menor que Meta/Google mas cliente já paga por ferramenta de ads que cobre a plataforma |
| **TikTok Ads** ✅ implementado, ⛔ excluído do build padrão até teste real | Mesma categoria de marketing analytics, alto volume |
| **YouTube Analytics** ✅ implementado | Mesma categoria, complementa GA4/Google Ads no ecossistema Google |
| **Stripe** ✅ implementado (read-only por design — nunca ganha sink, transação financeira real fica fora de escopo) | Dados financeiros/billing, alta demanda em SaaS — único conector de pagamento, não é "infraestrutura de dado", é API de negócio |

## 3. Protocolos industriais

Categoria à parte — comprador claro (chão de fábrica/manufatura, mesmo
perfil de Oracle/SAP antes de migrarem pro OSS), protocolo bem mais
complexo que os message queues já OSS (modelo de informação tipado, não
é só pub/sub).

| Conector | Por quê é pago |
|---|---|
| **OPC-UA** | Padrão industrial/SCADA (chão de fábrica, automação predial) — driver Rust real confirmado (`opcua`/`opcua-rs`, MPL-2.0, mantido), não implementado ainda. Cliente disposto a pagar por conectividade industrial certificada. |

## O que saiu daqui na Fase 32 (agora OSS, `crates/nexus-connectors/`)

Só pra referência histórica — não são mais candidatos a pago:

- **SQL/DW**: BigQuery, Databricks, SAP HANA, MSSQL (+ `mssql-cdc`), Oracle (+ `oracle-cdc`), Redshift, Snowflake, Starburst, Teradata, Vertica
- **Vetorial/busca**: Elasticsearch, Weaviate, Vertex AI Vector Search, Azure AI Search
- **Streaming**: Kinesis, Pulsar
- **Arquivo/storage**: Excel, PDF OCR, Google Drive, Google Sheets, Dropbox, SharePoint

`ClickHouse` já tinha saído numa rodada anterior, mesmo racional (RBAC e
cluster mode são recursos OSS do próprio banco, não existe feature
"avançada" genuína pra reservar como paga).

Casos que continuam **sem crate próprio**, cobertos pelo `kafka` OSS +
`docs/KAFKA_MANAGED_SERVICES.md` (protocolo compatível, sem lock-in real):
Confluent Cloud, Azure Event Hubs. `Pinecone managed`/`Milvus cluster
mode` também nunca viraram SKU separado — os básicos já são OSS
(`LICENSING.md §1`) e cobrem o caso hoje.

## Priorização sugerida

Ordenado por (demanda de mercado × disposição a pagar), não por
dificuldade técnica — dos 16 que restam:

1. **Salesforce** — o conector mais pedido em ferramentas comerciais concorrentes, ainda não implementado.
2. **HubSpot, Zendesk, Shopify, Dynamics 365, NetSuite, ServiceNow, Workday** — CRM/ERP/suporte/RH, todos já implementados.
3. **Marketing/Ads** (GA4, Google Ads, Meta Ads, TikTok Ads, X Ads, LinkedIn Ads, YouTube Analytics, Stripe) — alto volume, ticket médio menor, bom motor de PLG — todos já implementados, 5 ainda travados até validação real de conta.
4. **SAP (BAPI/IDoc/S/4HANA)** — legado enterprise, ticket alto, mas bloqueio legal (sem SDK redistribuível).
5. **OPC-UA** — nicho industrial, ticket alto, baixo volume, driver existe mas não implementado ainda.

Decisão de "o que construir primeiro" na Fase 12 deve seguir demanda real
confirmada (mesmo racional já usado pro CDC nativo condicional em
`ROADMAP.md`), não essa lista sozinha — ela é o inventário de
candidatos, não um compromisso de roadmap.

**Status real desta priorização:** dos 16 conectores que restam, todos
já têm crate implementado (`Cargo.toml`/`bin/Cargo.toml` do repo
privado), exceto **OPC-UA** (driver Rust existe, ainda não construído).
SAP (BAPI/IDoc), IBM Db2 e Db2 CDC são exclusões deliberadas (bloqueio
legal ou decisão explícita), não trabalho pendente.
