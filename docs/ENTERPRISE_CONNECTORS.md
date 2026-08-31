# 💰 Conectores Enterprise (candidatos) — NexusFlow

Este doc detalha os candidatos a conector pago citados em `LICENSING.md §2`. Vivem em repo privado separado (`nexus-connectors-enterprise`, binário próprio — ver `LICENSING.md §2`), atrás de license key — nunca entram em `crates/nexus-connectors/` (OSS). Ver `ARCHITECTURE.md` e `ROADMAP.md` (Fase 12).

**Status real (auditado):** 24 dos candidatos abaixo já foram construídos (marcados **✅ implementado**) — repo privado tem hoje 24 crates de conector / 51 entradas no catálogo (25 OSS + 26 nomes enterprise; `opensearch`/`synapse` são modos alternativos dos crates `elasticsearch`/`mssql`, não crates próprios). O resto segue como candidato/roadmap, não construído por falta de demanda confirmada — este doc continua servindo de inventário de candidatos, agora com o que já saiu do papel marcado.

Ponto de partida do usuário: Excel, Oracle, Snowflake, ClickHouse, BigQuery, Redshift. Abaixo, esses mais outros candidatos organizados por categoria, com a lógica de mercado por trás de cada um (o mesmo racional que Fivetran/Airbyte/Matillion usam pra decidir o que cobra).

## 1. Data Warehouses / bancos analíticos enterprise

| Conector | Por quê é pago |
|---|---|
| **Snowflake** ✅ implementado | Maior demanda de mercado em ferramentas ELT — praticamente todo concorrente cobra por esse conector |
| **BigQuery** ✅ implementado | Mesma categoria do Snowflake, par indissociável em RFPs enterprise |
| **Redshift** ✅ implementado | Terceiro da tríade "cloud DW" — quem pede um, geralmente pede os três |
| **Databricks** (SQL Warehouse / Unity Catalog via Flight SQL) | Ligado ao lakehouse — encaixa direto na proposta "AI Lakehouse Builder" do NexusFlow |
| **Oracle** ✅ implementado (batch + `oracle-cdc` via LogMiner) | Legado enterprise, ticket médio alto, cliente já paga licença Oracle cara — tolerância a pagar por conector é maior |
| **SAP HANA** ✅ implementado (SQL via ODBC; BAPI/IDoc/RFC fora de escopo, ver README do repo privado) | Mesma lógica do Oracle — instalado em empresas grandes com orçamento de integração |
| **Microsoft SQL Server / Azure Synapse** ✅ implementado (batch + `mssql-cdc`, `synapse` como modo do mesmo crate) | Meio-termo — SQL Server básico poderia ser OSS, Synapse/CDC avançado fica enterprise |
| ~~ClickHouse~~ | **Foi pro repo público** (`crates/nexus-connectors/nexus-connector-clickhouse`) — investigado numa sessão anterior: RBAC e cluster mode (`Distributed`/`Replicated`, ClickHouse Keeper) são recursos OSS do próprio ClickHouse self-hosted, não existe feature "avançada" genuína pra reservar como paga (diferente de Snowflake/Oracle/SAP, que têm licenciamento pago real por trás). Driver ADBC oficial também é grátis (`dbc install clickhouse`). Não cabe nesta lista. |
| **Teradata** | Nicho legado, ticket alto, baixo volume |
| **IBM Db2** | Mesma categoria de legado corporativo |
| **Vertica** | Nicho analítico, baixo volume mas clientes dispostos a pagar |

## 2. SaaS / CRM / ERP

| Conector | Por quê é pago |
|---|---|
| **Salesforce** ✅ implementado | O conector mais pedido em qualquer ferramenta de integração de dados — prioridade alta |
| **SAP** (BAPI/IDoc/S/4HANA) | ERP mais comum em grandes empresas, integração cara e complexa — alto ticket |
| **HubSpot** | CRM popular em empresas médias, bom volume |
| **Workday** | RH/financeiro enterprise, ticket alto |
| **NetSuite** | ERP de média empresa, demanda constante |
| **Dynamics 365** | Ecossistema Microsoft, correlaciona com clientes que já usam Azure |
| **ServiceNow** | ITSM enterprise, dados de operação |
| **Zendesk** | Suporte/CS, volume alto, ticket menor |

## 3. Marketing / Ads / Analytics (alto volume, padrão Fivetran/Airbyte)

| Conector | Por quê é pago |
|---|---|
| **Google Analytics (GA4)** ✅ implementado | Conector mais usado em stacks de marketing analytics |
| **Google Ads** ✅ implementado | Par natural do GA4 |
| **Meta Ads** (Facebook/Instagram) ✅ implementado | Mesma categoria, alto volume de contas pequenas/médias |
| **LinkedIn Ads** ✅ implementado | Nicho B2B, ticket médio |
| **Stripe** ✅ implementado (read-only por design — nunca ganha sink, transação financeira real fica fora de escopo) | Dados financeiros/billing, alta demanda em SaaS |
| **Shopify** ✅ implementado | E-commerce, alto volume |
| **TikTok Ads** ✅ implementado (não estava na lista original, construído por analogia ao Meta Ads/GA4) | Mesma categoria de marketing analytics, alto volume |
| **YouTube Analytics** ✅ implementado (idem, não estava na lista original) | Mesma categoria, complementa GA4/Google Ads no ecossistema Google |

## 4. Arquivos de escritório / produtividade

| Conector | Por quê é pago |
|---|---|
| **Excel** (`.xlsx`, via `calamine`) ✅ implementado | Fonte de dados mais comum em PMEs sem stack de dados madura — baixa barreira, alto volume |
| **Google Sheets** | Mesma lógica do Excel, mas cloud-native |
| **SharePoint / OneDrive** | Fonte de arquivo genérica em ambiente corporativo Microsoft |

## 5. Vetorial / busca enterprise

| Conector | Por quê é pago |
|---|---|
| **Elasticsearch / OpenSearch** ✅ implementado (`opensearch` como modo do mesmo crate `elasticsearch`) | Busca híbrida (full-text + vetor), presente em boa parte das empresas |
| **Weaviate** ✅ implementado | Vector DB com adoção enterprise crescente |
| **Vertex AI Vector Search / Azure AI Search** ✅ implementado (ambos) | Ligado a cloud specific — cliente já paga a nuvem, dispõe a pagar o conector |
| **Pinecone managed / Milvus cluster mode** | `pinecone`/`milvus` básicos já são OSS (`LICENSING.md §1`); modo gerenciado/cluster como SKU enterprise separado não foi construído — mesmo conector serve os dois hoje |

## 6. Streaming enterprise

| Conector | Por quê é pago |
|---|---|
| **Confluent Cloud** | Kafka gerenciado com Schema Registry + RBAC — não virou conector separado; `docs/KAFKA_MANAGED_SERVICES.md` documenta como conectar o `kafka` OSS existente a Confluent Cloud/Azure Event Hubs via config (SASL/TLS), sem crate novo |
| **Amazon Kinesis** ✅ implementado (source+sink) | Streaming nativo AWS |
| **Azure Event Hubs** | Sem crate próprio — protocolo compatível com Kafka, coberto pelo `kafka` OSS + `docs/KAFKA_MANAGED_SERVICES.md`, mesmo caso do Confluent Cloud acima |
| **Apache Pulsar** ✅ implementado (source+sink) | Alternativa enterprise ao Kafka em alguns setores (telco/financeiro) |

## 7. CDC avançado (já citado em `LICENSING.md §2`)

| Conector | Por quê é pago |
|---|---|
| **Oracle CDC** ✅ implementado (`oracle-cdc`, via LogMiner — não GoldenGate especificamente, mesmo objetivo de CDC nativo sem Debezium) | CDC nativo Oracle sem depender de Debezium |
| **SQL Server CDC enterprise** ✅ implementado (`mssql-cdc`, via `sys.fn_cdc_get_all_changes_*`) | CDC nativo via CT/CDC do SQL Server |
| **Db2 CDC** | Mesma lógica pro legado IBM |

## 8. Protocolos industriais

Categoria nova — não existia até a pesquisa de conector MQTT (OSS, `crates/nexus-connectors/nexus-connector-mqtt`) trazer à tona o contraste: MQTT é protocolo aberto sem lock-in (mesmo critério que já mantém `kafka` como OSS), OPC-UA é o inverso — comprador claro (chão de fábrica/manufatura, mesmo perfil de Oracle/SAP), protocolo bem mais complexo (modelo de informação tipado, não é só pub/sub).

| Conector | Por quê é pago |
|---|---|
| **OPC-UA** | Padrão industrial/SCADA (chão de fábrica, automação predial) — driver Rust real confirmado (`opcua`/`opcua-rs`, MPL-2.0, mantido), não implementado ainda. Diferente de MQTT: cliente disposto a pagar por conectividade industrial certificada, mesmo padrão de Oracle/SAP já enterprise. |

## Priorização sugerida

Ordenado por (demanda de mercado × disposição a pagar), não por dificuldade técnica:

1. **Snowflake, BigQuery, Redshift, Databricks** — tríade+um obrigatória em qualquer RFP enterprise de ELT.
2. **Salesforce, Excel** — os dois conectores mais pedidos em ferramentas comerciais concorrentes, públicos-alvo diferentes (enterprise CRM vs. PME sem stack de dados). Excel já implementado (ver nota no topo); Salesforce continua candidato.
3. **Oracle, SAP (HANA e/ou BAPI/IDoc)** — legado enterprise, ticket alto, cliente já paga caro por licença então tolera pagar pelo conector.
4. **SQL Server/Synapse** — meio-termo, complementa o que já existe OSS (ClickHouse saiu desta lista, foi pro repo público — ver seção 1).
5. **Marketing/Ads** (GA4, Google Ads, Meta Ads, Stripe, Shopify) — alto volume, ticket médio menor, bom motor de PLG (product-led growth).
6. **Vector/search enterprise + CDC avançado + streaming enterprise** — nicho, ticket alto, baixo volume — fazer sob demanda de cliente específico, não especulativamente.

Decisão de "o que construir primeiro" na Fase 12 deve seguir demanda real confirmada (mesmo racional já usado pro CDC nativo condicional em `ROADMAP.md`), não essa lista sozinha — ela é o inventário de candidatos, não um compromisso de roadmap.

**Status real desta priorização:** blocos 1, 2, 3 (parcial — Oracle e HANA feitos, BAPI/IDoc não), 5 e a maior parte do 6 (vector/search + CDC avançado) já foram construídos. Restam sem crate: Db2 (CDC e batch), SAP BAPI/IDoc, HubSpot, Workday, NetSuite, Dynamics 365, ServiceNow, Zendesk, Google Sheets, SharePoint/OneDrive, Teradata, Vertica. (Databricks foi implementado no repo privado; ClickHouse saiu desta lista — foi pro repo público, ver seção 1.)
