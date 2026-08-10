# 💰 Conectores Enterprise (candidatos) — NexusFlow

Este doc detalha os candidatos a conector pago citados em `LICENSING.md §2`. Vivem em repo/crate privado separado (`nexus-connectors-enterprise`), carregados via plugin em runtime com license key — nunca entram em `crates/nexus-connectors/` (OSS). Ver `ARCHITECTURE.md` e `ROADMAP.md` (Fase 12, ainda não implementada).

Ponto de partida do usuário: Excel, Oracle, Snowflake, ClickHouse, BigQuery, Redshift. Abaixo, esses mais outros candidatos organizados por categoria, com a lógica de mercado por trás de cada um (o mesmo racional que Fivetran/Airbyte/Matillion usam pra decidir o que cobra).

## 1. Data Warehouses / bancos analíticos enterprise

| Conector | Por quê é pago |
|---|---|
| **Snowflake** | Maior demanda de mercado em ferramentas ELT — praticamente todo concorrente cobra por esse conector |
| **BigQuery** | Mesma categoria do Snowflake, par indissociável em RFPs enterprise |
| **Redshift** | Terceiro da tríade "cloud DW" — quem pede um, geralmente pede os três |
| **Databricks** (SQL Warehouse / Unity Catalog via Flight SQL) | Ligado ao lakehouse — encaixa direto na proposta "AI Lakehouse Builder" do NexusFlow |
| **ClickHouse** (features enterprise / ClickHouse Cloud) | ADBC básico pode ficar OSS; recursos avançados (cluster, RBAC) ficam pagos |
| **Oracle** | Legado enterprise, ticket médio alto, cliente já paga licença Oracle cara — tolerância a pagar por conector é maior |
| **SAP HANA** | Mesma lógica do Oracle — instalado em empresas grandes com orçamento de integração |
| **Microsoft SQL Server / Azure Synapse** | Meio-termo — SQL Server básico poderia ser OSS, Synapse/CDC avançado fica enterprise |
| **Teradata** | Nicho legado, ticket alto, baixo volume |
| **IBM Db2** | Mesma categoria de legado corporativo |
| **Vertica** | Nicho analítico, baixo volume mas clientes dispostos a pagar |

## 2. SaaS / CRM / ERP

| Conector | Por quê é pago |
|---|---|
| **Salesforce** | O conector mais pedido em qualquer ferramenta de integração de dados — prioridade alta |
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
| **Google Analytics (GA4)** | Conector mais usado em stacks de marketing analytics |
| **Google Ads** | Par natural do GA4 |
| **Meta Ads** (Facebook/Instagram) | Mesma categoria, alto volume de contas pequenas/médias |
| **LinkedIn Ads** | Nicho B2B, ticket médio |
| **Stripe** | Dados financeiros/billing, alta demanda em SaaS |
| **Shopify** | E-commerce, alto volume |

## 4. Arquivos de escritório / produtividade

| Conector | Por quê é pago |
|---|---|
| **Excel** (`.xlsx`, via `calamine`) | Fonte de dados mais comum em PMEs sem stack de dados madura — baixa barreira, alto volume |
| **Google Sheets** | Mesma lógica do Excel, mas cloud-native |
| **SharePoint / OneDrive** | Fonte de arquivo genérica em ambiente corporativo Microsoft |

## 5. Vetorial / busca enterprise

| Conector | Por quê é pago |
|---|---|
| **Elasticsearch / OpenSearch** | Busca híbrida (full-text + vetor), presente em boa parte das empresas |
| **Weaviate** | Vector DB com adoção enterprise crescente |
| **Vertex AI Vector Search / Azure AI Search** | Ligado a cloud specific — cliente já paga a nuvem, dispõe a pagar o conector |
| **Pinecone managed / Milvus cluster mode** | Já citados em `LICENSING.md §2` — modo gerenciado/cluster é o que diferencia do que já é OSS |

## 6. Streaming enterprise

| Conector | Por quê é pago |
|---|---|
| **Confluent Cloud** | Kafka gerenciado com Schema Registry + RBAC — o Kafka OSS via Debezium já existe, isso é a camada enterprise |
| **Amazon Kinesis** | Streaming nativo AWS |
| **Azure Event Hubs** | Streaming nativo Azure |
| **Apache Pulsar** | Alternativa enterprise ao Kafka em alguns setores (telco/financeiro) |

## 7. CDC avançado (já citado em `LICENSING.md §2`)

| Conector | Por quê é pago |
|---|---|
| **Oracle GoldenGate-style** | CDC nativo Oracle sem depender de Debezium |
| **SQL Server CDC enterprise** | CDC nativo via CT/CDC do SQL Server |
| **Db2 CDC** | Mesma lógica pro legado IBM |

## Priorização sugerida

Ordenado por (demanda de mercado × disposição a pagar), não por dificuldade técnica:

1. **Snowflake, BigQuery, Redshift, Databricks** — tríade+um obrigatória em qualquer RFP enterprise de ELT.
2. **Salesforce, Excel** — os dois conectores mais pedidos em ferramentas comerciais concorrentes, públicos-alvo diferentes (enterprise CRM vs. PME sem stack de dados).
3. **Oracle, SAP (HANA e/ou BAPI/IDoc)** — legado enterprise, ticket alto, cliente já paga caro por licença então tolera pagar pelo conector.
4. **ClickHouse (avançado), SQL Server/Synapse** — meio-termo, complementam o que já existe OSS.
5. **Marketing/Ads** (GA4, Google Ads, Meta Ads, Stripe, Shopify) — alto volume, ticket médio menor, bom motor de PLG (product-led growth).
6. **Vector/search enterprise + CDC avançado + streaming enterprise** — nicho, ticket alto, baixo volume — fazer sob demanda de cliente específico, não especulativamente.

Decisão de "o que construir primeiro" na Fase 12 deve seguir demanda real confirmada (mesmo racional já usado pro CDC nativo condicional em `ROADMAP.md`), não essa lista sozinha — ela é o inventário de candidatos, não um compromisso de roadmap.
