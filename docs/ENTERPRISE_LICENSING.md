# 🔐 Licenciamento de Conectores Enterprise — Mercado Pago + Stripe

Detalha a implementação técnica do modelo de licenciamento enterprise descrito em `LICENSING.md §2` e do catálogo em `docs/ENTERPRISE_CONNECTORS.md`, usando **dois** gateways de pagamento, roteados por região/moeda do cliente:

- **Mercado Pago** — Brasil/LatAm, cartão de crédito, débito e **Pix** (essencial pro público PME que conectores como `excel` miram — menor fricção, sem taxa de cartão).
- **Stripe** — compras internacionais (cliente fora do Brasil/LatAm, fatura em USD/EUR/etc.) — tier mais provável pra conectores enterprise de ticket alto (Snowflake/BigQuery/Databricks/Salesforce, `docs/ENTERPRISE_CONNECTORS.md`), onde o cliente já espera pagar em cartão internacional e não tem Pix como opção de qualquer forma.

Ambos convergem no mesmo `nexus-licensing` (emissão de license key), então trocar/somar gateway é troca de um adapter no webhook receiver, não uma reescrita — ver §2.

**Estado real (auditado, não o que a v1 deste doc dizia):** `POST /license` e `GET /license` existem em `nexus-server` (`crates/nexus-server/src/license.rs` + `license_store.rs`) — validam assinatura Ed25519 + `exp` e persistem a JWT no metadata store. **Bloco 1 do `ROADMAP.md` Fase 12 (enforcement real) está implementado**: `validate_source_config`/`validate_sink_config`/`build_source`/`build_sink` (`connectors.rs`) checam a license ativa antes de validar/salvar/rodar, e o frontend consome `licensed`/`requires_license` (cadeado no `ConnectorPalette.tsx`, aba Store com status de license). O que falta é só o lado de cobrança: o serviço `nexus-licensing` separado e a integração com Mercado Pago/Stripe ainda não existem — este doc continua sendo o design de referência pra essa parte (Bloco 2). `docs/PLUGIN_STORE_PLAN.md` cobre o mesmo Bloco 2/planejamento de store, escrito antes do Bloco 1 existir — precisa de uma atualização de status, não é a fonte de verdade atual.

## 1. Visão geral do fluxo

```
[Cliente]         [Checkout Mercado Pago OU Stripe]     [nexus-licensing (novo serviço)]      [nexus-server do cliente]
    |                            |                                  |                                    |
    |--- escolhe conector(es) -->|                                  |                                    |
    |    (gateway roteado por    |                                  |                                    |
    |     país/moeda do cliente) |                                  |                                    |
    |--- paga -------------------|                                  |                                    |
    |                            |--- webhook: payment approved --->|                                    |
    |                            |                                  |--- gera license key (JWT assinado)-|
    |                            |                                  |--- dispara emissão de NFe/NFSe     |
    |                            |                                  |    (só p/ pedido via Mercado Pago) |
    |<----------- email com a license key / link do portal ---------|                                    |
    |                                                                                                     |
    |------------------------------- cola a license key na config do nexus-server -------------------->  |
    |                                                                                                     |
    |                                                              [nexus-server valida assinatura +      |
    |                                                               expiry + lista de conectores] --------|
    |                                                                  |
    |                                                                  v
    |                                                     [conector enterprise liberado no Canvas]
```

`nexus-licensing` é um serviço novo, separado do `nexus-server` OSS — nunca fica no repo público (mesma regra dos conectores enterprise em `LICENSING.md §3`). Ele fala com os dois gateways, mas a emissão de license key (JWT Ed25519, §4) é idêntica pros dois — o gateway só decide *como o pagamento foi cobrado e confirmado*, não o formato da license.

**Como escolher o gateway:** endereço de cobrança/país do cliente no checkout decide — Brasil/LatAm vai pro Mercado Pago (Pix disponível), qualquer outro país vai pro Stripe. v1 pode ser um toggle manual na página de venda (Bloco 4) em vez de detecção automática de geolocalização — decisão de produto, não bloqueia o design técnico abaixo.

## 2. Mercado Pago — qual API usar

- **Checkout Pro** (redirect hospedado pelo Mercado Pago) para a v1: já cobre cartão de crédito, débito e Pix sem o NexusFlow nunca tocar em dado de cartão — resolve escopo de PCI compliance de graça. Cliente é redirecionado, paga, volta pra `back_url` de sucesso/falha/pendente.
- **Preapproval API** (assinatura recorrente) só entra se o modelo de preço virar assinatura por seat/mês em vez de compra avulsa por conector — decisão de negócio em aberto, não bloqueia a v1.
- **Webhooks v2** (`https://api.mercadopago.com/v1/webhooks` config) — endpoint público em `nexus-licensing` recebe notificação `payment.updated`; sempre **buscar o pagamento de volta na API do Mercado Pago pelo ID recebido** (nunca confiar no payload do webhook sozinho — payload pode ser forjado) e validar o header `x-signature` (HMAC com o webhook secret da conta).
- Idempotência: `payment_id` do Mercado Pago é a chave de deduplicação — webhook pode chegar mais de uma vez pro mesmo pagamento.

## 2b. Stripe — compras internacionais

- **Checkout Session** (`stripe.checkout.sessions.create`, modo `payment`) — mesmo racional do Checkout Pro: hospedado pela Stripe, redirect, NexusFlow nunca toca dado de cartão. Um `line_item` por conector/bundle selecionado (`price_data` com `product_data.metadata.connector_slug` — mesmo jeito de carregar o slug que os `products.connector_slug` já fazem pro Mercado Pago, ver §3), `success_url`/`cancel_url` equivalentes aos `back_url` do Mercado Pago.
- **Stripe Billing/Subscriptions** — mesmo status do Preapproval API do Mercado Pago: só entra se o modelo virar assinatura recorrente, não bloqueia a v1 de compra avulsa.
- **Webhooks** (`checkout.session.completed`, e/ou `payment_intent.succeeded` como confirmação redundante) — endpoint separado em `nexus-licensing` (rota própria, ex. `/webhooks/stripe`, distinta da `/webhooks/mercadopago`). Validar a assinatura via `Stripe-Signature` header (HMAC com o webhook signing secret, biblioteca oficial `stripe` do Rust já resolve isso — `stripe::Webhook::construct_event`) antes de processar qualquer payload, mesmo princípio de "nunca confiar sozinho no payload" do Mercado Pago. Diferente do Mercado Pago, o evento do Stripe já vem com o objeto completo (não precisa um segundo round-trip pra buscar o pagamento por ID) — mas ainda assim reconferir `payment_status == "paid"` no evento antes de emitir a license.
- **Moeda**: Stripe cobra na moeda que o produto for cadastrado (tipicamente USD para o catálogo internacional) — não faz FX automático com o preço em BRL do Mercado Pago; os dois preços (`products.price_cents` por gateway, ver §3) são cadastrados/mantidos separadamente.
- **Nota fiscal**: Stripe Tax (opcional, cobra automaticamente VAT/sales tax conforme jurisdição do cliente) resolve a maior parte do compliance fiscal internacional — bem mais simples que NFe/NFSe brasileira, que só se aplica ao fluxo Mercado Pago (§6). Sem Stripe Tax configurado, é responsabilidade do vendedor determinar imposto por jurisdição manualmente — recomendo ativar desde o primeiro pedido internacional.
- Idempotência: `checkout.session.id` (ou `payment_intent.id`) é a chave de deduplicação, mesmo papel que `payment_id` cumpre no lado Mercado Pago.

## 3. Modelo de dados (Postgres — usa o `MetadataPool` já existente, ver `ARCHITECTURE.md §14`)

Tabelas novas em `nexus-licensing` (banco próprio, não compartilha com o metadata store do `nexus-server` do cliente):

- `products` — catálogo: `id`, `connector_slug` (ex. `salesforce`, `snowflake`), `name`, `price_cents_brl` (Mercado Pago), `price_cents_usd` (Stripe), `active`. Dois preços porque os gateways não fazem FX automático entre si (§2b) — mantidos manualmente.
- `customers` — `id`, `email`, `mp_customer_id` (opcional, Customer API do Mercado Pago), `stripe_customer_id` (opcional, Stripe Customer object).
- `orders` — `id`, `customer_id`, `gateway` (`mercado_pago` | `stripe`), `gateway_payment_id` (`mp_payment_id` ou `checkout.session.id`/`payment_intent.id` — um campo genérico em vez de duas colunas separadas, já que nunca são preenchidas juntas), `status` (`pending`/`approved`/`rejected`), `product_ids[]`, `created_at`.
- `licenses` — `id`, `customer_id`, `connector_slugs[]`, `seats`, `issued_at`, `expires_at`, `revoked_at` (nullable), `jwt_kid` (qual chave assinou, pra suportar rotação). Sem coluna de gateway — a license em si é idêntica não importa por onde foi paga (§4).

## 4. License key (formato)

JWT assinado com **Ed25519** (mais rápido de verificar que RSA, chave menor):
```json
{
  "sub": "<customer_id>",
  "connectors": ["salesforce", "snowflake"],
  "seats": 5,
  "iat": 1754784000,
  "exp": 1786320000,
  "kid": "2026-v1"
}
```
- Chave **privada** só existe em `nexus-licensing` (nunca no binário distribuído).
- Chave **pública** embutida no binário do `nexus-server`/conector enterprise via `include_str!` em build time — sem chamada de rede pra validar (funciona offline, cliente pode rodar air-gapped).
- Renovação/revogação: como é offline-first, `exp` curto (ex. 13 meses pra dar margem de renovação anual) é o mecanismo principal de revogação — não dá pra revogar um JWT já emitido sem phone-home. Se precisar de revogação ativa no meio da vigência (ex. chargeback), isso exige uma segunda camada opcional de verificação online — **fora de escopo da v1**, documentar como limitação conhecida.

## 5. Mudanças no `nexus-server` (OSS)

**Já implementado:**
- `POST /license` (Admin-only, RBAC) — recebe a license key, valida assinatura (chave pública embutida) + `exp`, salva a JWT em texto puro numa tabela `license` no metadata store existente (a própria assinatura já é à prova de adulteração — não tem segredo adicional a proteger com criptografia em repouso, ao contrário de uma senha/URI de conector).
- `GET /license` — retorna a license ativa (claims decodificadas: `connectors`, `seats`, `exp`).
- `GET /connectors` já calcula `licensed: bool` por conector no catálogo — só cosmético hoje, o Canvas não consome ainda.

**Também já implementado (não estava quando a v1 deste doc foi escrita):**
- Enforcement real: `validate_source_config`/`validate_sink_config`/`build_source`/`build_sink` (`connectors.rs`) checam a license ativa antes de validar/salvar/rodar um pipeline com conector `requires_license` (`check_connector_license`, chamado em cada um). O conector segue sempre listado no catálogo mesmo sem license cobrindo (pra quem não tem license ver que existe e comprar) — só bloqueia salvar/rodar, não a listagem.
- Frontend consumindo `licensed`/`requires_license`: ícone de cadeado no `ConnectorPalette.tsx`, aba **Store** (`frontend/src/components/Store.tsx`) com status de license instalada + form de instalação (Admin-only).

**Ainda não implementado:** o serviço `nexus-licensing` em si e a integração com os dois gateways (§2/§2b) — é o que este doc continua desenhando.

## 6. Nota fiscal

- **Mercado Pago**: não emite nota fiscal pelo vendedor — é só o gateway de pagamento. Precisa de integração separada disparada no mesmo webhook de pagamento aprovado: **NFE.io** ou **eNotas** (ambas têm API REST simples, focadas em SaaS brasileiro).
- **Stripe**: **Stripe Tax** (opcional, ver §2b) cobre a maior parte do compliance de VAT/sales tax internacional direto na Checkout Session — não precisa de um NFE.io/eNotas equivalente pro fluxo internacional.

Ambos ficam no `nexus-licensing`, não no `nexus-server`.

## 7. Segurança

- Nunca processar/armazenar número de cartão — Checkout Pro e Stripe Checkout Session garantem isso por design (redirect, PCI compliance do gateway).
- Validar a assinatura de todo webhook antes de processar: `x-signature` (Mercado Pago, HMAC com o webhook secret da conta) ou `Stripe-Signature` (Stripe, via `stripe::Webhook::construct_event`) — nunca confiar no payload sozinho em nenhum dos dois.
- Rate-limit em cada endpoint de webhook (mesmo padrão já usado no rate-limit de login do `nexus-server`).
- `nexus-licensing` fica em repo privado separado, mesma regra dos conectores enterprise — inclusive os dois webhook secrets (Mercado Pago e Stripe) só existem lá, nunca no repo público.

## 8. Fora de escopo da v1 (documentar, não implementar agora)

- Portal do cliente self-service (ver licenças, baixar binário) — v1 manda tudo por email.
- Revogação ativa online (phone-home) — v1 é offline-only com `exp` curto.
- Assinatura recorrente (Preapproval API / Stripe Billing) — v1 é compra avulsa por conector/bundle.
- Reembolso automatizado (direito de arrependimento CDC 7 dias, Mercado Pago; ou reembolso Stripe) — v1 é processo manual (suporte revoga license na mão via `revoked_at`, mesmo sem phone-home isso serve de registro interno).
- Detecção automática de região/moeda pra rotear entre os dois gateways — v1 é um toggle manual na página de venda (§1).

## Próximos passos

1. Confirmar modelo de preço (por conector avulso vs. bundle vs. tier) — decisão de negócio, não técnica.
2. Criar `nexus-licensing` como repo privado novo (fora do monorepo OSS, mesma regra dos conectores enterprise) — webhook receivers pros dois gateways (§2/§2b), tabelas do §3.
3. Até isso existir, license keys de teste assinadas na mão (par de chaves de teste em `license.rs`) já bastam pra vender/validar o primeiro conector (Excel) — ver `ROADMAP.md` Fase 12 Bloco 3.
