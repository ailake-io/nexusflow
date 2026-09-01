# 🔐 Licenciamento de Conectores Enterprise — Stripe

Detalha a implementação técnica do modelo de licenciamento enterprise descrito em `LICENSING.md §2` e do catálogo em `docs/ENTERPRISE_CONNECTORS.md`, usando **Stripe** como único gateway de pagamento — cartão (BRL/USD) e **Pix** (BRL, via `payment_method_types` do Stripe, disponível pra contas domiciliadas no Brasil).

> Versão anterior deste doc descrevia dois gateways (Mercado Pago + Stripe). Decisão de produto: só Stripe — cobre cartão nas duas moedas e Pix na mesma integração, sem manter dois webhook receivers/dois esquemas de preço.

**Estado real (auditado):** `POST /license` e `GET /license` existem em `nexus-server` (`crates/nexus-server/src/license.rs` + `license_store.rs`) — validam assinatura Ed25519 + `exp` e persistem a JWT no metadata store. Enforcement real (`connectors.rs`) e UI (cadeado no `ConnectorPalette.tsx`, aba **Store**) já funcionam. `LICENSE_PUBLIC_KEY_PEM` era um placeholder sem par privado em lugar nenhum até `3c49e57` — nenhuma license real teria verificado antes desse fix; confirmado end-to-end com um par Ed25519 real (§5). O serviço `nexus-licensing` (repo privado novo, `ailake-io/nexus-licensing`) **existe** — scaffold Axum/Postgres/Stripe completo (checkout, webhook, emissão de license, API admin), commitado na `main`, ainda não deployado nem ligado a uma conta Stripe real. O que falta: rodar em produção com credenciais reais e o botão "Comprar" no `Store.tsx` (§5) chamando o serviço.

## 1. Visão geral do fluxo

```
[Cliente]              [Stripe Checkout]           [nexus-licensing]              [nexus-server do cliente]
    |                          |                          |                                |
    |--- escolhe conector(es) e moeda (BRL/USD) ---------->|                                |
    |                          |    POST /checkout cria a Checkout Session                  |
    |<--- redireciona pra checkout_url ---------------------|                               |
    |--- paga (cartão, ou Pix se BRL) --------------------->|                                |
    |                          |--- webhook: checkout.session.completed ------------------->|
    |                          |                          |--- gera license key (JWT Ed25519)-|
    |<----------- email com a license key -------------------------------------------------|
    |                                                                                        |
    |------------------------------- cola a license key na config do nexus-server -------->  |
    |                                                                                        |
    |                                                     [nexus-server valida assinatura +   |
    |                                                      expiry + lista de conectores] -----|
    |                                                          |
    |                                                          v
    |                                             [conector enterprise liberado no Canvas]
```

`nexus-licensing` é um serviço separado, sempre operado centralmente pela ailake — nunca fica no repo público nem é distribuído no binário do cliente (mesma regra dos conectores enterprise, `LICENSING.md §3`). O `nexus-server` do cliente nunca fala com o Stripe nem guarda a chave secreta do Stripe — só recebe a license key já pronta, por email, e cola no `POST /license` que já existe.

## 2. Stripe — fluxo de checkout e webhook

- **Checkout Session** (`POST /checkout` em `nexus-licensing`, cria uma `stripe::checkout_session::CreateCheckoutSession` em modo `payment`) — hospedado pela Stripe, redirect, NexusFlow nunca toca dado de cartão. Um `line_item` por conector selecionado (`price_data` com `product_data.metadata.connector_slug`), `payment_method_types: [card, pix]` se `currency == brl`, só `[card]` se `usd` (Stripe não aceita Pix em cobrança USD). `success_url`/`cancel_url` configuráveis.
- **Webhook** (`POST /webhooks/stripe`) — escuta `checkout.session.completed`; valida `Stripe-Signature` via `stripe::Webhook::construct_event` **antes** de processar qualquer payload (nunca confiar no payload sozinho), reconfere `payment_status == "paid"` mesmo com o tipo de evento já implicando isso. Idempotente por `orders.stripe_checkout_session_id` (`UNIQUE`, `ON CONFLICT DO NOTHING`) — Stripe pode entregar o mesmo evento mais de uma vez.
- **Stripe Billing/Subscriptions** — fora de escopo da v1 (compra avulsa por conector, sem assinatura recorrente).
- **Nota fiscal (BRL)**: mesmo cobrando via Stripe, venda em BRL pra cliente brasileiro provavelmente ainda exige NFe/NFSe por lei — é o local da venda que importa, não o gateway (§6). **Confirmar com contabilidade/jurídico antes de habilitar checkout BRL real em produção.**
- Idempotência: `checkout_session.id` é a chave de deduplicação.

## 3. Modelo de dados (Postgres, banco próprio do `nexus-licensing` — não compartilha com o metadata store do `nexus-server` do cliente)

Implementado em `nexus-licensing/migrations/0001_init.sql`:

- `products` — catálogo: `id`, `connector_slug` (ex. `salesforce`, `snowflake`), `name`, `price_cents_brl`, `price_cents_usd`, `active`. Dois preços porque Stripe não faz FX automático — mantidos manualmente.
- `customers` — `id`, `email` (`UNIQUE`), `stripe_customer_id` (`UNIQUE`, opcional).
- `orders` — `id`, `customer_id`, `stripe_checkout_session_id` (`UNIQUE` — chave de idempotência), `stripe_payment_intent_id` (`UNIQUE`), `currency` (`brl`/`usd`), `payment_method` (`card`/`pix`), `status` (`pending`/`paid`/`failed`), `product_ids[]`, `created_at`.
- `licenses` — `id`, `customer_id`, `order_id` (nullable — `NULL` pra license emitida manualmente via `/admin/licenses`, sem pedido Stripe por trás), `connector_slugs[]`, `seats`, `jwt`, `issued_at`, `expires_at`, `revoked_at` (nullable, só bookkeeping — ver §8), `jwt_kid`.

## 4. License key (formato)

JWT assinado com **Ed25519** (mais rápido de verificar que RSA, chave menor):
```json
{
  "sub": "<customer_id>",
  "connectors": ["salesforce", "snowflake"],
  "seats": 1,
  "iat": 1754784000,
  "exp": 1786320000,
  "kid": "2026-v1"
}
```
- Chave **privada** só existe como variável de ambiente (`LICENSE_SIGNING_PRIVATE_KEY_PEM`) no deploy do `nexus-licensing` — nunca commitada, nunca no binário distribuído.
- Chave **pública** embutida no binário do `nexus-server`/conector enterprise (`crates/nexus-server/src/license.rs::LICENSE_PUBLIC_KEY_PEM`, repo público) — sem chamada de rede pra validar (funciona offline, cliente pode rodar air-gapped).
- Validade fixa de 395 dias (`license::LICENSE_VALIDITY`) — dá margem de renovação anual. Como é offline-first, `exp` curto é o mecanismo principal de "revogação" — não dá pra revogar um JWT já emitido sem phone-home (§8).
- `kid` identifica qual chave assinou, pra suportar rotação — `nexus-server` pode aceitar chave antiga + nova durante uma janela de transição, sem invalidar license já emitida.

## 5. Mudanças no `nexus-server` (OSS) e no frontend

**Já implementado:**
- `LICENSE_PUBLIC_KEY_PEM` em `license.rs` é o par Ed25519 real (não mais o placeholder que nunca teve chave privada correspondente em lugar nenhum — bug achado e corrigido em `3c49e57`: toda license emitida teria falhado verificação contra a constante antiga; confirmado gerando um par real, emitindo license via `/admin/licenses` e verificando local com `nexus-server`). Testes continuam usando `test_support::TEST_PUBLIC_KEY_PEM`/`TEST_PRIVATE_KEY_PEM` via `#[cfg(test)]`, sem precisar da chave privada real no repo.
- `POST /license` / `GET /license` (Admin-only, RBAC) — valida assinatura + `exp`, persiste a JWT.
- Enforcement real: `validate_source_config`/`validate_sink_config`/`build_source`/`build_sink` (`connectors.rs`) checam a license ativa antes de validar/salvar/rodar um pipeline com conector `requires_license`.
- Frontend: cadeado no `ConnectorPalette.tsx`, aba **Store** (`frontend/src/components/Store.tsx`) com status de license instalada + form de instalação manual (colar a JWT).

**Ainda falta:**
- Botão "Comprar" no `Store.tsx` pra cada conector bloqueado, com toggle BRL/USD, chamando `POST /checkout` do `nexus-licensing` (fetch cross-origin, URL configurável via `VITE_LICENSING_API_URL`) e redirecionando pro `checkout_url` retornado. O form de colar a license key manualmente continua existindo — o botão só adiciona o caminho automatizado até o Stripe.

## 6. Nota fiscal

- **Stripe Tax** (opcional) cobre boa parte do compliance de VAT/sales tax internacional direto na Checkout Session — mas venda BRL pra cliente brasileiro é uma questão separada de legislação fiscal local, não coberta pelo Stripe Tax. NFe/NFSe (NFE.io ou eNotas) continua sendo necessário pra esse caso — **documentado como não implementado ainda em `nexus-licensing/README.md`, confirmar com contabilidade/jurídico antes de vender BRL de verdade.**

## 7. Segurança

- Nunca processar/armazenar número de cartão — Stripe Checkout Session garante isso por design (redirect, PCI compliance da Stripe).
- Validar `Stripe-Signature` via `stripe::Webhook::construct_event` antes de processar qualquer payload de webhook — nunca confiar no payload sozinho.
- `/admin/*` (emissão manual, consulta, revoke bookkeeping) atrás de `X-Admin-Api-Key`, comparação constant-time.
- `nexus-licensing` fica em repo privado separado (`ailake-io/nexus-licensing`), mesma regra dos conectores enterprise — a chave secreta do Stripe e o webhook signing secret só existem lá (env vars), nunca no repo público.
- `.gitignore` do `nexus-licensing` bloqueia `*.pem`/`.env` — chave privada real e credenciais nunca ficam `git add`-áveis por acidente.

## 8. Fora de escopo da v1 (documentado, não implementado agora)

- Portal do cliente self-service (ver licenças, baixar binário) — v1 manda tudo por email.
- Revogação ativa online (phone-home) — `DELETE /admin/licenses/{id}` só marca `revoked_at` pra bookkeeping interno; uma JWT já emitida continua validando no `nexus-server` do cliente até `exp`, mesmo revogada.
- Assinatura recorrente (Stripe Billing) — v1 é compra avulsa por conector.
- Reembolso automatizado — v1 é processo manual (reembolso no dashboard da Stripe + revoke da license via API admin, registro interno apenas).
- Seleção de quantidade de seats no checkout — v1 sempre emite `seats: 1`.
- Emissão de NFe/NFSe — ver §6.

## Próximos passos

1. ~~Gerar o par Ed25519 de produção e trocar `LICENSE_PUBLIC_KEY_PEM`~~ — resolvido em `3c49e57` (§5). A chave privada real ainda precisa ser configurada como `LICENSE_SIGNING_PRIVATE_KEY_PEM` no deploy de produção do `nexus-licensing` quando este for de fato colocado no ar (item 3 abaixo) — hoje só foi validada localmente.
2. Confirmar com jurídico/contabilidade a obrigação de NFe/NFSe antes de habilitar checkout BRL real (§6).
3. Deployar `nexus-licensing` com credenciais Stripe reais (test mode primeiro, `stripe listen` pra validar o webhook localmente).
4. Botão "Comprar" no `Store.tsx` (§5).
