# 🔐 Licenciamento de Conectores Enterprise — Mercado Pago

Detalha a implementação técnica do modelo de licenciamento enterprise descrito em `LICENSING.md §2` e do catálogo em `docs/ENTERPRISE_CONNECTORS.md`, usando **Mercado Pago** como gateway (cartão de crédito, débito e Pix).

**Estado real (auditado, não o que a v1 deste doc dizia):** `POST /license` e `GET /license` existem em `nexus-server` (`crates/nexus-server/src/license.rs` + `license_store.rs`) — validam assinatura Ed25519 + `exp` e persistem a JWT no metadata store. Mas isso é só **metade** do gate: `GET /connectors` já calcula um `licensed: bool` por conector (cosmético, catálogo), só que **nada bloqueia de verdade** ainda — `validate_source_config`/`validate_sink_config`/`build_source`/`build_sink` (`connectors.rs`) não checam license nenhuma, e o frontend não consome `licensed` em lugar nenhum. Esse enforcement real é o item em aberto — ver `ROADMAP.md` Fase 12, Bloco 1. O serviço `nexus-licensing` separado e a integração com Mercado Pago também ainda não existem — este doc continua sendo o design de referência pra essa parte.

## 1. Visão geral do fluxo

```
[Cliente]                [Checkout Mercado Pago]        [nexus-licensing (novo serviço)]      [nexus-server do cliente]
    |                            |                                  |                                    |
    |--- escolhe conector(es) -->|                                  |                                    |
    |--- paga (cartão/débito/Pix)|                                  |                                    |
    |                            |--- webhook: payment approved --->|                                    |
    |                            |                                  |--- gera license key (JWT assinado)-|
    |                            |                                  |--- dispara emissão de NFe/NFSe     |
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

`nexus-licensing` é um serviço novo, separado do `nexus-server` OSS — nunca fica no repo público (mesma regra dos conectores enterprise em `LICENSING.md §3`).

## 2. Mercado Pago — qual API usar

- **Checkout Pro** (redirect hospedado pelo Mercado Pago) para a v1: já cobre cartão de crédito, débito e Pix sem o NexusFlow nunca tocar em dado de cartão — resolve escopo de PCI compliance de graça. Cliente é redirecionado, paga, volta pra `back_url` de sucesso/falha/pendente.
- **Preapproval API** (assinatura recorrente) só entra se o modelo de preço virar assinatura por seat/mês em vez de compra avulsa por conector — decisão de negócio em aberto, não bloqueia a v1.
- **Webhooks v2** (`https://api.mercadopago.com/v1/webhooks` config) — endpoint público em `nexus-licensing` recebe notificação `payment.updated`; sempre **buscar o pagamento de volta na API do Mercado Pago pelo ID recebido** (nunca confiar no payload do webhook sozinho — payload pode ser forjado) e validar o header `x-signature` (HMAC com o webhook secret da conta).
- Idempotência: `payment_id` do Mercado Pago é a chave de deduplicação — webhook pode chegar mais de uma vez pro mesmo pagamento.

## 3. Modelo de dados (Postgres — usa o `MetadataPool` já existente, ver `ARCHITECTURE.md §14`)

Tabelas novas em `nexus-licensing` (banco próprio, não compartilha com o metadata store do `nexus-server` do cliente):

- `products` — catálogo: `id`, `connector_slug` (ex. `salesforce`, `snowflake`), `name`, `price_cents`, `active`.
- `customers` — `id`, `email`, `mp_customer_id` (opcional, se usar Customer API do Mercado Pago).
- `orders` — `id`, `customer_id`, `mp_payment_id`, `status` (`pending`/`approved`/`rejected`), `product_ids[]`, `created_at`.
- `licenses` — `id`, `customer_id`, `connector_slugs[]`, `seats`, `issued_at`, `expires_at`, `revoked_at` (nullable), `jwt_kid` (qual chave assinou, pra suportar rotação).

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

**Ainda não implementado (Bloco 1, ROADMAP.md Fase 12):**
- Enforcement real: `validate_source_config`/`validate_sink_config`/`build_source`/`build_sink` (`connectors.rs`) e `run_pipeline` (`runner.rs`) checando a license ativa antes de validar/salvar/rodar um pipeline com conector `requires_license`. Diferente do design original deste doc (não registrar o conector se sem license) — a decisão foi manter o conector sempre listado no catálogo (pra quem não tem license ver que existe e comprar), só bloqueando salvar/rodar.
- Frontend consumindo `licensed` (ícone de cadeado no `ConnectorPalette.tsx`, mensagem clara de erro ao salvar/rodar).

## 6. Nota fiscal (NFe/NFSe)

Mercado Pago **não emite nota fiscal pelo vendedor** — é só o gateway de pagamento. Precisa de integração separada disparada no mesmo webhook de pagamento aprovado: **NFE.io** ou **eNotas** (ambas têm API REST simples, focadas em SaaS brasileiro). Ficam no `nexus-licensing`, não no `nexus-server`.

## 7. Segurança

- Nunca processar/armazenar número de cartão — Checkout Pro garante isso por design (redirect).
- Validar `x-signature` de todo webhook do Mercado Pago antes de processar.
- Rate-limit no endpoint de webhook (mesmo padrão já usado no rate-limit de login do `nexus-server`).
- `nexus-licensing` fica em repo privado separado, mesma regra dos conectores enterprise.

## 8. Fora de escopo da v1 (documentar, não implementar agora)

- Portal do cliente self-service (ver licenças, baixar binário) — v1 manda tudo por email.
- Revogação ativa online (phone-home) — v1 é offline-only com `exp` curto.
- Assinatura recorrente (Preapproval API) — v1 é compra avulsa por conector/bundle.
- Reembolso automatizado (direito de arrependimento CDC 7 dias) — v1 é processo manual (suporte revoga license na mão via `revoked_at`, mesmo sem phone-home isso serve de registro interno).

## Próximos passos

1. Enforcement real no `nexus-server` (§5, "ainda não implementado") — Bloco 1 do ROADMAP.md Fase 12. Não depende de nada abaixo, pode ser feito com license keys geradas manualmente pra teste (par de chaves de teste já existe em `license.rs`).
2. Confirmar modelo de preço (por conector avulso vs. bundle vs. tier) — decisão de negócio, não técnica.
3. Criar `nexus-licensing` como repo privado novo (fora do monorepo OSS, mesma regra dos conectores enterprise).
