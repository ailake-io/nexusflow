# 🏪 Plano de Implementação — Store de Plugins / Conectores Enterprise

> **Escopo:** catálogo, checkout, pagamento, emissão de license key e entrega/liberação de conectores enterprise no NexusFlow.
> **Status (auditado, ver `docs/ENTERPRISE_LICENSING.md` §"Estado real" — fonte de verdade atual):** o gate técnico de licenciamento no `nexus-server`, o enforcement em runtime e a integração frontend (cadeado + aba Store) já estão implementados; o catálogo enterprise real já existe (24 crates / 51 entradas no repo privado). O que falta é só o lado de cobrança — o serviço `nexus-licensing` (catálogo/checkout/webhook) não existe ainda. As seções abaixo (decisões de checkout/pricing/store) continuam válidas como planejamento pra essa parte que falta.
> **Repos envolvidos:**
> - `ailake-io/nexusflow` (este repo, OSS) — gate de license no servidor + integrações frontend.
> - `ailake-io/nexus-connectors-enterprise` (repo privado, já existe, 24 conectores) — conectores pagos.
> - `ailake-io/nexus-licensing` (repo privado, a criar) — catálogo, checkout e emissão de license keys.

---

## 1. Estado atual (gate técnico pronto no servidor)

Já existe no repo OSS a infra completa pra validar, armazenar e **fazer valer** uma license key:

| Componente | Onde | O que faz | Status |
|---|---|---|---|
| `LicenseClaims` + `verify()` | `crates/nexus-server/src/license.rs` | Valida JWT EdDSA (Ed25519), checa `exp`. | ✅ pronto |
| `LicenseStore` | `crates/nexus-server/src/license_store.rs` | Persiste a license ativa em `MetadataPool` (SQLite/Postgres), métodos `install` e `active`. | ✅ pronto |
| Endpoints `/license` | `crates/nexus-server/src/lib.rs:206-207` | `POST /license` (instala) e `GET /license` (status), ambos Admin-only — mesmo path, métodos diferentes, não `/license/status`. | ✅ pronto |
| Catálogo com filtro `licensed` | `crates/nexus-server/src/lib.rs` | `GET /connectors` retorna `licensed: bool` por conector, usando a license ativa. | ✅ pronto |
| Registry enterprise | `crates/nexus-core/src/registry.rs` | `ConnectorDescriptor` tem `requires_license: Option<&'static str>` e macro `submit_enterprise_connector!`. | ✅ pronto |
| **Enforcement em runtime** | `crates/nexus-server/src/connectors.rs` (`check_connector_license`) | `validate_source_config`/`validate_sink_config`/`build_source`/`build_sink` checam a license ativa antes de salvar/rodar um pipeline com conector `requires_license`. | ✅ pronto |
| **Frontend** | `frontend/src/components/ConnectorPalette.tsx`, `Store.tsx` | Cadeado nos conectores sem license cobrindo + aba Store com status de license e form de instalação (Admin-only). | ✅ pronto |
| **Catálogo enterprise real** | repo privado `nexus-connectors-enterprise` | 24 crates de conector / 51 entradas no catálogo (25 OSS + 26 nomes enterprise). | ✅ pronto |

**O que ainda NÃO existe:**

- Serviço `nexus-licensing` (catálogo, checkout, webhook, emissão de license key) — hoje license de teste é assinada na mão (ver `docs/ENTERPRISE_LICENSING.md` "Próximos passos").
- Trial limitado de verdade — hoje um conector sem license dá pra configurar livremente no Canvas, só bloqueia no Salvar/Executar; não há teto de uso real (ver `ROADMAP.md`, follow-up do Bloco 1).

---

## 2. Decisões pendentes (precisam ser tomadas antes de codar)

### 2.1 A store inclui frontend de catálogo/checkout ou é só o serviço `nexus-licensing`?

| Opção | Descrição | Trade-off |
|---|---|---|
| **A — Serviço + portal web próprio** | `nexus-licensing` expõe uma SPA/React de catálogo/checkout; o usuário compra fora do NexusFlow e cola a key em `Admin > Licenças`. | Maior trabalho; UX de compra desacoplada; não polui o repo OSS com UI de pagamento. |
| **B — Apenas API + email** | `nexus-licensing` só tem API e webhook; checkout é feito via link do Mercado Pago enviado por email/CRM. | Menor trabalho; v1 alinhada com `docs/ENTERPRISE_LICENSING.md` §8 (portal self-service fora de escopo). |
| **C — Integração dentro do Canvas** | O próprio frontend do NexusFlow mostra conectores bloqueados com botão "Comprar", redirecionando pro checkout. | Melhor UX; exige tocar no repo OSS (apenas redirecionamento, não processamento de pagamento). |

**Recomendação prévia:** começar por **B** (API + email) pra validar o fluxo de pagamento, e depois adicionar **C** (indicação de bloqueado + link de compra no Canvas) sem mudar o serviço.

### 2.2 Qual mecanismo de entrega dos plugins enterprise?

| Opção | Descrição | Trade-off |
|---|---|---|
| **1 — Feature flag `enterprise` em build** | `nexus-connectors-enterprise` é crate privado referenciado como `git` privado no `Cargo.toml` de uma build enterprise. O conector é linkado estaticamente; sem a feature, não aparece no catálogo. | Simples; binário único; requer acesso ao repo privado no build; não permite "ativar" depois sem recompilar. |
| **2 — Dynamic loading (`cdylib`/`.so`/`.dll`)** | `nexus-server` carrega `.so` de conectores enterprise em runtime a partir de um diretório, se a license for válida. | Permite ativação pós-instalação; maior complexidade (ABI estável, unsafe, dlopen); precisa distribuir os `.so`. |
| **3 — Binário enterprise separado** | Dois binários: `nexusflow` (OSS) e `nexusflow-enterprise` (com conectores pagos linkados). | Distribuição simples; gate é "ter o binário certo" + license key; duplica artefatos de release. |

**Recomendação prévia:** começar por **1** (feature flag `enterprise` com crate privado via `git`), porque:
- É o padrão já descrito em `LICENSING.md` §2 e `ARCHITECTURE.md` §11.
- Não introduz complexidade de ABI/dynamic loading no MVP.
- A limitação "recompilar para adicionar conector" é aceitável enquanto não houver marketplace self-service.

### 2.3 Quer começar implementando já ou só o documento de planejamento por enquanto?

Esta decisão depende de:
- Repo privado `nexus-licensing` já criado? (precisa de acesso/admin da org)
- Conta do Mercado Pago configurada? (production + test credentials)
- Primeiro conector enterprise definido? (Snowflake, Databricks, Salesforce, Excel...)

**Recomendação prévia:** antes de escrever código do serviço de pagamento, fazer um **spike técnico** no repo OSS:
1. Criar um conector enterprise "fake" (ex.: `snowflake`) num crate privado de teste.
2. Wirear a feature `enterprise` no `nexus-server`.
3. Garantir que `GET /connectors` só exponha o conector com license válida.
4. Garantir que `POST /pipelines/{id}/run` rejeite specs com conector enterprise sem license.

Esse spike valida toda a cadeia técnica sem depender de Mercado Pago.

---

## 3. Arquitetura proposta

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Cliente / Canvas                                │
│  - Vê conectores enterprise marcados como bloqueados (licensed=false)        │
│  - Admin cola a license key em Configurações > Licenças                      │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │ REST (nexus-server OSS)
┌───────────────────────────────▼─────────────────────────────────────────────┐
│  nexus-server                                                                 │
│  - GET /connectors  → filtra licensed com base na license ativa              │
│  - POST /license    → valida JWT e salva no metadata store                   │
│  - GET /license/status                                                    │
│  - POST /pipelines/{id}/run → rejeita conectores enterprise não licenciados  │
│  - build_source/build_sink  → gate em runtime                                │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │ (em build com feature enterprise)
┌───────────────────────────────▼─────────────────────────────────────────────┐
│  nexus-connectors-enterprise (repo privado)                                   │
│  - Crates como nexus-connector-salesforce, nexus-connector-snowflake         │
│  - Usam submit_enterprise_connector!(...)                                    │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  nexus-licensing (repo privado, serviço separado)                             │
│  - Catálogo de produtos (POST /products, GET /products)                      │
│  - Checkout Mercado Pago (POST /checkout)                                    │
│  - Webhook /webhooks/mercadopago                                             │
│  - Emissão de license key (POST /admin/licenses)                             │
│  - Nota fiscal (NFe/NFSe) — NFE.io ou eNotas                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Fases de implementação

### Fase 0 — Spike técnico no repo OSS (sem pagamento)

**Objetivo:** validar o gate de licença de ponta a ponta.

- [ ] Criar repo/crate privado temporário `nexus-connector-enterprise-stub` com um conector fake `snowflake`.
- [ ] Adicionar feature `enterprise` no `nexus-server/Cargo.toml` que dependa do crate privado via `git`.
- [ ] Garantir que o stub use `submit_enterprise_connector!("snowflake", ...)`.
- [ ] Atualizar `connectors::validate_source_config`/`validate_sink_config` para rejeitar conectores enterprise não licenciados.
- [ ] Atualizar `connectors::build_source`/`build_sink` para o mesmo gate.
- [ ] Adicionar testes de integração: sem license → 403/400; com license → conector disponível.
- [ ] Documentar como gerar license key de teste (usar `license.rs::test_support` ou gerar via CLI temporário).

**Critério de pronto:** `cargo test -p nexus-server --features enterprise` passa e o conector fake só aparece/funciona com license válida.

### Fase 1 — Serviço `nexus-licensing` (repo privado)

**Objetivo:** API de catálogo, checkout e webhook.

- [ ] Criar repo `ailake-io/nexusflow-licensing` (Rust/Axum ou Node/Express — decisão técnica a tomar; Rust alinha com stack).
- [ ] Schema do banco Postgres (`products`, `customers`, `orders`, `licenses`).
- [ ] CRUD de produtos (`POST/GET /products`).
- [ ] Endpoint `POST /checkout` que cria preferência no Mercado Pago Checkout Pro.
- [ ] Webhook `POST /webhooks/mercadopago`:
  - Validar `x-signature`.
  - Buscar pagamento pelo ID na API do Mercado Pago.
  - Dedup por `payment_id`.
  - Criar `order` + `license`.
  - Disparar emissão de NFe/NFSe (NFE.io/eNotas).
  - Enviar email com license key.
- [ ] Endpoint `POST /admin/licenses` para emitir license key manualmente (suporte interno).
- [ ] Gerenciamento de chaves Ed25519 (`kid`, rotação).

**Critério de pronto:** fluxo de pagamento de teste no Mercado Pago Sandbox gera uma license key JWT válida.

### Fase 2 — Integração frontend (indicação de bloqueado)

**Objetivo:** o Canvas refletir o campo `licensed` do `GET /connectors`.

- [ ] Adicionar `licensed: boolean` em `ConnectorDescriptor` (`frontend/src/lib/api.ts`).
- [ ] Em `ConnectorPalette.tsx`, desabilitar drag de conectores não licenciados e mostrar ícone de cadeado/upgrade.
- [ ] Adicionar tooltip/link para fluxo de compra (pode ser link externo pra `nexus-licensing` no v1).
- [ ] Criar tela `LicensePanel` (Admin) para instalar/visualizar license key.
- [ ] Adicionar strings de i18n (PT/EN) para estados bloqueado/liberado.

**Critério de pronto:** conector enterprise aparece no canvas como bloqueado até a license ser instalada.

### Fase 3 — Primeiro conector enterprise real

**Objetivo:** ter um conector pago funcional.

- [ ] Decidir primeiro conector (recomendação: **Excel** por alto volume/PME ou **Salesforce** por ticket enterprise — ver `docs/ENTERPRISE_CONNECTORS.md` §Priorização).
- [ ] Implementar no repo privado `nexus-connectors-enterprise`.
- [ ] Wirear no `nexus-server` via feature `enterprise`.
- [ ] Testes de integração no repo privado (não no OSS).
- [ ] Documentar pré-requisitos e limitações.

**Critério de pronto:** cliente consegue comprar, instalar a key e usar o conector no Canvas.

### Fase 4 — Self-service e assinatura (pós-v1)

- [ ] Portal do cliente (ver licenças, renovar, baixar binário enterprise).
- [ ] Assinatura recorrente (Preapproval API do Mercado Pago).
- [ ] Revogação ativa online (phone-home) — opcional, fora de escopo v1.

---

## 5. Detalhes técnicos

### 5.1 Schema do banco (`nexus-licensing`)

```sql
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connector_slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT,
    price_cents INTEGER NOT NULL,
    currency CHAR(3) DEFAULT 'BRL',
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE customers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL UNIQUE,
    mp_customer_id TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customers(id),
    product_ids UUID[] NOT NULL,
    mp_payment_id TEXT UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected')),
    total_cents INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE licenses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customers(id),
    connector_slugs TEXT[] NOT NULL,
    seats INTEGER NOT NULL DEFAULT 1,
    issued_at TIMESTAMPTZ DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    jwt_kid TEXT NOT NULL
);
```

### 5.2 APIs do `nexus-licensing`

| Método | Rota | Descrição | Auth |
|---|---|---|---|
| GET | `/products` | Lista produtos ativos. | Pública |
| POST | `/checkout` | Cria preferência no Mercado Pago. Retorna `init_point`. | Bearer (cliente autenticado) ou anônima com `email` |
| POST | `/webhooks/mercadopago` | Recebe notificação de pagamento. | Validação `x-signature` |
| POST | `/admin/licenses` | Emite license key manualmente. | API key interna |
| GET | `/admin/licenses/{id}` | Consulta license. | API key interna |

### 5.3 Gate em runtime no `nexus-server`

Hoje o gate só existe no catálogo (`GET /connectors`). Precisa ser replicado em:

1. **`connectors::validate_source_config` e `validate_sink_config`** — rejeitar na criação/edição do pipeline.
2. **`connectors::build_source` e `build_sink`** — rejeitar na execução (defesa em profundidade).
3. **Preview (`GET /pipelines/{id}/preview`)** — rejeitar se o node for enterprise não licenciado.

Implementação sugerida (função helper em `connectors.rs`):

```rust
use nexus_core::ConnectorRegistry;

fn ensure_licensed(
    connector: &str,
    license_store: &LicenseStore,
) -> anyhow::Result<()> {
    let Some(desc) = ConnectorRegistry::find(connector) else {
        anyhow::bail!("unsupported connector: {connector:?}");
    };
    if let Some(slug) = desc.requires_license {
        // Precisamos de acesso síncrono/async à license aqui.
        // Opção A: receber Option<LicenseClaims> pré-carregada.
        // Opção B: tornar a função async.
    }
    Ok(())
}
```

**Observação:** como `validate_source_config` é síncrona e `LicenseStore::active` é async, o gate em validação deve receber a `LicenseClaims` já carregada pelo handler (que chama `license_store.active().await` uma vez).

### 5.4 Mecanismo de entrega: feature flag `enterprise`

Exemplo de como ficaria `nexus-server/Cargo.toml`:

```toml
[features]
enterprise = ["dep:nexus-connectors-enterprise"]

[dependencies]
nexus-connectors-enterprise = { git = "ssh://git@github.com/ailake-io/nexusflow-connectors-enterprise.git", optional = true }
```

No crate privado:

```rust
// nexus-connector-salesforce/src/lib.rs
nexus_core::submit_enterprise_connector!(
    "salesforce",
    ConnectorCapability::Bridged,
    SalesforceConnectorConfig
);
```

O crate privado deve ter sua própria CI, nunca ser referenciado por `path` no workspace OSS.

---

## 6. Integração frontend

### 6.1 API type

```typescript
// frontend/src/lib/api.ts
export interface ConnectorDescriptor {
  name: string
  capability: ConnectorCapability
  config_schema: ConnectorConfigSchema
  licensed: boolean
}
```

### 6.2 Palette

```tsx
// ConnectorPalette.tsx
{connectors.map((c) => (
  <div
    key={c.name}
    draggable={c.licensed}
    className={cn(
      'group flex items-center gap-2.5 rounded-lg border px-3 py-2 text-sm',
      c.licensed
        ? 'cursor-grab hover:border-primary/30 hover:bg-primary/5'
        : 'cursor-not-allowed opacity-60'
    )}
  >
    <Database className="h-3.5 w-3.5" />
    <span>{c.name}</span>
    {!c.licensed && <Lock className="ml-auto h-3 w-3 text-muted-foreground" />}
  </div>
))}
```

### 6.3 Tela de licença

Nova rota `/admin/license` (visível só pra Admin):
- Campo textarea para colar a license key.
- Botão "Ativar" chamando `POST /license`.
- Card mostrando status, conectores liberados e validade (`GET /license/status`).

---

## 7. Riscos e mitigações

| Risco | Impacto | Mitigação |
|---|---|---|
| Chave pública de teste vaza no repo OSS | Licenças falsas validam no binário OSS | Substituir `LICENSE_PUBLIC_KEY_PEM` antes de emitir license real; manter test key só em `#[cfg(test)]`. |
| Dynamic loading adiado vira gargalo | V1 não permite ativação pós-instalação | Aceitar para v1; documentar que troca de conector exige novo binário. |
| Mercado Pago webhook forjado | Emissão indevida de license | Sempre buscar pagamento na API MP + validar `x-signature`; dedup por `payment_id`. |
| Repo privado sem CI | Builds enterprise não reproduzíveis | Criar CI separada no repo privado, nunca depender do repo OSS pra buildar enterprise. |
| License key vazada por cliente | Uso não autorizado | v1 offline-only com `exp` curto; v2 phone-home opcional. |

---

## 8. Próximos passos validados

> Decisão arquitetural recomendada para **rapidez de entrega + facilidade de manutenção**:
> **feature flag `enterprise` + crate privado via `git` + binário/imagem Docker enterprise separada**.
> Dynamic loading e self-service de ativação ficam para v2.

### 8.1 Ordem de execução recomendada

1. **Fase 0 — Spike técnico no OSS (1 semana, 1 dev)**
   - Criar repo privado temporário com conector enterprise fake (`snowflake`).
   - Adicionar feature `enterprise` no `nexus-server/Cargo.toml` (dependência `git` opcional).
   - Implementar gate em runtime nos handlers de validação e execução.
   - Testes: sem license → rejeitado; com license → conector disponível e executável.
   - **Não depende de Mercado Pago nem de primeiro conector real.**

2. **Fase 1 — Serviço `nexus-licensing` (2 semanas)**
   - Criar repo privado `nexusflow-licensing` (Rust/Axum + Postgres).
   - Catálogo de produtos, checkout Mercado Pago Pro, webhook com `x-signature`, emissão de license key.
   - Teste end-to-end em Sandbox.

3. **Fase 2 — Integração frontend (1 semana)**
   - Consumir campo `licensed` no `ConnectorDescriptor`.
   - Bloquear drag de conectores não licenciados, mostrar cadeado/tooltip.
   - Criar tela `LicensePanel` para Admin instalar/visualizar license key.

4. **Fase 3 — Primeiro conector real (2–3 semanas)**
   - Escolher primeiro conector: **Excel** (rápido, baixo risco, alto volume) ou **Salesforce** (ticket enterprise).
   - Implementar em `nexusflow-connectors-enterprise`.
   - Documentar, testar e publicar binário enterprise.

5. **Fase 4 — Evolução (pós-v1)**
   - Self-service no Canvas (botão "Comprar" linkando checkout).
   - Assinatura recorrente (Mercado Pago Preapproval).
   - Dynamic loading ou download de plugin, se houver demanda confirmada.

### 8.2 Entrega ao cliente

- Binário `.deb`/`.rpm`/tarball enterprise ou imagem Docker `nexusflow:enterprise`.
- O cliente instala o binário, cola a license key em `Configurações > Licenças` e o conector aparece no Canvas.
- V1 não permite ativar plugin sem reinstalar — isso é documentado e aceitável enquanto não houver marketplace self-service.

### 8.3 Cuidados de manutenção

- Nunca referenciar crate privado por `path` no workspace OSS.
- Substituir `LICENSE_PUBLIC_KEY_PEM` de teste antes de emitir licenses reais.
- Manter CI separada no repo privado.
- Alinhar versão do `nexus-core` entre repo OSS e repo privado.

---

## 9. Referências

- `LICENSING.md` — modelo open-core e regras de separação OSS/enterprise.
- `docs/ENTERPRISE_LICENSING.md` — fluxo de pagamento Mercado Pago e design da license key.
- `docs/ENTERPRISE_CONNECTORS.md` — candidatos a conector pago e priorização.
- `ARCHITECTURE.md` §11 — distribuição de conectores enterprise.
- `ROADMAP.md` Fase 12 — item de roadmap correspondente.
