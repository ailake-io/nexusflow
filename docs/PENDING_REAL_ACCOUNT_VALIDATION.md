# Validação contra conta/tenant real — checklist

Todos os conectores listados aqui já têm o fluxo inteiro coberto por
teste `wiremock` (ver `tests/*.rs` de cada crate) — o gap não é "será que
funciona", é "confirmar contra o serviço de verdade" (formato exato de
resposta, paginação, envelope de erro). Ver
`docs/CONNECTOR_FIELD_REFERENCE.md` pra campos de config completos.

Marque `[x]` conforme for resolvendo. Ordem sugerida: menor fricção
primeiro.

> **2026-10-28 (Fase 35)**: estes conectores migraram do antigo repo
> privado pro OSS e entram em `connectors-all` — inclusive os 5 de ads
> (Google/Meta/LinkedIn/TikTok/X), por decisão explícita, mesmo sem
> validação real ainda. Este checklist continua valendo como pendência
> de validação, não como bloqueio de build.

## Baixa fricção (conta grátis, sem aprovação de terceiro)

- [ ] **Google Ads** — [ads.google.com](https://ads.google.com) → conta
      de anúncios (sandbox serve) → developer token tier **Explorer**
      (auto-aprovado, sem fila) → OAuth2 client em
      [console.cloud.google.com](https://console.cloud.google.com) →
      `refresh_token` via OAuth playground.
- [ ] **GA4** — Google Cloud Console → habilitar **Analytics Data API**
      → criar service account → copiar `client_email`/`private_key` →
      no Admin do GA4, adicionar o service account como usuário
      (Viewer) na property.
- [ ] **YouTube Analytics** — Google Cloud Console → habilitar **YouTube
      Analytics API** → OAuth2 client → dono do canal autoriza o
      consentimento (`yt-analytics.readonly`) uma vez, gera
      `refresh_token`. Sem aprovação tipo MDP — funciona pra qualquer
      client OAuth2.
- [ ] **Meta Ads** — caminho mais leve: [developers.facebook.com](https://developers.facebook.com)
      → criar App (modo Development, sem review) → Marketing API →
      **Test Ad Account** (gerado direto pela ferramenta de dev, sem
      Business Manager real nem cartão — é sandbox oficial da Graph
      API, aceita criar campanha/ad set/ad de teste, dados fake). Token
      de teste gerado no próprio painel do App. Alternativa mais pesada
      (se quiser dados reais de conta): Business Manager real + System
      User + token `ads_read`.
- [ ] **Stripe** — [dashboard.stripe.com](https://dashboard.stripe.com)
      → modo teste (sempre disponível, grátis) → copiar `sk_test_...`.
      Mais barato de validar de todo o catálogo.
- [ ] **HubSpot** — conta developer grátis
      ([developers.hubspot.com](https://developers.hubspot.com)) → test
      account → Settings → Private Apps → criar app, copiar
      `pat-...`.
- [ ] **Shopify** — [partners.shopify.com](https://partners.shopify.com)
      → conta Partner grátis → criar **development store** → apps →
      custom app → gerar `access_token` (Admin API).
- [ ] **Zendesk** — trial grátis (zendesk.com) → Admin Center → Apps and
      integrations → API tokens → gerar token, usar com
      `{email}/token:{token}`.
- [ ] **ServiceNow** — [developer.servicenow.com](https://developer.servicenow.com)
      → **Personal Developer Instance** (grátis, provisiona uma
      instância própria) → usuário admin já vem pronto (Basic auth).
- [ ] **Dynamics 365** / **SharePoint** — [developer.microsoft.com/microsoft-365/dev-program](https://developer.microsoft.com/microsoft-365/dev-program)
      → sandbox tenant Microsoft 365 grátis (E5, 90 dias renovável) →
      Azure AD → registrar app → permissões `Dynamics CRM`/`Sites.ReadWrite.All`
      → gerar `access_token`.
- [ ] **Google Sheets** / **Google Drive** — mesmo projeto GCP do
      GA4/Google Ads acima → habilitar Sheets API / Drive API → OAuth2
      client → token via playground (mesma conta Google já usada nos
      outros).
- [ ] **Dropbox** — [dropbox.com/developers](https://dropbox.com/developers)
      → conta grátis → criar app → gerar access token direto no painel
      (sem fluxo OAuth completo pra teste pontual).

## Fricção média (precisa pedir aprovação, mas não discricionária)

- [ ] **TikTok Ads** — [business-api.tiktok.com](https://business-api.tiktok.com)
      → conta TikTok for Business → criar app → **App Review**
      self-serve, 2–7 dias úteis (não é aprovação discricionária, é
      fila).
- [ ] **X/Twitter Ads** — [developer.x.com](https://developer.x.com) →
      criar app → solicitar acesso **Standard/Conversion-Only** →
      gerar `consumer_key`/`consumer_secret`/`access_token`/
      `access_token_secret` (OAuth 1.0a).

## Alta fricção / fora do controle direto

- [ ] **LinkedIn Ads** — token obtido manualmente (OAuth 3-legged via
      navegador) funciona pra um teste pontual, mas **expira em 60 dias
      sem renovação automática** — isso só existe com aprovação
      **Marketing Developer Platform (MDP)**, revisão discricionária da
      LinkedIn (semanas a meses). Sem MDP aprovado, é reautenticação
      manual toda vez. Pior caso do lote.
- [ ] **NetSuite** — sem signup público — precisa de conta NetSuite de
      cliente/parceiro existente com sandbox habilitado. Se não tiver
      uma à mão, só via parceria/revenda.
- [ ] **Workday** — mesma situação do NetSuite: sem trial público,
      precisa de tenant de cliente com um Integration System User (ISU)
      provisionado e um report RaaS publicado.

## Fora deste checklist (já validado, ou não é "conta")

- **BigQuery** — já testado contra conta/projeto GCP real.
- **MSSQL**/**mssql-cdc**/**synapse** — já validado manualmente contra
  container real (não é conta de terceiro, é instância própria).
- Oracle, SAP HANA, Teradata, Vertica, Snowflake, Databricks, Redshift,
  Pulsar, Kinesis, Starburst — banco/serviço próprio, não conta
  OAuth de terceiro — fora do escopo deste checklist.
