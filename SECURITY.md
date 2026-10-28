# Política de Segurança

## Reportando uma vulnerabilidade

**Não abra uma issue pública para vulnerabilidades de segurança.**

Use o **GitHub Private Vulnerability Reporting** deste repositório: aba
[**Security** → **Report a vulnerability**](../../security/advisories/new).
O reporte é privado entre você e os mantenedores até uma correção estar
pronta.

Inclua, quando possível:
- Versão/commit afetado.
- Passos para reproduzir (ou PoC).
- Impacto esperado (o que um atacante ganha).

## O que está no escopo

- `nexus-core`, `nexus-server`, `nexus-ai`, os conectores em
  `crates/nexus-connectors/` e o frontend (`frontend/`) — este repositório.
- Vulnerabilidades em dependências de terceiros usadas por este repo
  (reporte aqui primeiro; nós avaliamos se cabe reportar upstream também).

## Tempo de resposta

Sem SLA formal (projeto mantido fora de horário comercial dedicado) —
confirmação de recebimento em até alguns dias úteis. Severidade alta
(RCE, bypass de auth/RBAC, vazamento de credencial de conector) tem
prioridade sobre o resto do backlog.

## Versões suportadas

Sem branches de LTS — só o binário mais recente publicado
(`releases` deste repositório) recebe correção de segurança. Não há
backport para versões antigas.

## Riscos conhecidos e aceitos (transparência)

Já auditados e aceitos deliberadamente — não é necessário reportar de
novo, mas releituras/segundas opiniões são bem-vindas:

- **`RUSTSEC-2023-0071`** (`rsa`, via `jsonwebtoken`'s RS256) — sem
  correção disponível upstream.
- **`RUSTSEC-2026-0194`/`-0195`** (`quick-xml`, via `object_store`/
  `datafusion`) — atrelado ao pin de `arrow` 58.x, ver `ROADMAP.md`
  "Débitos conhecidos".
- **`RUSTSEC-2025-0009`/`RUSTSEC-2024-0336`** (`ring`/`rustls`, via
  `milvus-sdk-rust`'s `tonic` 0.8.3) — sem release mais nova do SDK.

O alerta **`GHSA-2f9f-gq7v-9h6m` / `CVE-2026-43868`** (Apache Thrift) foi
corrigido com um backport local em `vendor/thrift`: tanto o Parquet 52.2
quanto o 58.4 ainda exigem `thrift ^0.17`, enquanto a correção upstream só
está publicada a partir de 0.23. O backport mantém a API 0.17 e rejeita
varints Compact acima dos limites do protocolo antes de convertê-los em
tamanhos de alocação; há testes unitários para os limites de 32 e 64 bits.

Detalhe técnico completo de cada um: `ROADMAP.md`, seção "Débitos
conhecidos".

## Segredos e credenciais

Nunca commite `NEXUS_JWT_SECRET`, `NEXUS_ENCRYPTION_KEY`, credenciais de
conector ou qualquer chave privada neste repositório. Segredos
sempre via variável de ambiente — ver `docs/GETTING_STARTED.md` §3.
