# Análise da branch `develop` — Bugs e pontos a melhorar

Análise read-only da branch `develop` consolidando achados de backend, frontend, CI/docs e segurança.

## 1. Resumo executivo

A branch `develop` está funcionalmente estável (CI verde, clippy limpo), mas ainda acumula débitos técnicos e riscos de segurança que não foram cobertos pelas fases anteriores do `REVIEW_ACTION_PLAN.md`. Os pontos mais críticos estão em:

- **Segurança:** bypass de path validation via `file://`, SSRF protocol-relative, JWT sem `aud`/`iss`.
- **Robustez:** panics no data path (`nexus-ai`, Pinecone), `Mutex` síncrono em async, tasks fire-and-forget.
- **Qualidade:** poucos testes no frontend, docs desatualizadas, Actions com Node.js 20 deprecated.

---

## 2. Achados críticos

### 2.1 Segurança

| # | Problema | Onde | Impacto | Ação recomendada |
|---|---|---|---|---|
| SEC-1 | Path traversal via `file://` no CSV e outros conectores locais | `crates/nexus-core/src/dag.rs:496-524` + `nexus-connector-csv/src/store.rs:21-27` | `file:///etc/passwd` não começa com `/` e passa pela validação; `http_host` retorna `None`, então conectores locais podem ler/escrever arquivos arbitrários | Rejeitar explicitamente URIs com scheme `file://` na validação de segurança do DAG |
| SEC-2 | SSRF protocol-relative (`//host/path`) no REST/webhook | `nexus-connector-rest/src/source.rs:73-77` | `base_url` externo + `path = "//169.254.169.254/..."` pode ser resolvido para host interno | Normalizar URL final, rejeitar componentes `//host`, desabilitar redirects automáticos ou revalidar destino |
| SEC-3 | JWT de auth não valida `aud`/`iss` | `nexus-server/src/auth.rs:97` | Tokens de outros serviços com mesmo `NEXUS_JWT_SECRET` seriam aceitos | Definir e validar `aud`/`iss` fixos no `issue`/`verify` |
| SEC-4 | `NodeSpec.name` usado em SQL sem `validate_identifier` | `nexus-core/src/dag.rs:20-25` | Permite injeção de SQL no transform | Aplicar `validate_identifier` em `resolved_name()` |
| SEC-5 | `pipeline_id` sem restrição de caracteres/comprimento | `nexus-core/src/dag.rs:196-199` | IDs longos/com caracteres especiais usados em SQL/URLs/logs | Validar `[A-Za-z0-9_-]{1,128}` |
| SEC-6 | `sanitize_error` não remove credenciais de query strings | `nexus-server/src/error.rs:100-132` | Tokens e paths em query params vazam no response | Usar `url::Url` para sanitizar query params |
| SEC-7 | Log de erro bruto em `record_run_failure` | `nexus-server/src/lib.rs:555` | `tracing::error!(error = format!("{error:?}"))` grava URI com credenciais antes da sanitização | Sanitizar antes de logar |

### 2.2 Robustez / bugs

| # | Problema | Onde | Impacto | Ação recomendada |
|---|---|---|---|---|
| ROB-1 | `panic!` no data path do embedding | `nexus-ai/src/embedding/pipeline.rs:318` | Coluna Arrow não suportada derruba o processo | Retornar `NexusError` em vez de panic |
| ROB-2 | `unwrap()` em downcasts no Pinecone | `nexus-connector-pinecone/src/rows.rs:119,133` | Schema inesperado → panic | Retornar erro |
| ROB-3 | `std::sync::Mutex` em async no `ProgressHub` | `nexus-server/src/progress.rs:15,23,30,36` | Lock pode bloquear executor Tokio | Migrar para `tokio::sync::Mutex` |
| ROB-4 | Alertas fire-and-forget | `nexus-server/src/alerts.rs:100-109,160-172` | Falhas silenciosas e possível vazamento de tasks | Observar `JoinHandle` e logar falhas |
| ROB-5 | Checkpoint parcial sem atomicidade | `nexus-server/src/runner.rs:105-112,190-205` | Falha após commit parcial deixa pipeline em estado inconsistente | Documentar limitação ou mover checkpoint para o fim de todas as partições |
| ROB-6 | Chave pública de license hardcoded | `nexus-server/src/license.rs:22-24` | Build enterprise aceita JWT auto-assinado por padrão | Carregar chave de env var e falhar se ausente |
| ROB-7 | `dbt` executa binário do PATH sem verificação | `nexus-server/src/dbt.rs:204-220` | Risco de executar binário malicioso | Validar caminho absoluto/assinatura ou documentar risco |

---

## 3. Achados de frontend

### Crítico

- **Timeout não limpo no desmonte:** `frontend/src/hooks/useRunProgress.ts:101-109` — `setTimeout` recursivo não é cancelado em `cleanupRun`.
- **JSON.parse sem try/catch no WebSocket:** `frontend/src/hooks/useRunProgress.ts:120` — frame malformado quebra o handler.
- **JWT em `sessionStorage`:** `frontend/src/lib/auth.tsx:11` + `api.ts:167-170` — vulnerável a XSS; idealmente migrar para cookie `HttpOnly`.
- **Importação de JSON sem validação:** `frontend/src/components/DagCanvas.tsx:226-229` — `handleImport` aplica payload sem schema.

### Alto

- IDs globais causam colisões: `frontend/src/lib/dag.ts:42,323` + `DagCanvas.tsx:92`.
- `SchemaForm` permite `NaN`: `frontend/src/components/SchemaForm.tsx:111`.
- `ConnectorPalette` inacessível por teclado.
- `FieldHint` sem `aria-describedby` e sem dismiss.
- Sem Error Boundary: `frontend/src/main.tsx:8-15`.
- Polling não pausa em background: `frontend/src/components/PipelineStatusBoard.tsx:52-55`.
- `radix-ui` e `shadcn` em `dependencies`: `frontend/package.json:20,23`.

---

## 4. Achados de CI/docs

### Crítico

- **CI não dispara em `pull_request`:** `.github/workflows/ci.yml:3-6` — código pode ser mergeado sem gates.
- **Actions sem pin por SHA:** `.github/workflows/release.yml:91` (`actions/upload-artifact@v4`) ainda usa tag flutuante.
- **Imagens base do Dockerfile com tags flutuantes:** `Dockerfile:17,26,37,46,85`.
- **ADBC drivers clonam `main` sem tag fixa:** `scripts/build-adbc-postgresql-driver.sh:39`, `scripts/build-adbc-sqlite-driver.sh:37`.

### Alto

- Node.js 20 deprecated warnings em todas as Actions.
- Self-hosted runners sem isolamento para PRs.
- Docker multi-arch usa QEMU para `linux/arm64` (lento).
- `README.md` afirma não haver release taggeado, mas `release.yml` publica tags `v*`.
- Ausência de `.github/dependabot.yml`.
- Ausência de workflow CodeQL.
- `install.sh` prossegue se `SHA256SUMS` falhar; parse de JSON com `grep`/`sed`.

---

## 5. Plano de ação sugerido

### Semana 1 — Segurança crítica
1. SEC-1: rejeitar `file://` na validação do DAG.
2. SEC-2: normalizar URL final no REST e rejeitar `//host`.
3. SEC-3: adicionar `aud`/`iss` no JWT de auth.
4. SEC-4 + SEC-5: validar `NodeSpec.name` e `pipeline_id`.
5. SEC-6 + SEC-7: melhorar `sanitize_error` e log sanitizado.

### Semana 2 — Robustez
6. ROB-1 + ROB-2: eliminar panics em `nexus-ai` e Pinecone.
7. ROB-3: `tokio::sync::Mutex` no `ProgressHub`.
8. ROB-4: observar `JoinHandle`s dos alertas.
9. ROB-6: chave pública de license via env var.
10. ROB-7: validar binário `dbt`.

### Semana 3 — Frontend
11. Limpar timeouts e JSON.parse no `useRunProgress`.
12. Validar schema de importação no `DagCanvas`.
13. Error Boundary, acessibilidade, polling com `visibilityState`.
14. Ajustar dependências do `package.json`.
15. Adicionar testes de comportamento.

### Semana 4 — CI/docs
16. Restaurar trigger `pull_request` em CI com runner isolado.
17. Atualizar Actions para versões sem Node.js 20 deprecated.
18. Pinar imagens base do Dockerfile por digest.
19. Fixar `ADBC_REF` e validar checksum.
20. Criar `dependabot.yml` e workflow CodeQL.
21. Corrigir divergências docs vs código.

---

## 6. Métricas de sucesso

- Zero achados críticos de segurança abertos por mais de 7 dias.
- CI sem warnings de Node.js 20.
- Frontend com pelo menos 4 suites de testes rodando no CI.
- Code scanning e secret scanning habilitados.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` continua passando.
