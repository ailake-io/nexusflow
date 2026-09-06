# NexusFlow no macOS

Adicionado 2026-09-05 (`release.yml`'s `build` job, matrix leg `macos`/`arm64`,
`runs-on: macos-latest`) — **ainda não validado num Mac real**, nem essa
combinação CI já rodou de ponta a ponta com sucesso confirmado. Trate como
best-effort até uma release real passar por aqui.

## O que tem aqui

- `nexusflow.rb` — formula Homebrew. **Checksum placeholder** — só instala
  de verdade depois que uma release real publicar
  `nexusflow-macos-arm64.tar.gz` e o `sha256` real for copiado do
  `SHA256SUMS` daquela release.

## Dois caminhos de build — igual ao Windows

`release.yml`'s leg `macos`/`arm64` (automático, a cada release) builda
só o binário **OSS puro** (`connectors-all`, sem enterprise) — é o que
entra na chain automática pra não arriscar o release principal com um
build ainda não validado (mesmo racional do Windows já ter um workflow
separado do zero).

`.github/workflows/build-macos-installer.yml` (novo, `workflow_dispatch`
manual, 2026-09-06) builda o binário **enterprise-bundled** completo —
mesmo truque do Windows (clonar `nexus-connectors-enterprise` + `[patch]`
de Cargo apontando pro checkout OSS já baixado, sem precisar de Docker,
que não existe em runner macOS hospedado). Roda-lo com `-f
version=vX.Y.Z` **sobrescreve** o `nexusflow-macos-arm64.tar.gz` que o
release automático já publicou (mesmo nome de arquivo, `--clobber`) —
resultado final: o asset publicado na release passa a ser o
enterprise-bundled, igual ao que Linux/Windows já entregam. **Ainda não
validado em execução real** — primeira vez que macOS + `connectors-all`
+ todo conector enterprise (vários linkam `odbc-api` contra libpq/
unixodbc) é tentado.

## Arquitetura suportada

Só Apple Silicon (`arm64`) — `macos-latest` da GitHub hoje é Apple
Silicon. Sem build Intel (`x86_64`).

## Drivers ADBC

`scripts/build-adbc-postgresql-driver.sh`/`build-adbc-sqlite-driver.sh`
ganharam suporte a macOS (detecção de `.dylib` em vez de `.so`, `sysctl
-n hw.ncpu` em vez de `nproc`) na mesma leva — também sem validação em
hardware real.

## Instalar (uma vez que o checksum real existir)

```bash
brew install --formula https://raw.githubusercontent.com/ailake-io/nexusflow/main/packaging/macos/nexusflow.rb
```

Tap dedicado (`ailake-io/homebrew-nexusflow`) é trabalho futuro — ver
comentário no topo de `nexusflow.rb`.
