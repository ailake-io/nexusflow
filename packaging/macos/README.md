# NexusFlow no macOS

Adicionado 2026-09-05 (`release.yml`'s `build` job, matrix leg `macos`/`arm64`,
`runs-on: macos-latest`). **Validado num run real em 2026-09-06**: tanto
`release.yml` quanto `build-macos-installer.yml` compilaram e publicaram
um tarball real na release v0.1.3.

## O que tem aqui

- `nexusflow.rb` — formula Homebrew, `sha256` real (calculado direto do
  tarball baixado da release v0.1.3, não do `SHA256SUMS` de `release.yml`
  — esse arquivo é gerado antes do `--clobber` de
  `build-macos-installer.yml` e fica desatualizado pra esse asset
  específico).

## Dois workflows, mesmo binário

`release.yml`'s leg `macos`/`arm64` (automático, a cada release) e
`.github/workflows/build-macos-installer.yml` (dispara sozinho via
`workflow_run` em `Release`, branch `main`, além do `workflow_dispatch`
manual) buildam o mesmo binário — `--features embed-ui,connectors-all`,
todo conector e toda feature, tudo OSS desde a Fase 35. O segundo
sobrescreve o `nexusflow-macos-arm64.tar.gz` publicado pelo primeiro
(mesmo nome de arquivo, `--clobber`) — é o asset que a formula usa.

## Arquitetura suportada

Só Apple Silicon (`arm64`) — `macos-latest` da GitHub hoje é Apple
Silicon. Sem build Intel (`x86_64`).

## Drivers ADBC

`scripts/build-adbc-postgresql-driver.sh`/`build-adbc-sqlite-driver.sh`/
`build-adbc-duckdb-driver.sh`/`build-adbc-clickhouse-driver.sh` têm
suporte a macOS (detecção de `.dylib` em vez de `.so`, `sysctl -n
hw.ncpu` em vez de `nproc`). Os 4 vêm empacotados no tarball; a formula
Homebrew instala todos (`Dir["libadbc_*"]` — cuidado, o driver
ClickHouse se chama `libadbc_clickhouse.dylib`, sem `driver_` no nome,
diferente dos outros 3).

## Instalar

```bash
brew install --formula https://raw.githubusercontent.com/ailake-io/nexusflow/main/packaging/macos/nexusflow.rb
```

Tap dedicado (`ailake-io/homebrew-nexusflow`) é trabalho futuro — ver
comentário no topo de `nexusflow.rb`.
