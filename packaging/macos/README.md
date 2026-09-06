# NexusFlow no macOS

Adicionado 2026-09-05 (`release.yml`'s `build` job, matrix leg `macos`/`arm64`,
`runs-on: macos-latest`). **Validado num run real em 2026-09-06**: tanto o
build OSS puro (`release.yml`) quanto o enterprise-bundled
(`build-macos-installer.yml`) compilaram e publicaram um tarball real na
release v0.1.3.

## O que tem aqui

- `nexusflow.rb` — formula Homebrew, `sha256` real (calculado direto do
  tarball baixado da release v0.1.3, não do `SHA256SUMS` de `release.yml`
  — esse arquivo é gerado antes do `--clobber` de
  `build-macos-installer.yml` e fica desatualizado pra esse asset
  específico).

## Dois caminhos de build — igual ao Windows

`release.yml`'s leg `macos`/`arm64` (automático, a cada release) builda
só o binário **OSS puro** (`connectors-all`, sem enterprise) — é o que
entra na chain automática pra não arriscar o release principal com um
build ainda não validado (mesmo racional do Windows já ter um workflow
separado do zero).

`.github/workflows/build-macos-installer.yml` (`workflow_dispatch`
manual) builda o binário **enterprise-bundled** completo — mesmo truque
do Windows (clonar `nexus-connectors-enterprise` + `[patch]` de Cargo
apontando pro checkout OSS já baixado, sem precisar de Docker, que não
existe em runner macOS hospedado). Rodá-lo com `-f version=vX.Y.Z`
**sobrescreve** o `nexusflow-macos-arm64.tar.gz` que o release automático
já publicou (mesmo nome de arquivo, `--clobber`) — resultado final: o
asset publicado na release passa a ser o enterprise-bundled, igual ao
que Linux/Windows já entregam.

Bug real encontrado e corrigido no primeiro run: faltava o step `cargo
build --release -p nexusflow` (builda o OSS puro) antes do step que
builda o binário enterprise — sem ele, `target/release/` nunca existia
na raiz do repo pro `cp` final copiar o binário enterprise por cima.
Corrigido espelhando `build-windows-installer.yml`, que já fazia isso.

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
