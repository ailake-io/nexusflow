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

## Limitação real: sem conectores enterprise

Diferente do tarball Linux e do `.msi` Windows, o binário macOS **não**
inclui os conectores enterprise. Os dois outros caminhos contornam a
ausência de `docker` no runner (Linux usa `docker build` direto; Windows
usa um `[patch]` de Cargo pra evitar precisar de Docker) — macOS por ora
só builda o binário OSS puro (`cargo build --release --features
embed-ui,connectors-all`). Estender pra enterprise depois é o mesmo truque
do Windows (clonar o repo privado + `[patch]` apontando pro checkout OSS
já baixado), não implementado ainda.

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
