# NexusFlow no winget

Manifests do NexusFlow no formato que o repositório comunitário
[microsoft/winget-pkgs](https://github.com/microsoft/winget-pkgs) espera
(`manifests/<primeira-letra-minúscula>/<Publisher>/<Package>/<Versão>/`).
`winget install` só funciona depois que esses arquivos forem aceitos via
PR nesse repo — winget não lê nada direto do GitHub do NexusFlow, então
publicar aqui é só o primeiro passo, não o install em si.

`PackageIdentifier: Ailake.NexusFlow` — publisher escolhido a partir da
org `ailake-io`; pode ser trocado antes do primeiro submit se preferir
outro nome, mas depois de aceito pelo winget-pkgs o identifier fica
praticamente fixo (mudar exige depreciar o pacote antigo).

## Os 3 arquivos por versão

- `Ailake.NexusFlow.yaml` — manifest de versão (aponta pro locale padrão).
- `Ailake.NexusFlow.installer.yaml` — URL do `.msi`, sha256, ProductCode.
- `Ailake.NexusFlow.locale.en-US.yaml` — descrição, tags, licença.

## Campos que mudam a cada release — regenerar sempre

`main.wxs` usa `<Product Id='*'>`, ou seja, o **ProductCode é aleatório
a cada build** (só o `UpgradeCode` é fixo, é assim que o WiX faz upgrade
funcionar corretamente — não é bug). Isso significa que
`InstallerSha256` e `ProductCode` no `installer.yaml` são válidos
**somente para a v0.1.3** e precisam ser extraídos de novo a cada
release nova:

```bash
# sha256 do .msi real (não confiar no SHA256SUMS de release.yml —
# aquele arquivo não inclui o .msi, que é publicado só por
# build-windows-installer.yml)
gh release download vX.Y.Z --repo ailake-io/nexusflow --pattern "*.msi" -O nexusflow.msi
sha256sum nexusflow.msi

# ProductCode real (msitools' msiinfo — `brew install msitools` ou
# `apt install msitools`; olefile por si só não dá pra parsear a
# Property table do MSI, só o SummaryInformation)
msiinfo export nexusflow.msi Property | grep ProductCode
```

## Submeter

1. Fork `microsoft/winget-pkgs`.
2. Copiar `manifests/a/Ailake/NexusFlow/<versão>/` pra dentro do fork.
3. Abrir PR — o bot do winget-pkgs valida schema/URL/hash automaticamente.

Alternativa mais simples pra manter isso em dia a cada release: o
[`wingetcreate`](https://github.com/microsoft/winget-create) CLI da
própria Microsoft automatiza os passos 1-3 a partir só da URL do
instalador (`wingetcreate update Ailake.NexusFlow --urls <msi-url> --version X.Y.Z --submit`)
— só roda em Windows/PowerShell, então não dá pra rodar direto deste
sandbox Linux; ainda não incorporado a nenhum workflow de CI.
