# Contribuindo com NexusFlow

## Antes de abrir PR

1. Contribuições vão pro **core OSS** (Apache-2.0). Não envie código de conector destinado a ser pago — ver [`LICENSING.md`](./LICENSING.md) pra saber o que é OSS vs. enterprise.
2. Toda contribuição de código, ao ser submetida, fica automaticamente sob Apache-2.0 (cláusula 5 da licença) — sem CLA extra necessário nesta fase.

## Regras de código (obrigatórias)

Ver `CLAUDE.md §8` pra lista completa. Resumo:

- **Zero-copy no data path**: sem `String::clone()` ou conversão pra JSON dentro do fluxo de dados. Manipule referências Arrow (`ArrayData`).
- **Conector novo sem ADBC nativo**: implemente via `RecordBatchBuilder` (fallback obrigatório).
- **Erros**: `thiserror` pra erros locais do crate, `anyhow` pra orquestração. Handlers Axum retornam `IntoResponse` mapeado pra status HTTP correto.
- **Testabilidade**: todo conector precisa de interface mockável — teste unitário não pode depender de banco real rodando.
- **Features pesadas atrás de flag**: `ort`, drivers RDKafka, suporte GPU — sempre em `[features]` no `Cargo.toml`, nunca dependência default.

## Workflow

1. Fork/branch a partir de `main`.
2. `cargo fmt` + `cargo clippy --all-targets --all-features -- -D warnings` antes de commitar.
3. `cargo test --workspace` local passando.
4. Commit message: formato conciso, foco no *porquê* (não repita o diff na mensagem).
5. PR: descreva o que muda e por quê; linke issue se houver. CI precisa passar antes de review.

## Estrutura de crate novo

Todo crate novo dentro de `crates/` segue o padrão do workspace (ver `CLAUDE.md §3`). Se for conector, adicione entrada correspondente na matriz de conectividade em `ARCHITECTURE.md §3` e indique `ConnectorCapability` (`AdbcNative` / `ArrowFlight` / `Bridged`).

## Dúvidas de arquitetura

Ver [`ARCHITECTURE.md`](./ARCHITECTURE.md) antes de perguntar — cobre traits centrais, roteador de conectores, streaming/checkpointing e pipeline de embeddings.
