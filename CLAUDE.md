# Workflow

- Nunca commitar direto na main.
- Para cada bug, feature ou problema novo, criar uma branch descritiva (ex: `fix/ci-uv-install`, `feat/add-logging`).
- Fazer os commits na branch.
- Quando o trabalho estiver pronto, abrir um PR para main usando `gh pr create`.
- O título do PR deve conter a categoria de versionamento: `[MAJOR]`, `[MINOR]` ou `[PATCH]` (ex: `[PATCH] fix: corrige parsing de datas`).
  - `[MAJOR]` — mudanças incompatíveis com versões anteriores.
  - `[MINOR]` — novas funcionalidades compatíveis.
  - `[PATCH]` — correções de bugs.
- Somente a Beatriz pode aprovar e mergear PRs — nunca fazer merge automático.
