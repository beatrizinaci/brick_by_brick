# Projeto

**brick_by_brick** é um pipeline de dados construído em Databricks para ingestão e transformação de dados do Citi Bike NYC (GBFS API). Segue a arquitetura medalhão (bronze → silver → gold) e é gerenciado via Databricks Asset Bundles.

## Objetivos

- Ingerir dados em tempo real da API GBFS do Citi Bike NYC na camada bronze.
- Transformar e disponibilizar dados nas camadas silver e gold (em construção).
- Servir como projeto de certificação de Databricks Professional Data Engineer.

## Estrutura

```
src/brick_by_brick/
├── bronze/
│   └── citibike_station_status.py   # Ingestão da API GBFS → Delta table
├── main.py                          # Entry point de teste de deploy

resources/
├── streaming_job.yml                # Job de streaming (a cada 5 min)
└── batch_job.yml                    # Job batch (diário às 6h, tasks silver/gold)

tests/
├── conftest.py                      # Fixtures Spark + Databricks Connect (opcionais)
└── bronze/
    └── test_citibike_station_status.py  # Testes unitários puros (sem Spark)

.github/workflows/
├── unit-tests.yml                   # Roda em todo push, sem Databricks
├── dev-bundle.yml                   # Deploy dev + destroy ao mergear PR
└── deploy-bundle.yml                # Deploy prod ao mergear na main
```

## Features implementadas

- Ingestão bronze: `citibike_station_status` — busca status das estações via HTTP, aplica schema estrito e faz append em Delta table.
- Isolamento por branch no ambiente dev (variável `branch` no bundle).
- CI com testes unitários independentes de Databricks.

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

# Testes

- Sempre que fizer sentido (nova função, lógica de transformação, integração com API externa), escrever testes unitários.
- Testes unitários ficam em `tests/bronze/`, `tests/silver/`, etc. (espelhando `src/`).
- Testes puros (sem Spark) usam apenas `pytest` + `unittest.mock` — sem precisar de Databricks Connect.
- Testes com Spark usam a fixture `spark` do `conftest.py`, que requer Databricks Connect configurado.
- O grupo de deps `unit` no `pyproject.toml` instala só o necessário para testes unitários: `uv run --group unit pytest tests/`.
- O CI roda os testes unitários em todo push via `.github/workflows/unit-tests.yml`.

# Segurança

- Nunca hardcodar credenciais, tokens ou senhas no código ou em arquivos commitados.
- Segredos devem ser configurados como GitHub Secrets e acessados via `${{ secrets.* }}` nos workflows.
- Validar e tratar erros de HTTP (status codes, timeouts) em toda integração com APIs externas.
- Atentar para injeção de SQL ao construir queries dinâmicas — preferir f-strings apenas com valores controlados (nomes de catalog/schema vindos de configuração, não de input do usuário).
- Nunca expor stack traces ou dados sensíveis em logs de produção.
