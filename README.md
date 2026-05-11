# Pipeline de Dados — Vagas de Emprego

Pipeline de dados completo usando Python e AWS, 
processando 122.130 vagas do LinkedIn em camadas Medallion.

## Arquitetura

Kaggle → S3 Raw → S3 Bronze → S3 Silver → S3 Gold → Step Functions

## Tecnologias

- Python, Pandas, PyArrow
- AWS S3, Step Functions
- GitHub Actions (CI/CD)
- pytest, ruff

## Como rodar

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python src/orquestracao/pipeline.py
```

## Camadas

- **Raw** — CSV original (516MB)
- **Bronze** — Parquet sem alterações (238MB)
- **Silver** — Dados limpos e tipados (122.130 registros)
- **Gold** — 4 agregações de negócio prontas para análise
