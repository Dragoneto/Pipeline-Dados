import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

BUCKET = "pipeline-vagas-davi"
SILVER_KEY = "silver/postings/postings.parquet"

def salvar_gold(df, nome):
    s3 = boto3.client("s3", region_name="us-east-1")
    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    key = f"gold/{nome}/{nome}.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    print(f"✓ {nome} salvo em s3://{BUCKET}/{key} ({len(df)} registros)")

def processar_gold():
    print("Iniciando job Gold...")

    s3 = boto3.client("s3", region_name="us-east-1")
    response = s3.get_object(Bucket=BUCKET, Key=SILVER_KEY)
    df = pd.read_parquet(BytesIO(response["Body"].read()))

    print(f"Shape Silver: {df.shape}")

    # 1. Top empresas por número de vagas
    top_empresas = (
        df.groupby("company_name")
        .agg(total_vagas=("job_id", "count"))
        .reset_index()
        .sort_values("total_vagas", ascending=False)
        .head(100)
    )
    salvar_gold(top_empresas, "top_empresas")

    # 2. Vagas por nível de experiência
    vagas_por_nivel = (
        df.groupby("formatted_experience_level")
        .agg(total_vagas=("job_id", "count"))
        .reset_index()
        .sort_values("total_vagas", ascending=False)
    )
    salvar_gold(vagas_por_nivel, "vagas_por_nivel")

    # 3. Salário médio por título
    salario_por_titulo = (
        df[df["max_salary"].notna()]
        .groupby("title")
        .agg(
            salario_medio=("max_salary", "mean"),
            salario_minimo=("min_salary", "min"),
            salario_maximo=("max_salary", "max"),
            total_vagas=("job_id", "count")
        )
        .reset_index()
        .sort_values("salario_medio", ascending=False)
        .head(100)
    )
    salvar_gold(salario_por_titulo, "salario_por_titulo")

    # 4. Vagas por localização
    vagas_por_local = (
        df.groupby("location")
        .agg(total_vagas=("job_id", "count"))
        .reset_index()
        .sort_values("total_vagas", ascending=False)
        .head(100)
    )
    salvar_gold(vagas_por_local, "vagas_por_local")

    print("\n✓ Job Gold concluído!")

if __name__ == "__main__":
    processar_gold()