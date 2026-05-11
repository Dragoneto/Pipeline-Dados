import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

BUCKET = "pipeline-vagas-davi"
BRONZE_KEY = "bronze/postings/postings.parquet"
SILVER_PREFIX = "silver/postings/"

def processar_silver():
    print("Iniciando job Silver...")

    s3 = boto3.client("s3", region_name="us-east-1")

    print(f"Lendo s3://{BUCKET}/{BRONZE_KEY}")
    response = s3.get_object(Bucket=BUCKET, Key=BRONZE_KEY)
    df = pd.read_parquet(BytesIO(response["Body"].read()))

    print(f"Shape original: {df.shape}")

    # Remove duplicatas
    df = df.drop_duplicates(subset=["job_id"])
    print(f"Após deduplicação: {df.shape}")

    # Remove linhas sem título ou empresa
    df = df.dropna(subset=["title", "company_name"])
    print(f"Após remover nulos essenciais: {df.shape}")

    # Padroniza texto
    df["title"] = df["title"].str.strip().str.title()
    df["company_name"] = df["company_name"].str.strip().str.title()

    # Padroniza tipos
    df["max_salary"] = pd.to_numeric(df["max_salary"], errors="coerce")
    df["min_salary"] = pd.to_numeric(df["min_salary"], errors="coerce")
    df["applies"] = pd.to_numeric(df["applies"], errors="coerce").fillna(0).astype(int)

    # Filtra apenas vagas com localização preenchida
    df = df[df["location"].notna()]

    # Adiciona coluna de data de processamento
    df["processed_at"] = pd.Timestamp.now()

    print(f"Shape final Silver: {df.shape}")

    # Salva como Parquet no S3
    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    silver_key = f"{SILVER_PREFIX}postings.parquet"
    print(f"Salvando em s3://{BUCKET}/{silver_key}")

    s3.put_object(
        Bucket=BUCKET,
        Key=silver_key,
        Body=buffer.getvalue()
    )

    print(f"✓ Silver concluído! {len(df)} registros limpos salvos.")

if __name__ == "__main__":
    processar_silver()