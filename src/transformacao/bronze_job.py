
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

BUCKET = "pipeline-vagas-davi"
RAW_KEY = "raw/postings.csv"
BRONZE_PREFIX = "bronze/postings/"

def processar_bronze():
    print("Iniciando job Bronze...")
    
    s3 = boto3.client("s3", region_name="us-east-1")
    
    print(f"Lendo s3://{BUCKET}/{RAW_KEY}")
    response = s3.get_object(Bucket=BUCKET, Key=RAW_KEY)
    df = pd.read_csv(response["Body"], low_memory=False)
    
    print(f"Shape do dataset: {df.shape}")
    print(f"Colunas: {list(df.columns)}")
    
    table = pa.Table.from_pandas(df)
    
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    
    bronze_key = f"{BRONZE_PREFIX}postings.parquet"
    print(f"Salvando em s3://{BUCKET}/{bronze_key}")
    
    s3.put_object(
        Bucket=BUCKET,
        Key=bronze_key,
        Body=buffer.getvalue()
    )
    
    print(f"✓ Bronze concluído! {len(df)} registros salvos como Parquet.")
    return df

if __name__ == "__main__":
    processar_bronze()