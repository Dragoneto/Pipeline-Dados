import boto3
import os
from pathlib import Path

BUCKET = "pipeline-vagas-davi"
PASTA_LOCAL = Path("data/raw")
PREFIX_S3 = "raw/"

def upload_para_s3():
    s3 = boto3.client("s3", region_name="us-east-1")

    arquivos = list(PASTA_LOCAL.glob("*.csv"))

    if not arquivos:
        print("Nenhum arquivo CSV encontrado em data/raw/")
        return

    for arquivo in arquivos:
        chave_s3 = f"{PREFIX_S3}{arquivo.name}"
        print(f"Enviando {arquivo.name} → s3://{BUCKET}/{chave_s3}")

        s3.upload_file(
            Filename=str(arquivo),
            Bucket=BUCKET,
            Key=chave_s3
        )
        print(f"✓ {arquivo.name} enviado com sucesso!")

if __name__ == "__main__":
    upload_para_s3()