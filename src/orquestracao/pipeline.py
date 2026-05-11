import sys
import traceback
from datetime import datetime

sys.path.append(".")

from src.transformacao.bronze_job import processar_bronze
from src.transformacao.silver_job import processar_silver
from src.transformacao.gold_job import processar_gold

def executar_pipeline():
    inicio = datetime.now()
    print(f"{'='*50}")
    print(f"PIPELINE INICIADO: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    etapas = [
        ("Bronze", processar_bronze),
        ("Silver", processar_silver),
        ("Gold",   processar_gold),
    ]

    for nome, funcao in etapas:
        print(f"\n{'─'*40}")
        print(f"ETAPA: {nome}")
        print(f"{'─'*40}")
        try:
            funcao()
            print(f"✓ Etapa {nome} concluída com sucesso!")
        except Exception as e:
            print(f"\n✗ ERRO na etapa {nome}:")
            print(f"  {str(e)}")
            traceback.print_exc()
            print(f"\nPipeline interrompido na etapa {nome}.")
            sys.exit(1)

    fim = datetime.now()
    duracao = (fim - inicio).seconds
    print(f"\n{'='*50}")
    print(f"PIPELINE CONCLUÍDO EM {duracao} SEGUNDOS")
    print(f"{'='*50}")

if __name__ == "__main__":
    executar_pipeline()