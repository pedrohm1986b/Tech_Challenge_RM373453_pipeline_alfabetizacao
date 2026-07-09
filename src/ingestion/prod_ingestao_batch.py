# -*- coding: utf-8 -*-
"""Ingestão batch das fontes consolidadas para a camada Bronze do data lake.

Código promovido do notebook `notebooks/desenv_ingestao_batch.ipynb`, onde o
desenvolvimento está documentado célula a célula, com os conceitos das aulas
de origem e as evidências de execução.

O que este script faz:
1. lê a configuração em `config/config.json` (ver `config/config.example.json`);
2. autoriza o acesso ao Google Cloud (o navegador abre na primeira execução;
   a credencial fica armazenada na máquina);
3. garante a existência do bucket do data lake;
4. para cada tabela da fonte: extrai do BigQuery público da Base dos Dados,
   adiciona os metadados de ingestão (`_ingestion_timestamp`, `_source`),
   grava Parquet particionado por data de ingestão na camada Bronze e
   reconcilia a contagem de linhas com a fonte;
5. imprime o relatório da carga e termina com código de saída 1 em caso de
   divergência, permitindo que a orquestração detecte a falha.

Execução:
    python src/ingestion/prod_ingestao_batch.py

Propriedades:
- Idempotência por sobrescrita de partição: reexecutar no mesmo dia substitui
  a partição do dia, sem duplicar dados. Partições de dias diferentes formam
  o histórico de cargas da Bronze.
- A fonte é pública; o projeto GCP configurado responde apenas pelo billing
  das consultas (free tier de 1 TB/mês) e pela posse do bucket.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas_gbq
import pydata_google_auth
from google.cloud import storage

# ---------------------------------------------------------------------------
# Constantes da fonte (públicas, iguais para qualquer executor)
# ---------------------------------------------------------------------------
FONTE = "basedosdados.br_inep_avaliacao_alfabetizacao"

# Tabelas ingeridas por batch (decisão D-005 do diário de decisões)
TABELAS_BATCH = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
    "alunos",
]

# Escopo amplo: cobre BigQuery (extração) e Cloud Storage (gravação)
ESCOPOS = ["https://www.googleapis.com/auth/cloud-platform"]

# Raiz do repositório (este arquivo está em src/ingestion/)
RAIZ = Path(__file__).resolve().parents[2]


def carregar_config() -> dict:
    """Lê config/config.json; orienta o executor caso não exista."""
    caminho = RAIZ / "config" / "config.json"
    if not caminho.exists():
        sys.exit(
            "Arquivo config/config.json nao encontrado.\n"
            "Copie config/config.example.json para config/config.json e "
            "preencha com o ID do seu projeto GCP e o nome do seu bucket."
        )
    return json.loads(caminho.read_text(encoding="utf-8"))


def garantir_bucket(cliente: storage.Client, nome: str, regiao: str) -> None:
    """Cria o bucket do data lake caso ainda não exista (idempotente)."""
    if nome not in [b.name for b in cliente.list_buckets()]:
        cliente.create_bucket(nome, location=regiao)
        print(f"Bucket criado: gs://{nome} (regiao {regiao})")


def ingerir_tabela(tabela: str, cfg: dict, credenciais) -> dict:
    """Ingere uma tabela da fonte para a Bronze e devolve as métricas da carga.

    Ciclo validado no notebook de desenvolvimento (Seções 4 a 6):
    extrair, carimbar metadados, gravar particionado, reconciliar.
    """
    # 1. Extração full da fonte
    df = pandas_gbq.read_gbq(
        f"SELECT * FROM `{FONTE}.{tabela}`",
        project_id=cfg["projeto_gcp"],
        credentials=credenciais,
        progress_bar_type=None,
    )

    # 2. Metadados de ingestão (rastreabilidade da Bronze)
    momento = datetime.now(timezone.utc)
    df["_ingestion_timestamp"] = momento.isoformat()
    df["_source"] = f"{FONTE}.{tabela}"

    # 3. Gravação em Parquet, particionada por data de ingestão
    dia = momento.strftime("%Y-%m-%d")
    destino = (
        f"gs://{cfg['bucket_lake']}/bronze/{tabela}/"
        f"data_ingestao={dia}/{tabela}.parquet"
    )
    df.to_parquet(destino, index=False, storage_options={"token": credenciais})

    # 4. Reconciliação de contagens fonte x Bronze
    qtd_fonte = pandas_gbq.read_gbq(
        f"SELECT COUNT(*) AS n FROM `{FONTE}.{tabela}`",
        project_id=cfg["projeto_gcp"],
        credentials=credenciais,
        progress_bar_type=None,
    )["n"][0]

    return {
        "tabela": tabela,
        "linhas": len(df),
        "fonte": int(qtd_fonte),
        "reconciliacao": "OK" if len(df) == qtd_fonte else "DIVERGIU",
        "destino": destino,
    }


def main() -> int:
    cfg = carregar_config()

    print(f"Ingestao batch iniciada em {datetime.now(timezone.utc).isoformat()}")
    print(f"Fonte:   {FONTE}")
    print(f"Destino: gs://{cfg['bucket_lake']}/bronze/")
    print()

    credenciais = pydata_google_auth.get_user_credentials(ESCOPOS)
    cliente = storage.Client(project=cfg["projeto_gcp"], credentials=credenciais)
    garantir_bucket(cliente, cfg["bucket_lake"], cfg["regiao"])

    resultados = []
    for tabela in TABELAS_BATCH:
        r = ingerir_tabela(tabela, cfg, credenciais)
        resultados.append(r)
        print(
            f"  {r['tabela']:<32} {r['linhas']:>10,} linhas   "
            f"reconciliacao: {r['reconciliacao']}"
        )

    divergentes = [r for r in resultados if r["reconciliacao"] != "OK"]
    print()
    if divergentes:
        print(f"FALHA: {len(divergentes)} tabela(s) com divergencia de contagem.")
        return 1

    print(f"Ingestao batch concluida: {len(resultados)} tabelas reconciliadas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
