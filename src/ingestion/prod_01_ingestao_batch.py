# -*- coding: utf-8 -*-
"""Ingestão batch das fontes consolidadas para a camada Bronze do data lake.

Código promovido do notebook `notebooks/desenv_01_ingestao_batch.ipynb`, onde o
desenvolvimento está documentado célula a célula, com os conceitos das aulas
de origem e as evidências de execução.

O que este script faz:
1. lê a configuração em `config/config.json` (ver `config/config.example.json`);
(Esta parte é muito importante, pois o avaliador deve criar um projeto atribuido à sua própria conta no Google Cloud para poder rodar o pipeline.)
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
    python src/ingestion/prod_01_ingestao_batch.py
    (no Windows, se o comando python não for reconhecido, use o launcher py:
    py src/ingestion/prod_01_ingestao_batch.py)

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
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas_gbq
import pydata_google_auth
from google.cloud import storage

# ---------------------------------------------------------------------------
# Constantes da fonte (públicas, iguais para qualquer executor)
# ---------------------------------------------------------------------------
FONTE = "basedosdados.br_inep_avaliacao_alfabetizacao"
FONTE_DIRETORIOS = "basedosdados.br_bd_diretorios_brasil"

# Tabelas ingeridas por batch (decisão D-005 do diário de decisões):
# nome da tabela na Bronze -> tabela qualificada na fonte
TABELAS_BATCH = {
    "uf": f"{FONTE}.uf",
    "municipio": f"{FONTE}.municipio",
    "meta_alfabetizacao_brasil": f"{FONTE}.meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf": f"{FONTE}.meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio": f"{FONTE}.meta_alfabetizacao_municipio",
    "dicionario": f"{FONTE}.dicionario",
    "alunos": f"{FONTE}.alunos",
    # Diretório de municípios do IBGE, a única tabela de DIMENSÃO da carga.
    # As 7 tabelas do INEP são fatos: registram medidas (taxas, proficiências)
    # por chave, e nenhuma traduz o id_municipio em nome, UF e região. Sem a
    # dimensão, as camadas analíticas entregariam apenas códigos. Necessidade
    # identificada na integração da camada Silver (desenv_03, Seção 5).
    "diretorio_municipio": f"{FONTE_DIRETORIOS}.municipio",
}

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


def ingerir_tabela(tabela: str, fonte_tabela: str, cfg: dict, credenciais) -> dict:
    """Ingere uma tabela da fonte para a Bronze e devolve as métricas da carga.

    Ciclo validado no notebook de desenvolvimento (Seções 4 a 6):
    extrair, carimbar metadados, gravar particionado, reconciliar.
    """
    # 1. Extração full da fonte
    df = pandas_gbq.read_gbq(
        f"SELECT * FROM `{fonte_tabela}`",
        project_id=cfg["projeto_gcp"],
        credentials=credenciais,
        progress_bar_type=None,
    )

    # 2. Metadados de ingestão (rastreabilidade da Bronze)
    momento = datetime.now(timezone.utc)
    df["_ingestion_timestamp"] = momento.isoformat()
    df["_source"] = fonte_tabela

    # 3. Gravação em Parquet, particionada por data de ingestão
    dia = momento.strftime("%Y-%m-%d")
    destino = (
        f"gs://{cfg['bucket_lake']}/bronze/{tabela}/"
        f"data_ingestao={dia}/{tabela}.parquet"
    )
    df.to_parquet(destino, index=False, storage_options={"token": credenciais})

    # 4. Reconciliação de contagens fonte x Bronze
    qtd_fonte = pandas_gbq.read_gbq(
        f"SELECT COUNT(*) AS n FROM `{fonte_tabela}`",
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
    fontes = sorted({t.rsplit(".", 1)[0] for t in TABELAS_BATCH.values()})
    print("Fontes:  " + ", ".join(fontes))
    print(f"Destino: gs://{cfg['bucket_lake']}/bronze/")
    print()

    credenciais = pydata_google_auth.get_user_credentials(ESCOPOS)
    cliente = storage.Client(project=cfg["projeto_gcp"], credentials=credenciais)
    garantir_bucket(cliente, cfg["bucket_lake"], cfg["regiao"])

    inicio = datetime.now(timezone.utc)
    total = len(TABELAS_BATCH)

    resultados = []
    for i, (tabela, fonte_tabela) in enumerate(TABELAS_BATCH.items(), start=1):
        # Progresso em tempo real: qual tabela, posicao na fila e duracao.
        # flush=True garante que a mensagem aparece antes da carga comecar.
        print(f"[{i}/{total}] Ingerindo {tabela}...", flush=True)
        t0 = time.time()
        r = ingerir_tabela(tabela, fonte_tabela, cfg, credenciais)
        r["duracao_s"] = time.time() - t0
        resultados.append(r)
        print(
            f"        {r['linhas']:>10,} linhas   "
            f"reconciliacao: {r['reconciliacao']}   "
            f"({r['duracao_s']:.0f}s)"
        )

    # -----------------------------------------------------------------
    # Resumo da execucao: validacao final do processamento
    # -----------------------------------------------------------------
    divergentes = [r for r in resultados if r["reconciliacao"] != "OK"]
    linhas_totais = sum(r["linhas"] for r in resultados)
    duracao_min = (datetime.now(timezone.utc) - inicio).total_seconds() / 60

    # Volume real gravado no lake na particao de hoje (conferencia fisica)
    dia = inicio.strftime("%Y-%m-%d")
    bytes_lake = sum(
        blob.size
        for tabela in TABELAS_BATCH
        for blob in cliente.list_blobs(
            cfg["bucket_lake"], prefix=f"bronze/{tabela}/data_ingestao={dia}/"
        )
    )

    print()
    print("=" * 60)
    print("RESUMO DA EXECUCAO")
    print(f"  Tabelas ingeridas:      {len(resultados)} de {len(TABELAS_BATCH)}")
    print(f"  Reconciliacao:          {len(resultados) - len(divergentes)} OK, "
          f"{len(divergentes)} divergente(s)")
    print(f"  Linhas totais:          {linhas_totais:,}")
    print(f"  Volume gravado (lake):  {bytes_lake / 1024 / 1024:.1f} MB")
    print(f"  Particao da carga:      data_ingestao={dia}")
    print(f"  Bucket:                 gs://{cfg['bucket_lake']}/bronze/")
    print(f"  Duracao total:          {duracao_min:.1f} min")
    status = "FALHA" if divergentes else "SUCESSO"
    print(f"  Status final:           {status}")
    print("=" * 60)

    if divergentes:
        for r in divergentes:
            print(f"  DIVERGENCIA em {r['tabela']}: "
                  f"fonte={r['fonte']:,} vs gravado={r['linhas']:,}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
