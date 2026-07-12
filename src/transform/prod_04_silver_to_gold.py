# -*- coding: utf-8 -*-
"""Transformação Silver -> Gold do data lake, com publicação no BigQuery.

Código promovido do notebook `notebooks/desenv_04_silver_to_gold.ipynb`, onde
o desenvolvimento está documentado célula a célula, com os conceitos das
aulas, as validações contra os números oficiais e as evidências de execução.

O que este script faz:
1. lê a configuração em `config/config.json` (ver `config/config.example.json`);
2. confere a existência das tabelas da Silver (com orientação para executar
   o prod_03 caso alguma falte);
3. calcula as três medidas da decisão D-011 a partir dos microdados:
   taxa oficial (presentes ponderados), taxa ajustada (piso com ausentes
   como não alfabetizados) e percentual de participação;
4. monta a série histórica municipal unindo o histórico oficial (batch,
   2023 e 2024) com a estimativa do fluxo (streaming, 2025), com a coluna
   `origem` explícita em cada linha (decisão D-005);
5. integra a meta da safra vigente (decisão D-013) e deriva as colunas de
   comparação (`distancia_meta` e a flag de três estados `atingiu_meta`);
6. grava a tabela analítica `indicador_municipio` em `gold/`, particionada
   por data de processamento, com reconciliação;
7. publica a Gold no BigQuery como tabela externa (decisão D-012): o dado
   permanece no lake e o BigQuery guarda apenas os metadados, com o ponteiro
   movido para a partição mais recente a cada execução.

Execução:
    python src/transform/prod_04_silver_to_gold.py
    (no Windows, se o comando python não for reconhecido, use o launcher py:
    py src/transform/prod_04_silver_to_gold.py)

Propriedades:
- Idempotência por sobrescrita de partição na gravação e por recriação de
  metadados na tabela externa.
- Verificações executáveis: joins sem alteração de contagem, conservação da
  série e reconciliação da gravação derrubam a execução com código de saída
  1 e mensagem explicativa, permitindo que a orquestração detecte a falha.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pydata_google_auth
from google.cloud import bigquery, storage

# ---------------------------------------------------------------------------
# Constantes (públicas, iguais para qualquer executor)
# ---------------------------------------------------------------------------
# Redes dos microdados equivalentes ao agregado rede Pública código 5 (D-010)
REDES_PUBLICAS_MICRODADOS = ["Estadual", "Municipal"]

# Tabelas da Silver consumidas por este script
TABELAS_SILVER = ["alunos", "municipio", "estimativa_2025", "metas_municipio"]

ESCOPOS = ["https://www.googleapis.com/auth/cloud-platform"]
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


# ---------------------------------------------------------------------------
# Acesso ao lake (mesmo padrão do prod_03)
# ---------------------------------------------------------------------------
class Lake:
    """Leitura da Silver e gravação na Gold, com credencial renovável."""

    def __init__(self, cfg: dict):
        self.bucket = cfg["bucket_lake"]
        self.credenciais = pydata_google_auth.get_user_credentials(ESCOPOS)
        self.credenciais = self.credenciais.with_quota_project(cfg["projeto_gcp"])
        self.cliente = storage.Client(project=cfg["projeto_gcp"],
                                      credentials=self.credenciais)

    def garantir_credencial(self) -> None:
        """Renova o token expirado e limpa o cache do gcsfs (sessões longas)."""
        import google.auth.transport.requests
        import gcsfs
        if not self.credenciais.valid:
            self.credenciais.refresh(google.auth.transport.requests.Request())
            gcsfs.GCSFileSystem.clear_instance_cache()

    def ultima_particao(self, area: str, tabela: str) -> str:
        """Partição mais recente de uma tabela; erro orientado se faltar."""
        particoes = sorted({
            b.name.split("/")[2]
            for b in self.cliente.list_blobs(self.bucket,
                                             prefix=f"{area}/{tabela}/")
        })
        if not particoes:
            sys.exit(
                f"Tabela '{tabela}' nao encontrada em {area}/ "
                f"(gs://{self.bucket}/{area}/{tabela}/).\n"
                "Execute antes a transformacao Bronze -> Silver: "
                "python src/transform/prod_03_bronze_to_silver.py"
            )
        return particoes[-1]

    def ler_silver(self, tabela: str, **kwargs) -> pd.DataFrame:
        """Lê a partição mais recente de uma tabela da Silver."""
        self.garantir_credencial()
        caminho = (f"gs://{self.bucket}/silver/{tabela}/"
                   f"{self.ultima_particao('silver', tabela)}/{tabela}.parquet")
        return pd.read_parquet(caminho,
                               storage_options={"token": self.credenciais},
                               **kwargs)

    def gravar_gold(self, df: pd.DataFrame, tabela: str) -> dict:
        """Grava uma tabela em gold/, particionada por data de processamento."""
        self.garantir_credencial()
        momento = datetime.now(timezone.utc)
        df = df.copy()
        df["_processing_timestamp"] = momento.isoformat()
        destino = (f"gs://{self.bucket}/gold/{tabela}/"
                   f"data_processamento={momento:%Y-%m-%d}/{tabela}.parquet")
        df.to_parquet(destino, index=False,
                      storage_options={"token": self.credenciais})
        return {"tabela": tabela, "linhas": len(df), "destino": destino}


# ---------------------------------------------------------------------------
# Transformações (ciclo validado no notebook de desenvolvimento)
# ---------------------------------------------------------------------------
def verificar_join(antes: int, depois: int, nome: str) -> None:
    """Join muitos-para-um não pode criar nem eliminar linhas."""
    if antes != depois:
        sys.exit(f"Join '{nome}' alterou a contagem de linhas: "
                 f"{antes:,} -> {depois:,}. Verifique chaves duplicadas.")


def calcular_metricas(df_alunos: pd.DataFrame) -> pd.DataFrame:
    """As três medidas da D-011 por município e ano (Seção 3 do notebook)."""
    publica = df_alunos[
        df_alunos["rede_nome"].isin(REDES_PUBLICAS_MICRODADOS)
    ].copy()
    publica["alfabetizado_bin"] = publica["alfabetizado"].astype(str) == "1"
    publica["peso_alfabetizado"] = (publica["peso_aluno"]
                                    * publica["alfabetizado_bin"])

    metricas = (publica.groupby(["ano", "id_municipio"])
                .agg(alunos_total=("presente", "size"),
                     alunos_presentes=("presente", "sum"),
                     soma_pesos=("peso_aluno", "sum"),
                     soma_pesos_alfabetizados=("peso_alfabetizado", "sum"))
                .reset_index())
    metricas["percentual_participacao"] = (100 * metricas["alunos_presentes"]
                                           / metricas["alunos_total"])
    metricas["taxa_oficial_calculada"] = (
        100 * metricas["soma_pesos_alfabetizados"] / metricas["soma_pesos"])
    metricas["taxa_ajustada"] = (metricas["taxa_oficial_calculada"]
                                 * metricas["percentual_participacao"] / 100)
    return metricas


def montar_serie(df_municipio: pd.DataFrame, metricas: pd.DataFrame,
                 df_estimativa: pd.DataFrame) -> pd.DataFrame:
    """Série histórica: estoque oficial + ponto estimado (Seção 4)."""
    oficial5 = df_municipio.loc[
        df_municipio["rede"].astype(str) == "5",
        ["ano", "id_municipio", "nome", "sigla_uf", "taxa_alfabetizacao"],
    ]
    antes = len(oficial5)
    estoque = oficial5.merge(
        metricas[["ano", "id_municipio", "percentual_participacao",
                  "taxa_ajustada", "alunos_presentes"]],
        on=["ano", "id_municipio"], how="left",
    ).rename(columns={"taxa_alfabetizacao": "taxa"})
    verificar_join(antes, len(estoque), "oficial x metricas")
    estoque["origem"] = "oficial_inep"

    fluxo = (df_estimativa
             .rename(columns={"taxa_estimada": "taxa"})
             [["ano", "id_municipio", "nome", "sigla_uf", "taxa",
               "alunos_no_fluxo", "origem"]])

    serie = pd.concat([estoque, fluxo], ignore_index=True)
    if len(serie) != len(estoque) + len(fluxo):
        sys.exit("Violacao de conservacao na uniao estoque + fluxo.")
    return serie


def integrar_metas(serie: pd.DataFrame,
                   metas_municipio: pd.DataFrame) -> pd.DataFrame:
    """Meta da safra vigente (D-013) e colunas de comparação (Seção 5)."""
    safra = metas_municipio["ano_referencia"].max()
    vigentes = metas_municipio.loc[
        metas_municipio["ano_referencia"] == safra,
        ["id_municipio", "ano_meta", "meta_taxa"],
    ]
    antes = len(serie)
    serie = serie.merge(vigentes, left_on=["id_municipio", "ano"],
                        right_on=["id_municipio", "ano_meta"],
                        how="left").drop(columns="ano_meta")
    verificar_join(antes, len(serie), "serie x metas vigentes")

    serie["distancia_meta"] = serie["taxa"] - serie["meta_taxa"]
    # Veredito oficial de atingimento: apenas com dado oficial E meta presente
    com_veredito = ((serie["origem"] == "oficial_inep")
                    & serie["meta_taxa"].notna())
    serie["atingiu_meta"] = pd.NA
    serie.loc[com_veredito, "atingiu_meta"] = (
        serie.loc[com_veredito, "taxa"] >= serie.loc[com_veredito, "meta_taxa"])
    serie["atingiu_meta"] = serie["atingiu_meta"].astype("boolean")
    return serie


def publicar_bigquery(cfg: dict, lake: Lake) -> str:
    """Tabela externa do BigQuery sobre a partição mais recente da Gold."""
    cliente_bq = bigquery.Client(project=cfg["projeto_gcp"],
                                 credentials=lake.credenciais)

    dataset = bigquery.Dataset(f"{cfg['projeto_gcp']}.gold")
    dataset.location = cfg["regiao"]
    cliente_bq.create_dataset(dataset, exists_ok=True)

    particao = lake.ultima_particao("gold", "indicador_municipio")
    uri = (f"gs://{cfg['bucket_lake']}/gold/indicador_municipio/"
           f"{particao}/*.parquet")

    config_externa = bigquery.ExternalConfig("PARQUET")
    config_externa.source_uris = [uri]
    tabela = bigquery.Table(f"{cfg['projeto_gcp']}.gold.indicador_municipio")
    tabela.external_data_configuration = config_externa

    # Recriar move o ponteiro para a particao vigente (apenas metadados)
    cliente_bq.delete_table(tabela, not_found_ok=True)
    cliente_bq.create_table(tabela)
    return uri


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------
def main() -> int:
    cfg = carregar_config()
    lake = Lake(cfg)
    inicio = datetime.now(timezone.utc)

    print(f"Transformacao Silver -> Gold iniciada em {inicio.isoformat()}")
    print(f"Lake: gs://{cfg['bucket_lake']}/")
    print()

    etapas = ["inventario da Silver", "metricas D-011", "serie historica",
              "integracao de metas", "gravacao e reconciliacao",
              "publicacao no BigQuery"]
    total = len(etapas)

    def progresso(i: int) -> float:
        print(f"[{i}/{total}] {etapas[i - 1]}...", flush=True)
        return time.time()

    t = progresso(1)
    for tabela in TABELAS_SILVER:
        lake.ultima_particao("silver", tabela)  # existencia; erro orienta
    print(f"        {len(TABELAS_SILVER)} tabelas presentes "
          f"({time.time() - t:.0f}s)")

    t = progresso(2)
    metricas = calcular_metricas(lake.ler_silver(
        "alunos", columns=["ano", "id_municipio", "rede_nome", "presente",
                           "alfabetizado", "peso_aluno"]))
    print(f"        {len(metricas):,} combinacoes municipio x ano "
          f"({time.time() - t:.0f}s)")

    t = progresso(3)
    serie = montar_serie(lake.ler_silver("municipio"), metricas,
                         lake.ler_silver("estimativa_2025"))
    print(f"        {len(serie):,} linhas na serie ({time.time() - t:.0f}s)")

    t = progresso(4)
    serie = integrar_metas(serie, lake.ler_silver("metas_municipio"))
    com_meta = serie["meta_taxa"].notna().sum()
    print(f"        {com_meta:,} linhas com meta vigente "
          f"({time.time() - t:.0f}s)")

    t = progresso(5)
    entrega = lake.gravar_gold(serie, "indicador_municipio")
    relido = pd.read_parquet(entrega["destino"],
                             columns=["_processing_timestamp"],
                             storage_options={"token": lake.credenciais})
    reconciliacao = "OK" if len(relido) == entrega["linhas"] else "DIVERGIU"
    print(f"        {entrega['linhas']:,} linhas gravadas, "
          f"reconciliacao {reconciliacao} ({time.time() - t:.0f}s)")

    t = progresso(6)
    uri = publicar_bigquery(cfg, lake)
    print(f"        tabela externa sobre {uri} ({time.time() - t:.0f}s)")

    # -----------------------------------------------------------------
    # Resumo da execucao: validacao final do processamento
    # -----------------------------------------------------------------
    duracao_min = (datetime.now(timezone.utc) - inicio).total_seconds() / 60
    origens = serie["origem"].value_counts().to_dict()
    print()
    print("=" * 60)
    print("RESUMO DA EXECUCAO")
    print(f"  gold/indicador_municipio:  {entrega['linhas']:,} linhas  "
          f"{reconciliacao}")
    print(f"  Composicao por origem:     {origens}")
    print(f"  Linhas com meta vigente:   {com_meta:,}")
    print(f"  Consumo SQL:               {cfg['projeto_gcp']}.gold."
          f"indicador_municipio (tabela externa)")
    print(f"  Particao da carga:         data_processamento={inicio:%Y-%m-%d}")
    print(f"  Duracao total:             {duracao_min:.1f} min")
    status = "FALHA" if reconciliacao != "OK" else "SUCESSO"
    print(f"  Status final:              {status}")
    print("=" * 60)

    return 1 if status == "FALHA" else 0


if __name__ == "__main__":
    sys.exit(main())
