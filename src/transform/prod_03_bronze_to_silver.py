# -*- coding: utf-8 -*-
"""Transformação Bronze -> Silver do data lake.

Código promovido do notebook `notebooks/desenv_03_bronze_to_silver.ipynb`, onde
o desenvolvimento está documentado célula a célula, com os conceitos das aulas,
as verificações empíricas e as evidências de execução.

O que este script faz:
1. lê a configuração em `config/config.json` (ver `config/config.example.json`);
2. constrói os mapas de decodificação a partir da tabela `dicionario` da Bronze;
3. converte as tabelas de metas do formato wide para long (unpivot);
4. aplica a flag `presente` aos microdados de alunos (decisão D-011);
5. integra os resultados municipais com o diretório de municípios (dimensão
   IBGE) e com a meta da safra vigente (decisões D-010 e D-013);
6. agrega os eventos do fluxo em estimativa preliminar de 2025 por município,
   com origem explícita (decisão D-005);
7. aplica as regras estruturais de qualidade e isola os reprovados em
   quarentena, com o motivo carimbado;
8. grava as tabelas em `silver/` (e a quarentena em `quarentena/`),
   particionadas por data de processamento, e reconcilia cada gravação
   relendo o dado do lake.

Execução:
    python src/transform/prod_03_bronze_to_silver.py
    (no Windows, se o comando python não for reconhecido, use o launcher py:
    py src/transform/prod_03_bronze_to_silver.py)

Propriedades:
- Idempotência por sobrescrita de partição: reexecutar no mesmo dia substitui
  a partição do dia, sem duplicar dados (mesmo padrão do prod_01).
- Verificações executáveis: joins sem explosão, conservação de linhas na
  qualidade e reconciliação da gravação derrubam a execução com código de
  saída 1 e mensagem explicativa, permitindo que a orquestração detecte a
  falha.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pydata_google_auth
from google.cloud import storage

# ---------------------------------------------------------------------------
# Constantes (públicas, iguais para qualquer executor)
# ---------------------------------------------------------------------------
# Limite inferior da proficiência considerada alfabetizado, verificado
# empiricamente no levantamento das fontes (desenv_00)
CORTE_ALFABETIZACAO = 743.0

# Redes dos microdados equivalentes ao agregado rede Pública código 5 (D-010)
REDES_PUBLICAS_MICRODADOS = ["Estadual", "Municipal"]

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
# Acesso ao lake
# ---------------------------------------------------------------------------
class Lake:
    """Leitura da Bronze e gravação na Silver, com credencial renovável."""

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

    def ultima_particao(self, tabela: str) -> str:
        """Partição mais recente de uma tabela batch da Bronze."""
        particoes = sorted({
            b.name.split("/")[2]
            for b in self.cliente.list_blobs(self.bucket, prefix=f"bronze/{tabela}/")
        })
        if not particoes:
            sys.exit(
                f"Tabela '{tabela}' nao encontrada na Bronze "
                f"(gs://{self.bucket}/bronze/{tabela}/).\n"
                "Execute antes a ingestao batch: "
                "python src/ingestion/prod_01_ingestao_batch.py"
            )
        return particoes[-1]

    def ler_bronze(self, tabela: str, **kwargs) -> pd.DataFrame:
        """Lê a partição mais recente de uma tabela batch da Bronze."""
        self.garantir_credencial()
        caminho = (f"gs://{self.bucket}/bronze/{tabela}/"
                   f"{self.ultima_particao(tabela)}/{tabela}.parquet")
        return pd.read_parquet(caminho, storage_options={"token": self.credenciais},
                               **kwargs)

    def ler_micro_lotes(self, tabela: str) -> pd.DataFrame:
        """Lê e empilha todos os micro-lotes de uma tabela de eventos."""
        self.garantir_credencial()
        blobs = [b.name for b in
                 self.cliente.list_blobs(self.bucket, prefix=f"bronze/{tabela}/")]
        if not blobs:
            sys.exit(
                f"Nenhum micro-lote encontrado em bronze/{tabela}/.\n"
                "Execute antes a ingestao streaming (publicar e consumir): "
                "python src/ingestion/prod_02_ingestao_streaming.py"
            )
        lotes = [pd.read_parquet(f"gs://{self.bucket}/{caminho}",
                                 storage_options={"token": self.credenciais})
                 for caminho in blobs]
        return pd.concat(lotes, ignore_index=True)

    def gravar(self, df: pd.DataFrame, tabela: str, area: str = "silver") -> dict:
        """Grava uma tabela particionada por data de processamento."""
        self.garantir_credencial()
        momento = datetime.now(timezone.utc)
        df = df.copy()
        df["_processing_timestamp"] = momento.isoformat()
        destino = (f"gs://{self.bucket}/{area}/{tabela}/"
                   f"data_processamento={momento:%Y-%m-%d}/{tabela}.parquet")
        df.to_parquet(destino, index=False,
                      storage_options={"token": self.credenciais})
        return {"area": area, "tabela": tabela, "linhas": len(df),
                "destino": destino}


# ---------------------------------------------------------------------------
# Transformações (ciclo validado no notebook de desenvolvimento)
# ---------------------------------------------------------------------------
def construir_mapas(df_dicionario: pd.DataFrame) -> dict:
    """Mapas codigo -> significado extraídos da tabela dicionario (Seção 2)."""
    def mapa(id_tabela: str, coluna: str) -> dict:
        filtro = ((df_dicionario["id_tabela"] == id_tabela)
                  & (df_dicionario["nome_coluna"] == coluna))
        return dict(zip(df_dicionario.loc[filtro, "chave"],
                        df_dicionario.loc[filtro, "valor"]))
    return {"rede_agregadas": mapa("uf", "rede"),
            "rede_alunos": mapa("alunos", "rede")}


def unpivot_metas(df: pd.DataFrame, chaves: list) -> pd.DataFrame:
    """Converte as colunas meta_alfabetizacao_AAAA em linhas (Seção 3)."""
    colunas = [c for c in df.columns if c.startswith("meta_alfabetizacao_2")]
    longo = df.melt(id_vars=chaves, value_vars=colunas,
                    var_name="ano_meta", value_name="meta_taxa")
    longo["ano_meta"] = (longo["ano_meta"]
                         .str.replace("meta_alfabetizacao_", "")
                         .astype(int))
    return longo.rename(columns={"ano": "ano_referencia"})


def preparar_alunos(df_alunos: pd.DataFrame, mapas: dict) -> pd.DataFrame:
    """Flag presente (D-011) e decodificação da rede (Seção 4)."""
    df = df_alunos.copy()
    df["presente"] = df["presenca"] == "1"
    df["rede_nome"] = df["rede"].map(mapas["rede_alunos"])
    return df


def verificar_join(antes: int, depois: int, nome: str) -> None:
    """Join muitos-para-um não pode criar nem eliminar linhas (Seção 5)."""
    if antes != depois:
        sys.exit(f"Join '{nome}' alterou a contagem de linhas: "
                 f"{antes:,} -> {depois:,}. Verifique chaves duplicadas.")


def integrar_municipio(df_municipio: pd.DataFrame, df_diretorio: pd.DataFrame,
                       metas_municipio: pd.DataFrame, mapas: dict) -> pd.DataFrame:
    """Integra resultados municipais com dimensão e meta vigente (Seção 5)."""
    df = df_municipio.copy()
    df["rede_nome"] = df["rede"].map(mapas["rede_agregadas"])

    antes = len(df)
    df = df.merge(df_diretorio, on="id_municipio", how="left")
    verificar_join(antes, len(df), "municipio x diretorio")
    sem_nome = df["nome"].isna().sum()
    if sem_nome:
        sys.exit(f"{sem_nome} municipios sem correspondencia no diretorio.")

    # Safra vigente das metas (D-013): ano_referencia maximo, sem fallback
    safra = metas_municipio["ano_referencia"].max()
    vigentes = metas_municipio.loc[metas_municipio["ano_referencia"] == safra,
                                   ["id_municipio", "ano_meta", "meta_taxa"]]
    antes = len(df)
    df = df.merge(vigentes, left_on=["id_municipio", "ano"],
                  right_on=["id_municipio", "ano_meta"], how="left")
    df = df.drop(columns="ano_meta")
    verificar_join(antes, len(df), "municipio x metas vigentes")

    # A meta vale para a rede Publica codigo 5 (D-010)
    df.loc[df["rede"].astype(str) != "5", "meta_taxa"] = pd.NA
    return df


def estimar_2025(df_eventos: pd.DataFrame, df_diretorio: pd.DataFrame,
                 metas_municipio: pd.DataFrame, mapas: dict) -> pd.DataFrame:
    """Agrega o fluxo em estimativa preliminar por município (Seção 6)."""
    ev = df_eventos.copy()
    duplicados = ev["id_evento"].duplicated().sum()
    if duplicados:
        sys.exit(f"{duplicados} eventos duplicados no fluxo; "
                 "a deduplicacao do consumidor falhou.")

    ev["alfabetizado"] = ev["proficiencia"] >= CORTE_ALFABETIZACAO
    ev["rede_nome"] = ev["rede"].map(mapas["rede_alunos"])
    publica = ev[ev["rede_nome"].isin(REDES_PUBLICAS_MICRODADOS)].copy()

    publica["peso_alfabetizado"] = publica["peso_aluno"] * publica["alfabetizado"]
    df = (publica.groupby("id_municipio")
          .agg(alunos_no_fluxo=("id_evento", "size"),
               soma_pesos=("peso_aluno", "sum"),
               soma_pesos_alfabetizados=("peso_alfabetizado", "sum"))
          .reset_index())
    df["taxa_estimada"] = 100 * df["soma_pesos_alfabetizados"] / df["soma_pesos"]
    df = df[["id_municipio", "taxa_estimada", "alunos_no_fluxo"]]
    df.insert(0, "ano", 2025)
    df["origem"] = "estimativa_streaming"

    antes = len(df)
    df = df.merge(df_diretorio, on="id_municipio", how="left")
    verificar_join(antes, len(df), "estimativa x diretorio")

    safra = metas_municipio["ano_referencia"].max()
    metas_2025 = metas_municipio.loc[
        (metas_municipio["ano_referencia"] == safra)
        & (metas_municipio["ano_meta"] == 2025),
        ["id_municipio", "meta_taxa"],
    ]
    antes = len(df)
    df = df.merge(metas_2025, on="id_municipio", how="left")
    verificar_join(antes, len(df), "estimativa x meta 2025")
    return df


def aplicar_qualidade(df_alunos: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Regras estruturais da D-011; devolve (aprovados, quarentena) (Seção 7)."""
    df = df_alunos.copy()
    regras = {
        "presente_sem_nota": df["presente"] & df["proficiencia"].isna(),
        "ausente_com_nota": ~df["presente"] & df["proficiencia"].notna(),
    }
    df["motivo_quarentena"] = pd.NA
    for motivo, condicao in regras.items():
        df.loc[condicao & df["motivo_quarentena"].isna(),
               "motivo_quarentena"] = motivo

    quarentena = df[df["motivo_quarentena"].notna()].copy()
    aprovados = (df[df["motivo_quarentena"].isna()]
                 .drop(columns="motivo_quarentena"))
    if len(aprovados) + len(quarentena) != len(df):
        sys.exit("Violacao de conservacao: aprovados + quarentena != total.")
    return aprovados, quarentena


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------
def main() -> int:
    cfg = carregar_config()
    lake = Lake(cfg)
    inicio = datetime.now(timezone.utc)

    print(f"Transformacao Bronze -> Silver iniciada em {inicio.isoformat()}")
    print(f"Lake: gs://{cfg['bucket_lake']}/")
    print()

    etapas = [
        "dicionario e mapas", "metas (unpivot)", "alunos (flag presente)",
        "integracao municipal", "estimativa 2025 (fluxo)", "qualidade",
        "gravacao e reconciliacao",
    ]
    total = len(etapas)

    def progresso(i: int) -> float:
        print(f"[{i}/{total}] {etapas[i - 1]}...", flush=True)
        return time.time()

    t = progresso(1)
    mapas = construir_mapas(lake.ler_bronze("dicionario"))
    print(f"        mapas construidos ({time.time() - t:.0f}s)")

    t = progresso(2)
    metas = {
        "brasil": unpivot_metas(lake.ler_bronze("meta_alfabetizacao_brasil"),
                                ["ano", "rede"]),
        "uf": unpivot_metas(lake.ler_bronze("meta_alfabetizacao_uf"),
                            ["ano", "sigla_uf", "rede"]),
        "municipio": unpivot_metas(lake.ler_bronze("meta_alfabetizacao_municipio"),
                                   ["ano", "id_municipio", "rede"]),
    }
    print(f"        {sum(len(m) for m in metas.values()):,} linhas long "
          f"({time.time() - t:.0f}s)")

    t = progresso(3)
    df_alunos = preparar_alunos(
        lake.ler_bronze("alunos",
                        columns=["ano", "id_municipio", "id_aluno", "rede",
                                 "presenca", "alfabetizado", "proficiencia",
                                 "peso_aluno"]),
        mapas,
    )
    print(f"        {len(df_alunos):,} alunos ({time.time() - t:.0f}s)")

    t = progresso(4)
    df_diretorio = lake.ler_bronze(
        "diretorio_municipio",
        columns=["id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao"],
    )
    df_municipio = integrar_municipio(lake.ler_bronze("municipio"),
                                      df_diretorio, metas["municipio"], mapas)
    print(f"        {len(df_municipio):,} linhas integradas "
          f"({time.time() - t:.0f}s)")

    t = progresso(5)
    df_estimativa = estimar_2025(lake.ler_micro_lotes("eventos_resultado_aluno"),
                                 df_diretorio, metas["municipio"], mapas)
    print(f"        {len(df_estimativa):,} municipios estimados "
          f"({time.time() - t:.0f}s)")

    t = progresso(6)
    df_aprovados, df_quarentena = aplicar_qualidade(df_alunos)
    print(f"        {len(df_aprovados):,} aprovados, "
          f"{len(df_quarentena):,} em quarentena ({time.time() - t:.0f}s)")

    t = progresso(7)
    entregas = [
        lake.gravar(df_aprovados, "alunos"),
        lake.gravar(df_municipio, "municipio"),
        lake.gravar(df_estimativa, "estimativa_2025"),
        lake.gravar(metas["brasil"], "metas_brasil"),
        lake.gravar(metas["uf"], "metas_uf"),
        lake.gravar(metas["municipio"], "metas_municipio"),
        lake.gravar(df_quarentena, "alunos", area="quarentena"),
    ]
    divergentes = []
    for e in entregas:
        relido = pd.read_parquet(e["destino"],
                                 columns=["_processing_timestamp"],
                                 storage_options={"token": lake.credenciais})
        e["reconciliacao"] = "OK" if len(relido) == e["linhas"] else "DIVERGIU"
        if e["reconciliacao"] != "OK":
            divergentes.append(e)
    print(f"        {len(entregas)} tabelas gravadas ({time.time() - t:.0f}s)")

    # -----------------------------------------------------------------
    # Resumo da execucao: validacao final do processamento
    # -----------------------------------------------------------------
    duracao_min = (datetime.now(timezone.utc) - inicio).total_seconds() / 60
    print()
    print("=" * 60)
    print("RESUMO DA EXECUCAO")
    for e in entregas:
        print(f"  {e['area'] + '/' + e['tabela']:<28} "
              f"{e['linhas']:>10,} linhas  {e['reconciliacao']}")
    print(f"  Quarentena por motivo:  "
          f"{df_quarentena['motivo_quarentena'].value_counts().to_dict()}")
    print(f"  Particao da carga:      "
          f"data_processamento={inicio:%Y-%m-%d}")
    print(f"  Duracao total:          {duracao_min:.1f} min")
    status = "FALHA" if divergentes else "SUCESSO"
    print(f"  Status final:           {status}")
    print("=" * 60)

    if divergentes:
        for e in divergentes:
            print(f"  DIVERGENCIA em {e['area']}/{e['tabela']}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
