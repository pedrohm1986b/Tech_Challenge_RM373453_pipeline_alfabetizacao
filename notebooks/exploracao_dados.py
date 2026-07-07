# -*- coding: utf-8 -*-
"""
Exploração dos dados — Tech Challenge Fase 2 (Fase 1 do plano de desenvolvimento)
=================================================================================

O que este script faz:
  Conecta no BigQuery público da Base dos Dados e levanta tudo que precisamos
  saber sobre o dataset "Avaliação da Alfabetização" (INEP) ANTES de desenhar
  a pipeline: schemas, tamanhos, anos disponíveis, chaves, integridade e nulos.

  Os resultados deste script alimentaram o arquivo `dicionario_dados.md`.

Pré-requisitos (já feitos em 05/07/2026):
  1. Projeto GCP criado: tech-challenge-fase2
  2. Bibliotecas:  python -m pip install google-cloud-bigquery pandas-gbq pydata-google-auth pandas pyarrow
  3. Na primeira execução, o navegador abre pedindo autorização da sua conta
     Google (a credencial fica salva na máquina — nas próximas execuções não pede mais).

Como rodar:
  python exploracao_dados.py

Custo: ~zero. As consultas somam poucos MB lidos; o free tier do BigQuery
  dá 1 TB de leitura por mês. Consultas de metadados (schemas, tamanhos)
  não gastam nada.
"""

import sys
sys.stdout.reconfigure(encoding="utf-8")

import pandas_gbq

# --------------------------------------------------------------------------
# Configuração
# --------------------------------------------------------------------------
# Projeto GCP que "assina" as consultas (billing project — é o free tier dele que usamos)
PROJETO = "tech-challenge-fase2"

# Endereço do dataset público da Base dos Dados no BigQuery
BASE = "basedosdados.br_inep_avaliacao_alfabetizacao"

# Atalho: executa uma consulta SQL e devolve um DataFrame pandas
def consultar(sql: str):
    return pandas_gbq.read_gbq(sql, project_id=PROJETO, progress_bar_type=None)


# --------------------------------------------------------------------------
# 0) Teste de conexão — amostra de 5 linhas da tabela mais simples (uf)
# --------------------------------------------------------------------------
print("=" * 70)
print("0) TESTE DE CONEXÃO — amostra da tabela uf")
print("=" * 70)
df = consultar(f"SELECT * FROM `{BASE}.uf` LIMIT 5")
print(df.to_string())


# --------------------------------------------------------------------------
# 1) Tamanhos e contagens de linhas de todas as tabelas
#    (__TABLES__ é uma visão de metadados do BigQuery — leitura gratuita)
# --------------------------------------------------------------------------
print("\n" + "=" * 70)
print("1) TAMANHOS E CONTAGENS")
print("=" * 70)
meta = consultar(f"""
    SELECT table_id, row_count, ROUND(size_bytes/1024/1024, 2) AS size_mb
    FROM `{BASE}.__TABLES__`
    ORDER BY size_bytes DESC
""")
print(meta.to_string(index=False))


# --------------------------------------------------------------------------
# 2) Schema de todas as tabelas (colunas, tipos e descrições)
#    INFORMATION_SCHEMA é o "catálogo" do BigQuery — também metadado
# --------------------------------------------------------------------------
print("\n" + "=" * 70)
print("2) SCHEMAS (colunas, tipos, descrições)")
print("=" * 70)
cols = consultar(f"""
    SELECT table_name, column_name, data_type, description
    FROM `{BASE}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    ORDER BY table_name
""")
for tabela, grupo in cols.groupby("table_name"):
    print(f"\n--- {tabela} ---")
    for _, linha in grupo.iterrows():
        desc = (linha["description"] or "")[:90]
        print(f"  {linha['column_name']:<28} {linha['data_type']:<10} {desc}")


# --------------------------------------------------------------------------
# 3) Dicionário de códigos — decodifica as colunas categóricas
#    (ex.: rede 3 = Municipal; alfabetizado 1 = Sim)
# --------------------------------------------------------------------------
print("\n" + "=" * 70)
print("3) DICIONÁRIO DE CÓDIGOS")
print("=" * 70)
dic = consultar(f"""
    SELECT id_tabela, nome_coluna, chave, valor
    FROM `{BASE}.dicionario`
    ORDER BY id_tabela, nome_coluna, chave
""")
print(dic.to_string(index=False))


# --------------------------------------------------------------------------
# 4) Anos disponíveis em cada tabela (cobertura temporal real)
# --------------------------------------------------------------------------
print("\n" + "=" * 70)
print("4) ANOS DISPONÍVEIS POR TABELA")
print("=" * 70)
tabelas = ["uf", "municipio", "meta_alfabetizacao_uf",
           "meta_alfabetizacao_municipio", "meta_alfabetizacao_brasil", "alunos"]
for t in tabelas:
    anos = consultar(f"SELECT ano, COUNT(*) n FROM `{BASE}.{t}` GROUP BY ano ORDER BY ano")
    print(f"  {t:<32} ->", dict(zip(anos["ano"], anos["n"])))


# --------------------------------------------------------------------------
# 5) Verificação de chave única (existe linha duplicada para a mesma chave?)
#    Se linhas == chave_unica, a chave candidata é única (0 duplicatas)
# --------------------------------------------------------------------------
print("\n" + "=" * 70)
print("5) GRANULARIDADE / CHAVES ÚNICAS")
print("=" * 70)
chaves = consultar(f"""
    SELECT 'uf' AS tabela, COUNT(*) AS linhas,
           COUNT(DISTINCT CONCAT(ano, sigla_uf, serie, rede)) AS chave_unica
    FROM `{BASE}.uf`
    UNION ALL
    SELECT 'municipio', COUNT(*),
           COUNT(DISTINCT CONCAT(ano, id_municipio, serie, rede))
    FROM `{BASE}.municipio`
    UNION ALL
    SELECT 'meta_uf', COUNT(*),
           COUNT(DISTINCT CONCAT(ano, sigla_uf, rede))
    FROM `{BASE}.meta_alfabetizacao_uf`
    UNION ALL
    SELECT 'meta_municipio', COUNT(*),
           COUNT(DISTINCT CONCAT(ano, id_municipio, rede))
    FROM `{BASE}.meta_alfabetizacao_municipio`
""")
print(chaves.to_string(index=False))


# --------------------------------------------------------------------------
# 6) Integridade referencial — todo município citado existe no diretório
#    oficial (IBGE)? LEFT JOIN: quem ficar "sem par" é órfão.
# --------------------------------------------------------------------------
print("\n" + "=" * 70)
print("6) INTEGRIDADE: municipio (indicador) x diretório IBGE")
print("=" * 70)
integ1 = consultar(f"""
    SELECT COUNT(DISTINCT m.id_municipio) AS municipios_indicador,
           COUNT(DISTINCT IF(d.id_municipio IS NULL, m.id_municipio, NULL)) AS orfaos_sem_diretorio
    FROM `{BASE}.municipio` m
    LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` d USING (id_municipio)
""")
print(integ1.to_string(index=False))

print("\n--- meta_municipio x municipio (toda meta tem indicador?) ---")
integ2 = consultar(f"""
    SELECT COUNT(DISTINCT mm.id_municipio) AS municipios_na_meta,
           COUNT(DISTINCT IF(m.id_municipio IS NULL, mm.id_municipio, NULL)) AS meta_sem_indicador
    FROM `{BASE}.meta_alfabetizacao_municipio` mm
    LEFT JOIN (SELECT DISTINCT id_municipio FROM `{BASE}.municipio`) m USING (id_municipio)
""")
print(integ2.to_string(index=False))


# --------------------------------------------------------------------------
# 7) Nulos e faixas de valores nos campos importantes
# --------------------------------------------------------------------------
print("\n" + "=" * 70)
print("7) NULOS E FAIXAS (tabela municipio)")
print("=" * 70)
nulos = consultar(f"""
    SELECT COUNT(*) AS linhas,
           COUNTIF(taxa_alfabetizacao IS NULL)      AS taxa_nula,
           COUNTIF(media_portugues IS NULL)         AS media_nula,
           COUNTIF(proporcao_aluno_nivel_0 IS NULL) AS prop_nivel_nula,
           MIN(taxa_alfabetizacao) AS taxa_min,
           MAX(taxa_alfabetizacao) AS taxa_max
    FROM `{BASE}.municipio`
""")
print(nulos.to_string(index=False))

print("\n--- alunos: distribuição de alfabetizado e proficiência nula ---")
alunos = consultar(f"""
    SELECT ano, alfabetizado, COUNT(*) AS n,
           COUNTIF(proficiencia IS NULL) AS prof_nula
    FROM `{BASE}.alunos`
    GROUP BY ano, alfabetizado
    ORDER BY ano, alfabetizado
""")
print(alunos.to_string(index=False))

print("\nFim da exploração. Resultados documentados em dicionario_dados.md")
