# -*- coding: utf-8 -*-
"""Ingestão streaming: eventos de resultado de aluno para a camada Bronze.

Código promovido do notebook `notebooks/desenv_02_ingestao_streaming.ipynb`,
onde o desenvolvimento está documentado célula a célula, com os conceitos das
aulas de origem e as evidências de execução.

O script opera em dois modos, espelhando os dois papéis do streaming:

    python src/ingestion/prod_02_ingestao_streaming.py publicar --eventos 200
        Simula o sistema externo de avaliação (producer): lê os microdados
        reais da Bronze, gera eventos do ciclo de 2025 conforme o contrato
        (config/schemas/evento_resultado_aluno.md) e os publica no tópico.

    python src/ingestion/prod_02_ingestao_streaming.py consumir
        Executa o consumer da pipeline: lê o backlog da subscription, valida
        cada evento contra o contrato, desvia malformados para a DLQ,
        descarta duplicatas (registro persistido entre execuções) e grava
        os válidos como micro-lote Parquet na Bronze.

    (no Windows, se o comando python não for reconhecido, use o launcher py:
    py src/ingestion/prod_02_ingestao_streaming.py ...)

Infraestrutura (criada automaticamente se não existir): tópico de eventos,
tópico de DLQ e as subscriptions da pipeline e de inspeção.

Lições incorporadas do desenvolvimento:
- a credencial de usuário declara o quota project (APIs fora do conjunto
  padrão do client OAuth exigem essa declaração explícita);
- o pull síncrono lança DeadlineExceeded quando o backlog está vazio; a
  exceção é tratada como fim do consumo;
- os clientes de mensageria são encerrados explicitamente ao final (em
  notebook eles permanecem vivos no kernel; em script, o encerramento
  garante a saída limpa do processo);
- o registro de deduplicação é persistido no lake (controle/), preservando
  a idempotência entre execuções do consumer.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pydata_google_auth
from google.api_core.exceptions import AlreadyExists, DeadlineExceeded, NotFound
from google.cloud import pubsub_v1, storage

# ---------------------------------------------------------------------------
# Constantes do contrato e da mensageria
# ---------------------------------------------------------------------------
TOPICO_EVENTOS = "eventos-resultado-aluno"
TOPICO_DLQ = "eventos-resultado-aluno-dlq"
SUB_PIPELINE = "sub-pipeline-bronze"
SUB_DLQ = "sub-inspecao-dlq"

CAMPOS_OBRIGATORIOS = {
    "id_evento", "schema_version", "event_time", "id_municipio",
    "rede", "id_aluno", "proficiencia", "peso_aluno",
}

ESCOPOS = ["https://www.googleapis.com/auth/cloud-platform"]
RAIZ = Path(__file__).resolve().parents[2]

# Registro de deduplicação persistido fora da área de dados da Bronze,
# para não interferir na leitura consolidada dos micro-lotes
CAMINHO_DEDUP = "controle/dedup_eventos_resultado_aluno.json"


def carregar_config() -> dict:
    caminho = RAIZ / "config" / "config.json"
    if not caminho.exists():
        sys.exit(
            "Arquivo config/config.json nao encontrado.\n"
            "Copie config/config.example.json para config/config.json e "
            "preencha com o ID do seu projeto GCP e o nome do seu bucket."
        )
    return json.loads(caminho.read_text(encoding="utf-8"))


def obter_credenciais(cfg: dict):
    """Credencial de usuário com o quota project do executor declarado."""
    credenciais = pydata_google_auth.get_user_credentials(ESCOPOS)
    return credenciais.with_quota_project(cfg["projeto_gcp"])


def garantir_infraestrutura(cfg: dict, credenciais) -> dict:
    """Cria tópicos e subscriptions caso não existam; devolve os caminhos."""
    publisher = pubsub_v1.PublisherClient(credentials=credenciais)
    subscriber = pubsub_v1.SubscriberClient(credentials=credenciais)
    projeto = cfg["projeto_gcp"]

    caminhos = {
        "topico": publisher.topic_path(projeto, TOPICO_EVENTOS),
        "dlq": publisher.topic_path(projeto, TOPICO_DLQ),
        "sub": subscriber.subscription_path(projeto, SUB_PIPELINE),
        "sub_dlq": subscriber.subscription_path(projeto, SUB_DLQ),
    }
    for topico in (caminhos["topico"], caminhos["dlq"]):
        try:
            publisher.create_topic(request={"name": topico})
        except AlreadyExists:
            pass
    try:
        subscriber.create_subscription(request={
            "name": caminhos["sub"], "topic": caminhos["topico"],
            "enable_message_ordering": True,
        })
    except AlreadyExists:
        pass
    try:
        subscriber.create_subscription(request={
            "name": caminhos["sub_dlq"], "topic": caminhos["dlq"],
        })
    except AlreadyExists:
        pass
    subscriber.close()
    return caminhos


def validar_evento(ev) -> str:
    """Valida um evento contra o contrato v1; devolve o motivo da falha
    ou uma string vazia se o evento for válido."""
    if not isinstance(ev, dict):
        return "payload nao e um objeto JSON"
    faltantes = CAMPOS_OBRIGATORIOS - ev.keys()
    if faltantes:
        return f"campos ausentes: {sorted(faltantes)}"
    if ev["schema_version"] != 1:
        return f"schema_version desconhecida: {ev['schema_version']}"
    if not isinstance(ev["proficiencia"], (int, float)):
        return "proficiencia nao numerica"
    if not isinstance(ev["peso_aluno"], (int, float)):
        return "peso_aluno nao numerico"
    return ""


# ---------------------------------------------------------------------------
# Modo publicar (producer)
# ---------------------------------------------------------------------------
def carregar_base_amostragem(cfg: dict, credenciais) -> pd.DataFrame:
    """Alunos presentes de 2024 da Bronze (partição mais recente)."""
    cliente = storage.Client(project=cfg["projeto_gcp"], credentials=credenciais)
    particoes = sorted({
        b.name.split("/")[2]
        for b in cliente.list_blobs(cfg["bucket_lake"], prefix="bronze/alunos/")
    })
    if not particoes:
        sys.exit("Bronze de alunos vazia. Execute antes a ingestao batch "
                 "(prod_01_ingestao_batch.py).")
    caminho = (f"gs://{cfg['bucket_lake']}/bronze/alunos/"
               f"{particoes[-1]}/alunos.parquet")
    base = pd.read_parquet(
        caminho,
        columns=["ano", "id_municipio", "rede", "proficiencia", "peso_aluno"],
        storage_options={"token": credenciais},
    )
    return base[(base["ano"] == 2024)
                & base["proficiencia"].notna()
                & base["peso_aluno"].notna()]


def gerar_evento(linha) -> dict:
    return {
        "id_evento": str(uuid.uuid4()),
        "schema_version": 1,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "id_municipio": str(linha["id_municipio"]),
        "rede": str(linha["rede"]),
        "id_aluno": f"A2025-{random.randint(0, 999999):06d}",
        "proficiencia": round(float(linha["proficiencia"]), 1),
        "peso_aluno": round(float(linha["peso_aluno"]), 4),
    }


def modo_publicar(cfg: dict, credenciais, caminhos: dict, n_eventos: int) -> int:
    print(f"Carregando base de amostragem da Bronze...")
    base = carregar_base_amostragem(cfg, credenciais)
    print(f"  {len(base):,} alunos presentes de 2024")

    publisher = pubsub_v1.PublisherClient(
        credentials=credenciais,
        publisher_options=pubsub_v1.types.PublisherOptions(
            enable_message_ordering=True),
    )

    t0 = time.time()
    futuros = []
    for _, linha in base.sample(n_eventos).iterrows():
        ev = gerar_evento(linha)
        futuros.append(publisher.publish(
            caminhos["topico"],
            json.dumps(ev).encode("utf-8"),
            ordering_key=ev["id_municipio"],
        ))
    for f in futuros:
        f.result(timeout=60)
    duracao = time.time() - t0

    publisher.stop()   # encerra a infraestrutura de fundo do cliente

    print()
    print("=" * 60)
    print("RESUMO DA PUBLICACAO")
    print(f"  Eventos publicados:   {n_eventos:,}")
    print(f"  Duracao:              {duracao:.1f}s "
          f"({n_eventos / duracao:,.0f} eventos/s)")
    print(f"  Topico:               {TOPICO_EVENTOS}")
    print("  Status final:         SUCESSO")
    print("=" * 60)
    return 0


# ---------------------------------------------------------------------------
# Modo consumir (consumer da pipeline)
# ---------------------------------------------------------------------------
def carregar_dedup(cliente: storage.Client, bucket: str) -> set:
    blob = cliente.bucket(bucket).blob(CAMINHO_DEDUP)
    if blob.exists():
        return set(json.loads(blob.download_as_text()))
    return set()


def salvar_dedup(cliente: storage.Client, bucket: str, ids: set) -> None:
    cliente.bucket(bucket).blob(CAMINHO_DEDUP).upload_from_string(
        json.dumps(sorted(ids)), content_type="application/json")


def modo_consumir(cfg: dict, credenciais, caminhos: dict) -> int:
    subscriber = pubsub_v1.SubscriberClient(credentials=credenciais)
    publisher_dlq = pubsub_v1.PublisherClient(credentials=credenciais)
    cliente = storage.Client(project=cfg["projeto_gcp"], credentials=credenciais)

    # Registro de deduplicação persistido: idempotência entre execuções
    ids_processados = carregar_dedup(cliente, cfg["bucket_lake"])
    print(f"Registro de deduplicacao: {len(ids_processados):,} ids conhecidos")

    validos = []
    metricas = {"lidos": 0, "validos": 0, "duplicados": 0, "malformados": 0}
    t0 = time.time()

    while True:
        try:
            resposta = subscriber.pull(
                request={"subscription": caminhos["sub"], "max_messages": 500},
                timeout=15,
            )
        except DeadlineExceeded:
            break   # backlog vazio: fim do consumo
        if not resposta.received_messages:
            break

        ack_ids = []
        for recebida in resposta.received_messages:
            metricas["lidos"] += 1
            try:
                ev = json.loads(recebida.message.data.decode("utf-8"))
            except json.JSONDecodeError:
                ev = None
            motivo = validar_evento(ev) if ev is not None else "JSON invalido"
            if motivo:
                publisher_dlq.publish(caminhos["dlq"], recebida.message.data,
                                      motivo=motivo)
                metricas["malformados"] += 1
            elif ev["id_evento"] in ids_processados:
                metricas["duplicados"] += 1
            else:
                ids_processados.add(ev["id_evento"])
                validos.append(ev)
                metricas["validos"] += 1
            ack_ids.append(recebida.ack_id)
        subscriber.acknowledge(request={"subscription": caminhos["sub"],
                                        "ack_ids": ack_ids})

    duracao = time.time() - t0
    arquivo = "(nada gravado)"

    if validos:
        agora = datetime.now(timezone.utc)
        df = pd.DataFrame(validos)
        df["_processing_timestamp"] = agora.isoformat()
        df["_source"] = f"pubsub:{TOPICO_EVENTOS}"
        arquivo = (f"gs://{cfg['bucket_lake']}/bronze/eventos_resultado_aluno/"
                   f"data_processamento={agora:%Y-%m-%d}/lote_{agora:%H%M%S}.parquet")
        df.to_parquet(arquivo, index=False,
                      storage_options={"token": credenciais})
        salvar_dedup(cliente, cfg["bucket_lake"], ids_processados)

    publisher_dlq.stop()
    subscriber.close()

    vazao = metricas["lidos"] / duracao if duracao > 0 else 0.0
    print()
    print("=" * 60)
    print("RESUMO DO CONSUMO")
    print(f"  Lidos:                {metricas['lidos']:,}")
    print(f"  Validos:              {metricas['validos']:,}")
    print(f"  Duplicados:           {metricas['duplicados']:,}")
    print(f"  Malformados (-> DLQ): {metricas['malformados']:,}")
    print(f"  Duracao:              {duracao:.1f}s ({vazao:,.0f} eventos/s)")
    print(f"  Micro-lote:           {arquivo}")
    print("  Status final:         SUCESSO")
    print("=" * 60)
    return 0


# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingestao streaming da pipeline (producer e consumer).")
    sub = parser.add_subparsers(dest="modo", required=True)

    p_pub = sub.add_parser("publicar",
                           help="simula o sistema externo publicando eventos")
    p_pub.add_argument("--eventos", type=int, default=100,
                       help="quantidade de eventos a publicar (padrao: 100)")

    sub.add_parser("consumir",
                   help="consome o backlog e grava micro-lote na Bronze")

    args = parser.parse_args()

    cfg = carregar_config()
    credenciais = obter_credenciais(cfg)
    caminhos = garantir_infraestrutura(cfg, credenciais)

    if args.modo == "publicar":
        return modo_publicar(cfg, credenciais, caminhos, args.eventos)
    return modo_consumir(cfg, credenciais, caminhos)


if __name__ == "__main__":
    sys.exit(main())
