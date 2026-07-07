# Diário de Decisões

Registro das decisões relevantes do projeto, com contexto e justificativa. Cada decisão indica a data, a escolha realizada e as alternativas consideradas. Este documento alimenta as seções de trade-offs do README e a apresentação executiva.

---

## D-001 · Nuvem do projeto: Google Cloud Platform (GCP)

**Data:** 05/07/2026 · **Etapa:** Fundação

**Decisão:** implementar a pipeline na GCP.

**Contexto:** o enunciado permite AWS, GCP ou Azure. A fonte de dados do projeto (dataset Avaliação da Alfabetização, da Base dos Dados) é distribuída como conjunto público no BigQuery, serviço nativo da GCP.

**Justificativa:** utilizar a mesma nuvem em que a fonte reside elimina a movimentação inicial de dados entre provedores, reduz custos de transferência e simplifica a autenticação. O free tier do BigQuery (1 TB de consulta por mês) e do Cloud Storage cobre o volume do projeto (cerca de 260 MB) com folga.

**Alternativas consideradas:** AWS e Azure atenderiam aos requisitos, mas exigiriam extrair os dados do BigQuery para outro provedor, adicionando uma etapa de movimentação sem benefício técnico para este caso.

---

## D-002 · Extração programática via BigQuery, sem download manual

**Data:** 06/07/2026 · **Etapa:** Fundação

**Decisão:** a ingestão batch consulta as tabelas públicas do BigQuery diretamente pelo código da pipeline, em vez de partir de arquivos baixados manualmente do site da Base dos Dados.

**Contexto:** o enunciado determina que as fontes sejam obtidas na plataforma Base dos Dados, sem especificar o método. A plataforma oferece download manual em CSV e acesso programático via BigQuery.

**Justificativa:** a extração programática torna a pipeline reprodutível de ponta a ponta (qualquer pessoa executa o código e obtém os mesmos dados), elimina passos manuais fora do versionamento e reflete a prática profissional de ingestão. O download manual permanece como recurso de conferência pontual.

---

## D-003 · Repositório público com README evolutivo

**Data:** 07/07/2026 · **Etapa:** Fundação

**Decisão:** repositório público no GitHub, com README construído de forma incremental: as seções são preenchidas conforme as etapas do projeto avançam, e as pendentes ficam marcadas como em construção.

**Contexto:** o enunciado avalia o uso adequado de Git, incluindo histórico de commits que evidencie a evolução do pipeline, branches e Pull Requests com discussão.

**Justificativa:** o repositório público facilita a avaliação pelos professores. O README evolutivo evita documentar funcionalidades inexistentes e faz o histórico de commits contar a construção real do projeto.

---

## D-004 · Fluxo de trabalho: branch por funcionalidade e Pull Request

**Data:** 07/07/2026 · **Etapa:** Fundação

**Decisão:** todo trabalho novo nasce em uma branch própria (convenção `tipo/descricao-curta`, por exemplo `feature/ingestao-batch` e `docs/diario-de-decisoes`) e chega à branch `main` por meio de Pull Request com descrição e discussão.

**Contexto:** requisito explícito do enunciado.

**Justificativa:** além de atender ao critério de avaliação, o fluxo cria um registro rastreável do porquê de cada mudança e permite revisão antes da integração.

---

## Decisões pendentes

| ID previsto | Tema | Sessão prevista |
|---|---|---|
| D-005 | Rede de ensino de referência para as análises (Total, Pública ou individuais) | 1.2 |
| D-006 | Tratamento dos alunos ausentes (proficiência nula) nas análises | 1.2 |
| D-007 | Mensageria do streaming (Pub/Sub ou Kafka) | 2.2 |
| D-008 | Motor de processamento da camada Silver (PySpark ou SQL no BigQuery) | 2.2 |
| D-009 | Ferramenta de orquestração | 7.1 |
