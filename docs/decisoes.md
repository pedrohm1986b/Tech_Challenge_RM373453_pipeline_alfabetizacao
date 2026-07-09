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

## D-005 · Classificação das fontes: batch para consolidados, streaming no nível do aluno

**Data:** 07/07/2026 · **Etapa:** Arquitetura

**Decisão:** a ingestão batch cobre as publicações consolidadas, passadas e futuras: diretório de municípios, metas pactuadas, o histórico do indicador e dos microdados de 2023 e 2024, e os consolidados oficiais que vierem a ser divulgados. O streaming simulado opera no nível do aluno, reproduzindo a chegada contínua de resultados da avaliação de 2025, que a pipeline agrega para compor uma estimativa preliminar do indicador por município e UF, utilizada apenas onde ainda não existe dado oficial publicado e substituída quando ele chegar. *(Redação atualizada após os refinamentos das PRs #4 e #5.)*

**Contexto:** o enunciado exige ingestão híbrida e cita como exemplos de eventos a atualização de indicadores e novas medições de desempenho, mas a definição de qual fonte flui por qual modo é uma decisão de arquitetura do time.

**Justificativa:** o critério adotado é a dinâmica natural de produção de cada dado. Cadastros e metas mudam raramente e são publicados de forma consolidada; resultados de avaliação nascem continuamente durante o período de aplicação das provas. Verificação empírica mostrou que as tabelas agregadas se comportam majoritariamente como agregações dos microdados (95,8% das taxas municipais coincidem quando recalculadas a partir dos alunos com peso amostral), mas a relação não é completa: em 2023, 643 municípios constam no consolidado sem microdados públicos correspondentes. Por isso, o consolidado oficial permanece como fonte de verdade do histórico (batch), e a agregação em fluxo produz uma visão atualizada do indicador identificada como estimativa da pipeline, sem substituir os valores oficiais. O streaming no nível do aluno é o cenário mais fiel à realidade e ainda exercita agregação em fluxo, um dos objetivos de aprendizado do projeto.

**Alternativas consideradas:** simular eventos de atualização do indicador municipal já consolidado. Descartada por ser menos fiel à dinâmica real, já que indicadores consolidados são publicados em lote, e por empobrecer o papel da pipeline, que apenas substituiria valores em vez de calcular a agregação.

---

## D-006 · Separação entre código e dados: repositório versiona código, dados vivem no data lake

**Data:** 07/07/2026 · **Etapa:** Arquitetura

**Decisão:** o repositório Git contém exclusivamente código, configuração e documentação. Os dados de todas as camadas do medalhão (Bronze, Silver, Gold e quarentena) residem no data lake, em bucket do Google Cloud Storage, e não são versionados no Git.

**Contexto:** ao estruturar o repositório, discutiu-se onde as camadas do medalhão deveriam viver, incluindo a hipótese de subpastas locais no projeto.

**Justificativa:** Git é adequado para arquivos de texto pequenos com diff legível; dados são volumosos e binários (só a tabela de alunos tem 256 MB), e o histórico dos dados é responsabilidade do próprio lake, com a Bronze preservando tudo que chegou. A separação entre computação e armazenamento é o princípio central das arquiteturas modernas de dados em nuvem, e mantém o repositório leve e clonável por qualquer avaliador.

**Alternativas consideradas:** manter amostras de dados no repositório. Descartada como regra geral; poderá ser usada pontualmente para dados de teste pequenos, se necessário.

---

## D-007 · Diagrama de arquitetura como código (Mermaid no README)

**Data:** 07/07/2026 · **Etapa:** Arquitetura

**Decisão:** o diagrama da pipeline é escrito em Mermaid, dentro do próprio README, e evolui por Pull Request como qualquer outro artefato do projeto.

**Contexto:** o enunciado exige um diagrama da pipeline no README. As opções avaliadas foram ferramentas visuais (draw.io, Excalidraw), que geram arquivos de imagem, e diagrama como código.

**Justificativa:** o GitHub renderiza Mermaid nativamente, o diagrama fica versionado como texto (com diff legível nas revisões) e não há risco de a imagem exportada ficar dessincronizada do fonte editável. O diagrama já passou por duas rodadas de revisão em PR, o que valida o fluxo.

**Alternativas consideradas:** draw.io com imagem exportada em `docs/`. Descartada pela manutenção dupla (fonte + imagem) e por não registrar a evolução do desenho no diff das PRs.

---

## D-008 · Convenção de desenvolvimento e produção: notebooks `desenv_`, scripts `prod_`

**Data:** 09/07/2026 · **Etapa:** Ingestão batch

**Decisão:** cada componente da pipeline é desenvolvido em um notebook em `notebooks/`, com o prefixo `desenv_`, contendo a documentação célula a célula (passos, conceitos com referência às aulas e evidências de execução). Quando o código é validado, ele é promovido para um script em `src/`, com o prefixo `prod_` e o mesmo nome-base. Após o prefixo, um número sequencial ordena os artefatos na visualização das pastas (exemplo: `desenv_01_ingestao_batch.ipynb` e `prod_01_ingestao_batch.py`; o levantamento de fontes, sem par de produção, recebe o número 00). Os cabeçalhos dos dois arquivos referenciam o par correspondente, e o README mantém a tabela de correspondência entre desenvolvimento, produção e etapa do roadmap.

**Contexto:** o desenvolvimento em notebook favorece a cocriação e o registro didático, mas a versão executada pela orquestração precisa ser um script. Era necessário um padrão de nomes que deixasse a correspondência evidente para o avaliador.

**Justificativa:** o prefixo explicita o estágio do artefato e o nome-base comum estabelece o vínculo entre os pares. A promoção do notebook validado para script funciona como revisão final do código e reflete prática comum de mercado (prototipação em notebook, produção em módulo).

**Alternativas consideradas:** numeração antes do prefixo (`01_prod_...`), descartada porque módulos Python não podem iniciar com dígito, o que inviabilizaria a importação dos scripts pela orquestração (a numeração após o prefixo preserva a ordenação e a importabilidade); nomes sem prefixo e sem número, descartados por não explicitarem o estágio nem a ordem dos artefatos.

---

## Decisões pendentes

Os identificadores são atribuídos apenas quando a decisão é tomada, para evitar renumerações.

| Tema | Sessão prevista |
|---|---|
| Rede de ensino de referência para as análises (Total, Pública ou individuais) | 1.2 |
| Tratamento dos alunos ausentes (proficiência nula) nas análises | 1.2 |
| Mensageria do streaming (Pub/Sub ou Kafka) | 2.2 |
| Motor de processamento da camada Silver (PySpark ou SQL no BigQuery) | 2.2 |
| Contrato do evento de streaming (payload, campos, versionamento) e diagrama do fluxo do dado | 4.1 |
| Ferramenta de orquestração | 7.1 |

---

## Discussões futuras

Ideias avaliadas e não adotadas agora, registradas com as condições que justificariam retomá-las.

### F-001 · Derivação dos agregados a partir dos microdados (potencial economia)

**Contexto:** a verificação da D-005 mostrou que 95,8% das taxas municipais são reproduzíveis a partir dos microdados de alunos com o peso amostral. Se a pipeline derivasse as agregações por conta própria, a ingestão das tabelas consolidadas (`uf`, `municipio`) poderia ser dispensada, reduzindo consultas à fonte, volume armazenado e dependência do calendário de publicação do INEP.

**Por que não agora:** a derivação não reproduz o dado oficial na totalidade. Em 2023, 643 municípios constam no consolidado sem microdados públicos correspondentes, e cerca de 4% dos valores divergem por regras do cálculo oficial não documentadas publicamente. Substituir a fonte pelo derivado criaria um ponto cego exatamente nos municípios menores.

**Gatilhos para retomar:** cobertura completa dos microdados nas próximas edições, ou documentação oficial do método de cálculo que permita reproduzir os refinamentos do INEP.
