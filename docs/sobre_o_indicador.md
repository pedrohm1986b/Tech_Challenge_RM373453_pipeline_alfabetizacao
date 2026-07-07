# Sobre o Indicador Criança Alfabetizada

Este documento aprofunda o funcionamento do Indicador Criança Alfabetizada, a métrica central deste projeto. O objetivo é registrar o entendimento de negócio que sustenta as decisões de modelagem da pipeline.

---

## 1. O que o indicador mede

O Indicador Criança Alfabetizada (ICA) expressa o **percentual de crianças do 2º ano do ensino fundamental consideradas alfabetizadas**. Ele foi criado no âmbito do Compromisso Nacional Criança Alfabetizada (CNCA), política do Ministério da Educação lançada em 2023 que articula União, estados, Distrito Federal e municípios em regime de colaboração. A experiência que inspirou a política é a do estado do Ceará, apontada pelo Banco Mundial como modelo de redução da pobreza de aprendizagem.

## 2. Como a nota de cada criança é calculada

A nota não corresponde ao número de acertos na prova. O processo tem quatro etapas:

**2.1. A prova.** Cada criança responde a uma avaliação de língua portuguesa composta por 16 itens de múltipla escolha e 3 itens de resposta construída, incluindo uma produção de texto. As provas são aplicadas pelas redes estaduais de ensino, com itens fornecidos pelo INEP, e existem diferentes versões de caderno (por isso os microdados registram a coluna `caderno`).

**2.2. A Teoria de Resposta ao Item (TRI).** A proficiência é calculada por TRI, o mesmo método utilizado no ENEM e no Saeb. Cada item tem uma dificuldade calibrada estatisticamente, e o modelo considera o padrão de respostas, não apenas a quantidade de acertos. Duas crianças com o mesmo número de acertos podem ter proficiências diferentes, dependendo da dificuldade dos itens que acertaram. A TRI posiciona alunos e itens na mesma escala, o que permite comparar resultados de cadernos e redes diferentes.

**2.3. A escala Saeb.** Os resultados são equalizados na escala de proficiência do Saeb, uma régua de unidade arbitrária. Os valores da escala não têm significado intrínseco; o que dá sentido a cada faixa é a descrição pedagógica do que uma criança naquele nível tipicamente consegue fazer. Nos microdados deste projeto, as proficiências observadas variam de aproximadamente 578 a 904 pontos.

**2.4. O ponto de corte de 743.** É considerada alfabetizada a criança com proficiência igual ou superior a 743 pontos. Esse valor não resulta de uma fórmula matemática: foi definido pela Pesquisa Alfabetiza Brasil (INEP, 2023) por meio de um processo de definição de padrão de desempenho (standard setting), no qual painéis de professores alfabetizadores de todo o país julgaram, a partir do desempenho real dos estudantes, quais habilidades caracterizam uma criança alfabetizada: ler e compreender pequenos textos, localizar informações explícitas, realizar inferências simples e escrever textos curtos do cotidiano, como bilhetes e relatos.

## 3. Verificação do corte nos microdados

A consistência entre a coluna `alfabetizado` e a coluna `proficiencia` dos microdados foi verificada durante a exploração dos dados deste projeto:

| Grupo | Menor proficiência | Maior proficiência |
|---|---|---|
| Não alfabetizado (0) | 578,5 | 743,0 |
| Alfabetizado (1) | 743,0 | 904,4 |

A fronteira entre os dois grupos ocorre exatamente em 743 pontos, confirmando que a classificação dos microdados aplica o critério oficial.

## 4. Relação entre os microdados e as tabelas consolidadas

Durante o levantamento das fontes, testou-se a hipótese de que as tabelas consolidadas (`municipio` e `uf`) seriam agregações diretas dos microdados de alunos. O teste recalculou a taxa de alfabetização de cada combinação de município, ano e rede a partir dos microdados, usando o peso amostral (`peso_aluno`), e comparou o resultado com o valor oficial:

| Métrica | Resultado |
|---|---|
| Combinações comparadas (município, ano, rede) | 12.408 |
| Diferença média | 0,036 ponto percentual |
| Coincidem em até 0,05 ponto percentual | 95,8% |

O teste confirma a relação na maior parte dos casos, mas revela duas limitações que impedem tratar a hipótese como regra:

1. **Cobertura incompleta dos microdados em 2023:** 643 municípios constam na tabela consolidada daquele ano sem nenhum registro correspondente nos microdados públicos (em 2024 a cobertura é completa). Os microdados disponibilizados, portanto, não são a base integral utilizada pelo INEP;
2. **Divergências residuais:** cerca de 4% das combinações apresentam diferenças de valor, sugerindo regras adicionais no cálculo oficial (critérios de participação, supressão de células pequenas ou tratamentos não documentados publicamente).

A consequência para a arquitetura do projeto: as tabelas consolidadas são tratadas como fonte oficial do histórico, e as agregações calculadas pela pipeline a partir de eventos no nível do aluno são identificadas como estimativas próprias, sem substituir os valores oficiais.

## 5. Resultados nacionais e metas pactuadas

Valores da tabela `meta_alfabetizacao_brasil` (rede pública):

| Ano | Taxa de alfabetização | Meta do ano | Participação |
|---|---|---|---|
| 2023 | 55,9% | linha de base | 86,0% |
| 2024 | 59,2% | 59,9% | 87,4% |
| 2025 | 66,0% | 64,0% | 88,0% |

As metas pactuadas seguem em progressão: aproximadamente 67% em 2026, 74% em 2028 e 80% em 2030. Cabe distinguir a meta numérica pactuada com as redes (80% em 2030) da aspiração da política, que é alfabetizar a totalidade das crianças. O percentual de participação também é relevante para a leitura do indicador, pois taxas calculadas sobre participação baixa podem não representar o conjunto dos estudantes.

## 6. Implicações para a pipeline

- A coluna `proficiencia` dos microdados permite recalcular o indicador em diferentes agregações, desde que utilizado o peso amostral (`peso_aluno`);
- Alunos ausentes na prova possuem proficiência nula e são classificados como não alfabetizados, o que exige tratamento explícito nas análises (a média simples de proficiência ignora nulos e pode inflar resultados);
- As metas são fornecidas em formato wide (colunas `meta_alfabetizacao_2024` a `meta_alfabetizacao_2030`) e serão convertidas para formato long na camada Silver, permitindo a comparação direta entre meta e resultado por ano;
- A coluna `rede` das tabelas agregadas mistura redes individuais e agregados (Total, Pública), exigindo filtro explícito para evitar dupla contagem.

## 7. Referências

- INEP. [Avaliação da Alfabetização](https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/avaliacao-da-alfabetizacao).
- INEP. [Inep divulga dados do Indicador Criança Alfabetizada por município](https://www.gov.br/inep/pt-br/centrais-de-conteudo/noticias/avaliacao-da-alfabetizacao/inep-divulga-dados-do-indicador-crianca-alfabetizada-por-municipio). Mar. 2026.
- INEP. [Brasil e 20 unidades da Federação alcançam meta de alfabetização](https://www.gov.br/inep/pt-br/centrais-de-conteudo/noticias/avaliacao-da-alfabetizacao/brasil-e-20-unidades-da-federacao-alcancam-meta-de-alfabetizacao). Mar. 2026.
- IBGE. [PNAD Contínua: rendimento de todas as fontes, ano-base 2025](https://agenciadenoticias.ibge.gov.br/agencia-noticias/2012-agencia-de-noticias/noticias/46579-rendimento-medio-da-populacao-brasileira-atinge-r-3-367-em-2025). 2026.
- INSPER; FUNDAÇÃO ROBERTO MARINHO. [Consequências da Violação do Direito à Educação](https://www.insper.edu.br/wp-content/uploads/2021/05/Conseque%CC%82ncias-da-Violac%CC%A7a%CC%83o-do-Direito-a%CC%80-Educac%CC%A7a%CC%83o.pdf). Ricardo Paes de Barros; Laura Machado. 2020.
- BANCO MUNDIAL. [The State of Global Learning Poverty: 2022 Update](https://www.worldbank.org/pt/news/press-release/2022/06/23/70-of-10-year-olds-now-in-learning-poverty-unable-to-read-and-understand-a-simple-text). 2022.
- BANCO MUNDIAL. [The State of Ceará in Brazil is a Role Model for Reducing Learning Poverty](https://documents1.worldbank.org/curated/en/200981594196175640/pdf/The-State-of-Ceara-in-Brazil-is-a-Role-Model-for-Reducing-Learning-Poverty.pdf). 2020.
- BASE DOS DADOS. [Dataset Avaliação da Alfabetização](https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72?table=e1de7a6a-5038-4e81-89f0-a15f2cc12c9b).
