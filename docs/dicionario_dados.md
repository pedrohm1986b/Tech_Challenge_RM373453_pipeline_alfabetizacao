# Dicionário de Dados — Avaliação da Alfabetização (INEP / Base dos Dados)

> Entregável da **Fase 1 (Exploração dos dados)** do plano de desenvolvimento.
> Fonte: BigQuery público `basedosdados.br_inep_avaliacao_alfabetizacao` · Explorado em 05/07/2026 via projeto `tech-challenge-fase2`.

---

## 1. Visão geral das tabelas

| Tabela | Linhas | Tamanho | Anos | Granularidade (chave única verificada) |
|---|---:|---:|---|---|
| `alunos` | 3.867.999 | 256,1 MB | 2023–2024 | 1 linha por **aluno avaliado** (`ano`, `id_aluno`) |
| `municipio` | 23.995 | 1,75 MB | 2023–2024 | (`ano`, `id_municipio`, `serie`, `rede`) |
| `meta_alfabetizacao_municipio` | 10.704 | 1,10 MB | 2023–2024 | (`ano`, `id_municipio`, `rede`) |
| `uf` | 145 | 0,01 MB | 2023–2024 | (`ano`, `sigla_uf`, `serie`, `rede`) |
| `meta_alfabetizacao_uf` | 81 | 0,01 MB | **2023–2025** | (`ano`, `sigla_uf`, `rede`) |
| `meta_alfabetizacao_brasil` | 3 | <0,01 MB | **2023–2025** | (`ano`, `rede`) — 1 linha/ano |
| `dicionario` | 27 | <0,01 MB | — | dicionário de códigos das colunas categóricas |

**Volume total: ~260 MB** — desenvolvimento inteiro cabe com folga no free tier do BigQuery (1 TB de consulta/mês).

---

## 2. Schemas

### 2.1 `uf` — Indicador por UF
| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | INT64 | Ano de aplicação da avaliação estadual |
| `sigla_uf` | STRING | Sigla da UF |
| `serie` | STRING | Ano escolar (sempre `2` = 2º ano EF) |
| `rede` | STRING | Rede de ensino (código — ver dicionário §3) |
| `taxa_alfabetizacao` | FLOAT64 | % de alunos considerados alfabetizados (**o Indicador Criança Alfabetizada**) |
| `media_portugues` | FLOAT64 | Média ponderada em Língua Portuguesa, escala equalizada ao Saeb |
| `proporcao_aluno_nivel_0` … `_8` | FLOAT64 | % de alunos por nível de desempenho (0 a 8) |

### 2.2 `municipio` — Indicador por município
Mesma estrutura da `uf`, trocando `sigla_uf` por `id_municipio` (STRING, código IBGE de 7 dígitos).

### 2.3 `meta_alfabetizacao_brasil` / `meta_alfabetizacao_uf` / `meta_alfabetizacao_municipio`
| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | INT64 | Ano da avaliação |
| (`sigla_uf` / `id_municipio`) | STRING | Chave geográfica (ausente na tabela Brasil) |
| `rede` | STRING | Rede de ensino |
| `taxa_alfabetizacao` | FLOAT64 | Taxa observada no ano |
| `meta_alfabetizacao_2024` … `meta_alfabetizacao_2030` | FLOAT64 | **Metas anuais em colunas separadas** (formato wide) |
| `nivel_alfabetizacao` | INT64 | *(só na tabela município)* nível de alfabetização |
| `percentual_participacao` | FLOAT64 | % de participação na avaliação |

### 2.4 `alunos` — Microdados (nível aluno)
| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | INT64 | Ano de aplicação |
| `id_municipio` | STRING | Município (IBGE 7 dígitos) |
| `id_escola` | STRING | **Máscara fictícia** (não cruza com Censo Escolar) |
| `id_aluno` | STRING | Código do aluno |
| `caderno` | STRING | Caderno da prova de LP |
| `serie` | STRING | Ano escolar (`2`) |
| `rede` | STRING | Dependência administrativa |
| `presenca` | STRING | 0 = Ausente, 1 = Presente |
| `preenchimento_caderno` | STRING | 0 = Não preenchida, 1 = Preenchida |
| `alfabetizado` | STRING | 0 = Não, 1 = Sim |
| `proficiencia` | FLOAT64 | Proficiência em LP (escala Saeb; corte = 743) |
| `peso_aluno` | FLOAT64 | Peso amostral do aluno |

### 2.5 `dicionario`
Colunas: `id_tabela`, `nome_coluna`, `chave`, `cobertura_temporal`, `valor`. Decodifica os códigos categóricos (§3).

---

## 3. Códigos categóricos (tabela `dicionario`)

**`rede`** (em `uf` e `municipio`):
| Código | Valor |
|---|---|
| 0 | Total (Federal, Estadual, Municipal e Privada) |
| 1 | Federal |
| 2 | Estadual |
| 3 | Municipal |
| 4 | Privada |
| 5 | Pública (Estadual e Municipal) |
| 6 | Pública (Federal, Estadual e Municipal) |

**`rede`** (em `alunos`): apenas 1–4 (sem agregados 0/5/6).
**`serie`**: 2 = 2º ano do Ensino Fundamental (constante).
**`alfabetizado` / `presenca` / `preenchimento_caderno`**: 0 = Não/Ausente, 1 = Sim/Presente.

⚠️ As tabelas agregadas (`uf`, `municipio`) misturam redes individuais (1–4) e agregados (0, 5, 6) — **filtrar a rede correta é obrigatório** para não somar dados duplicados.

---

## 4. Relacionamentos e integridade (verificados)

```
uf (indicador)                    municipio (indicador)
   │ (ano, sigla_uf)                 │ (ano, id_municipio)
   ▼                                 ▼
meta_alfabetizacao_uf             meta_alfabetizacao_municipio
                                     │
                                     ▼ (id_municipio)
alunos ──(id_municipio)──► br_bd_diretorios_brasil.municipio (nome, UF, região…)
```

| Verificação | Resultado |
|---|---|
| Duplicidade nas chaves candidatas (4 tabelas agregadas) | **0 duplicatas** — chaves únicas confirmadas |
| `municipio.id_municipio` × diretório IBGE (`br_bd_diretorios_brasil.municipio`) | 5.550 municípios distintos, **0 órfãos** |
| `meta_alfabetizacao_municipio.id_municipio` × `municipio` | 5.352 municípios, **0 órfãos** (todos têm indicador) |

Observações:
- O Brasil tem 5.570 municípios; o indicador cobre **5.550** e as metas municipais, **5.352** — a diferença (municípios sem dados/sem meta) deve ser tratada como *left join* consciente, não como erro;
- O **diretório de municípios da Base dos Dados** (`br_bd_diretorios_brasil.municipio`) é a tabela de enriquecimento natural para nome do município, UF e região — juntar na Silver.

---

## 5. Achados de qualidade (insumos para as regras da Fase 5)

1. **`proporcao_aluno_nivel_0..8` são 100% nulas em 2023** (11.547 linhas da `municipio` — exatamente todas as de 2023). As proporções por nível só existem em **2024**. → Regra de completude condicional por ano; não tratar como erro em 2023.
2. **`proficiencia` nula em ~25% dos alunos não alfabetizados** (244.630 em 2023; 268.708 em 2024) — presumivelmente alunos **ausentes** (`presenca=0`) classificados como não alfabetizados. Alfabetizados (=1) têm 0 nulos. → Validar cruzamento `presenca` × `proficiencia`; decidir tratamento (excluir ausentes do cálculo? manter com flag?). Cuidado com o viés do `AVG` ignorando nulos.
3. **`taxa_alfabetizacao` varia de 2,12 a 100,0** e não tem nulos nas tabelas agregadas → range check [0,100] passa.
4. **Metas em formato wide** (`meta_alfabetizacao_2024`…`_2030` em colunas) → **unpivot/melt na Silver** para formato long (`ano_meta`, `valor_meta`), viabilizando o dataset Gold "metas × resultados" por ano.
5. **`meta_alfabetizacao_uf` e `_brasil` já têm dados de 2025** (taxa observada), mas `uf`/`municipio`/`alunos` param em 2024 → confirma o cenário de streaming: "resultados de 2025 chegando como eventos" para as tabelas de indicador.
6. **Balanceamento do `alfabetizado`** (microdados): 2023 ≈ 50,2% alfabetizados; 2024 ≈ 52,2% — nota: difere da taxa oficial ponderada (56%/59%), pois a oficial usa `peso_aluno`. → Cálculos próprios devem usar o peso.
7. **`id_escola` é máscara fictícia** → não serve para cruzar com Censo Escolar; enriquecimento externo só por `id_municipio`.

---

## 6. Implicações para o desenho da pipeline

- **Chaves de integração:** `id_municipio` (IBGE 7 díg., STRING), `sigla_uf`, `ano`, `rede` — padronizar tipos na Silver (manter `id_municipio` como STRING preservando zeros à esquerda);
- **Filtro de rede:** definir a rede de análise (provável: `0` Total ou `6` Pública) e documentar;
- **Particionamento sugerido:** por `ano` nas camadas Silver/Gold (2 a 3 partições hoje; escala com novas edições);
- **Volume:** `alunos` é a única tabela relevante em tamanho (256 MB) — ainda assim pequena; a discussão de escalabilidade no README usa o cenário "e se virar microdado nacional censitário?";
- **Streaming:** simular eventos de novas medições 2025 para `municipio`/`uf` (o dado real de UF/Brasil 2025 já existe nas tabelas de meta — pode ancorar a simulação em valores plausíveis).
