# Contrato do evento: resultado de aluno (v1)

Define o payload dos eventos publicados no tópico de streaming da pipeline. O evento representa um resultado individual de avaliação processado pela rede estadual de ensino, no cenário simulado do ciclo de 2025 (decisão D-005 do diário).

Este documento é o contrato entre o producer (sistema de avaliação simulado) e o consumer (pipeline). Alterações de estrutura exigem incremento de `schema_version` e registro no diário de decisões.

---

## Exemplo

```json
{
  "id_evento": "a3f8c2e1-7b4d-4e2a-9c1f-5d8e6a0b3c72",
  "schema_version": 1,
  "event_time": "2025-11-14T10:32:00Z",
  "id_municipio": "3550308",
  "rede": "3",
  "id_aluno": "A2025-000184732",
  "proficiencia": 751.2,
  "peso_aluno": 1.04
}
```

## Campos

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `id_evento` | string (UUID) | sim | Identidade única do evento. É a chave da deduplicação no consumer: eventos com `id_evento` já processado são descartados (semântica at-least-once com consumidor idempotente) |
| `schema_version` | inteiro | sim | Versão deste contrato. O consumer rejeita versões que não conhece |
| `event_time` | string (ISO 8601, UTC) | sim | Momento em que o resultado foi produzido no sistema de origem. Distinto do processing time, registrado pelo consumer na gravação |
| `id_municipio` | string (7 dígitos, código IBGE) | sim | Município da escola do aluno. É a ordering key do tópico: eventos do mesmo município são entregues em ordem |
| `rede` | string | sim | Dependência administrativa da escola (códigos da fonte: 1 a 4) |
| `id_aluno` | string | sim | Código anonimizado do aluno, no padrão da fonte |
| `proficiencia` | número | sim | Proficiência em língua portuguesa na escala Saeb |
| `peso_aluno` | número | sim | Peso amostral do aluno, conforme calculado pelo INEP. Necessário para as agregações ponderadas da camada Silver |

## Regras

1. **Campo `alfabetizado` não existe no evento:** é derivável (`proficiencia >= 743`) e enviá-lo abriria a possibilidade de inconsistência entre o valor declarado e o derivado. A classificação é calculada pela pipeline, em uma única fonte de verdade;
2. **Eventos malformados** (campo obrigatório ausente, tipo inválido, `schema_version` desconhecida) são encaminhados pelo consumer à dead letter queue, sem bloquear o fluxo;
3. **Duplicatas** (mesmo `id_evento`) são descartadas pelo consumer, que mantém o registro dos eventos já processados;
4. **Origem dos valores na simulação:** o producer lê os microdados reais da camada Bronze (2023 e 2024) e gera resultados plausíveis para o ciclo de 2025, preservando as distribuições de proficiência e peso da fonte.

## Referências

- Decisões D-005 (streaming no nível do aluno) e D-009 (Pub/Sub) no [diário de decisões](../../docs/decisoes.md);
- Conceitos de contrato e versionamento de schema: módulo Fase 2, ETL Pipelines, Aula 2 (governança de schemas em streaming).
