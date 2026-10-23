# NovaRota - Desafio Engenharia de Dados

Pipeline de processamento de movimentações de cartões desenvolvido com Python, PySpark, Delta Lake e Databricks, seguindo a arquitetura Medallion (Bronze, Prata e Ouro).

O projeto utiliza dados sintéticos para simular situações comuns na engenharia de dados, como registros duplicados, informações inválidas, arquivos reenviados, correções de dados e estornos de transações. O objetivo é construir uma base confiável para análise, mantendo a rastreabilidade do que foi recebido da origem.

## 1. Tecnologias e ambiente

- **Python / PySpark:** geração da massa, ingestão e transformações.
- **Databricks:** execução dos notebooks e processamento Spark.
- **Delta Lake:** armazenamento transacional, `append`, `MERGE` e histórico das tabelas.
- **Unity Catalog:** organização das tabelas e do Volume por catálogo e schema.
- **SQL:** consultas analíticas, funções de janela e validações.
- **pytest:** testes automatizados das principais regras.

O projeto foi executado com o catálogo `novarota`, nos schemas `bronze`, `prata` e `ouro`. Os arquivos de entrada são armazenados em:

```text
/Volumes/novarota/bronze/raw/
```

## 2. Estrutura do repositório

```text
desafio_engenharia_dados/
├── main.py
├── scripts/
│   ├── gerar_massa.py

├── src/
│   ├── config.py
│   ├── ingestion/
│   │   └── bronze.py
│   ├── transformations/
│   │   ├── silver.py
│   │   └── gold.py
│   ├── quality/
│   │   └── rules.py
│   └── utils/
 delta.py
│       ├── versoes.py
│       └── logging.py
├── tests/
├── sql/
│   ├── consultas_analiticas.sql
│   ├── validacoes.sql
│   └── demo_sql_janelas.sql
├── notebooks/
│   ├── 01_executar_pipeline
│   ├── 02_validacao_pipeline
│   ├── 03_executar_testes
│   ├── 04_revisao_regras_negocio
│   └── 05_consultas_analiticas
├── docs/
│   └── evidencias/
│       ├── anonimizar_html_databricks_v2.py
│       ├── 01_executar_pipeline.html
│       ├── 02_validacao_pipeline.html
│       ├── 03_executar_testes.html
│       ├── 04_revisao_regras_negocio.html
│       └── 05_consultas_analiticas.html
├── requirements-dev.txt
└── README.md
```

## 3. Visão geral do pipeline

```text
Arquivos CSV (Unity Catalog Volume)
             |
             v
       BRONZE — Delta
 Dados brutos + metadados
 Versões de arquivos via append
             |
             v
   Seleção do lote vigente
        por arquivo
             |
             v
        PRATA — Delta
 Tipagem, deduplicação,
 regras de qualidade e FKs
        /          \
       v            v
 Dados válidos   Quarentena
       |            |
       +------v-----+
              |
              v
         OURO — Delta
 Transações financeiras,
 resumo mensal, ranking e
 alertas de qualidade
```

O arquivo `main.py` coordena as três etapas. A Bronze faz a ingestão dos arquivos, a Prata trata e valida os registros e a Ouro gera as tabelas para análise. As etapas de Prata e Ouro são recalculadas a partir do conjunto consolidado de dados, e não somente a partir das linhas recebidas no dia.

### 3.1 Bronze: preservação dos dados originais

Optei por manter os campos de negócio como strings na Bronze e não aplicar deduplicação nessa etapa. Assim, consigo verificar quantos registros chegaram, inclusive cópias idênticas ou linhas que serão rejeitadas posteriormente. A deduplicação e as validações ficam concentradas na Prata.

Cada linha recebe metadados que ajudam na rastreabilidade:

| Campo | Finalidade |
|---|---|
| `arquivo_origem` | Identifica o CSV de origem. |
| `arquivo_checksum` | SHA-256 do conteúdo completo do arquivo. |
| `id_ingestao` | Identifica o lote de recebimento. |
| `timestamp_ingestao` | Registra o horário da ingestão. |
| `data_referencia` | Identifica a data informada à execução. |
| `hash_linha` | SHA-256 calculado a partir dos campos de negócio da linha. |

O checksum do arquivo inteiro é usado no controle de reprocessamento; o hash de cada linha serve para comparar registros. Não utilizo `(arquivo_origem, hash_linha)` como chave única da Bronze, porque duas linhas idênticas de um mesmo CSV precisam continuar representadas.


O comportamento pretendido para cada arquivo é:

1. **Arquivo novo:** gravar todas as suas linhas em um novo lote.
2. **Mesmo conteúdo da versão vigente:** ignorar o reprocessamento, sem duplicar dados.
3. **Conteúdo corrigido:** acrescentar um novo lote com outro `id_ingestao`, preservando o anterior.
4. **Retorno a um conteúdo antigo:** tratar como um novo recebimento quando ele for diferente da versão vigente, mesmo que o checksum já tenha aparecido no histórico.

### 3.2 Seleção da versão vigente

Como a Bronze pode armazenar diferentes versões de um mesmo arquivo, a Prata não deve processar todas elas ao mesmo tempo.

Para isso, criei `src/utils/versoes.py`, com a função `selecionar_versoes_vigentes()`. Ela agrupa as linhas por `arquivo_origem` e `id_ingestao`, identifica o lote mais recente com `timestamp_ingestao` e usa `ROW_NUMBER()` para selecionar **um lote completo por arquivo**.

As linhas duplicadas dentro do lote selecionado continuam presentes nessa etapa: a deduplicação por chave de negócio acontece posteriormente. Também existe compatibilidade com registros legados que ainda não possuem `id_ingestao`: eles são tratados como um lote identificado pelo arquivo de origem.

Essa seleção é realizada em `executar_prata()`, antes de chamar as funções de transformação de cada entidade.


### 3.3 Prata: qualidade e consolidação

A Prata transforma os campos para os tipos adequados, normaliza valores, verifica relacionamentos e mantém apenas um registro válido por chave de negócio. Os registros rejeitados são enviados para `novarota.prata.quarentena`, junto com o motivo do erro.

| Entidade | Principais tratamentos |
|---|---|
| **Clientes** | CPF, campos obrigatórios, datas e escolha da versão mais atual por `id_cliente`. |
| **Contas** | Tipos e status permitidos, data de abertura e vínculo com cliente válido. |
| **Cartões** | Status, tipo, datas e vínculo com conta válida. |
| **Transações** | Data, valor positivo, estabelecimento, MCC e vínculo com cartão válido. |
| **Estornos** | Data e motivo obrigatórios, vínculo com transação válida e verificação cronológica. |

O processamento respeita a ordem de dependências: **clientes → contas → cartões → transações → estornos**. Essa sequência permite validar as chaves estrangeiras usando os DataFrames já tratados.

Para consolidar as tabelas Prata, utilizo `MERGE` Delta com o snapshot completo de cada entidade, incluindo `WHEN NOT MATCHED BY SOURCE DELETE`. Assim, se um registro deixar de existir na versão vigente de um arquivo, ele também poderá deixar de existir na Prata. Essa exclusão **não** é aplicada à Bronze.

A quarentena também é recomposta a partir dos dados vigentes. Portanto, ela representa as rejeições da situação atualmente processada, e não um histórico imutável de todos os erros de todas as versões dos arquivos. A lógica foi centralizada em uma tabela de quarentena, para simplificar manutenção, auditoria e criação dos alertas, tendo a coluna `entidade` para identificar a origem do dado.


### 3.4 Regra cronológica dos estornos

Durante a análise dos dados, identifiquei estornos com data anterior à transação correspondente. Acrescentei a validação:

```text
data_estorno >= data_transacao (comparada como data, sem horário)
```

Quando a condição não é atendida, o estorno vai para a quarentena com o motivo `ESTORNO_ANTERIOR_TRANSACAO`. Também são rejeitados estornos que apontem para transações inexistentes ou inválidas.

Um estorno válido mantém a transação na Prata para rastreabilidade, marcando-a como `estornada = true`. A transação estornada não entra na visão financeira da Ouro.

A comparação é feita por **data civil**, pois o CSV de estornos não contém horário. Ela não demonstra a ordem exata dos eventos dentro de um mesmo dia.

### 3.5 Ouro: indicadores analíticos

A camada Ouro disponibiliza quatro tabelas:

| Tabela | Conteúdo |
|---|---|
| `gold_transacoes_cartao` | Transações válidas e não estornadas, relacionadas a clientes, contas e cartões. |
| `gold_resumo_cliente_mes` | Quantidade, valor total, ticket médio e cartões utilizados por cliente/mês. |
| `gold_ranking_estabelecimento` | Ranking de estabelecimentos por mês e cidade. |
| `gold_alertas_qualidade` | Quantidade de rejeições agrupadas por entidade, motivo e data de referência. |

Mantive os status cadastrais de contas e cartões como atributos informativos. Não excluo automaticamente uma transação apenas porque o cartão consta hoje como cancelado/bloqueado ou a conta como encerrada/bloqueada: os arquivos não fornecem o instante efetivo dessas mudanças, necessário para determinar o status na data da compra.

## 4. Massa sintética e execução

1. `01_executar_pipeline` — ingestão e execução das três camadas.
2. `02_validacao_pipeline` — contagens, duplicatas, idempotência e reconciliação.
3. `03_executar_testes` — testes automatizados.
4. `04_revisao_regras_negocio` — status, estornos e consistência financeira.
5. `05_consultas_analiticas` — consultas e funções de janela SQL.

## 5. Consultas SQL e validação

A pasta `sql/` concentra consultas para análises e conferência das tabelas. Foram utilizados `CTE`, `ROW_NUMBER`, `LAG`, `NTILE(4)` e `DENSE_RANK`, incluindo crescimento mensal acima de 50%, quartis e ranking por estabelecimento.

Os arquivos de transações são recebidos em março de 2025, mas algumas transações possuem **data de negócio em fevereiro**, simulando atraso no recebimento. Por isso os indicadores incluem fevereiro e março. Os meses têm coberturas diferentes; os percentuais de crescimento são apenas demonstrativos.

Exemplos de verificações:

```sql
-- Distribuição dos registros por arquivo na Bronze
SELECT arquivo_origem, COUNT(*) AS registros
FROM novarota.bronze.transacoes
GROUP BY arquivo_origem
ORDER BY arquivo_origem;

-- Reconciliação financeira: Prata x Ouro
SELECT
  (SELECT COUNT(*) FROM novarota.prata.transacoes WHERE estornada = FALSE) AS qtd_prata,
  (SELECT COUNT(*) FROM novarota.ouro.gold_transacoes_cartao) AS qtd_ouro,
  (SELECT SUM(valor) FROM novarota.prata.transacoes WHERE estornada = FALSE) AS valor_prata,
  (SELECT SUM(valor) FROM novarota.ouro.gold_transacoes_cartao) AS valor_ouro;

-- Histórico das operações Delta
DESCRIBE HISTORY novarota.prata.transacoes;
```

Para visualizar lotes distintos após ocorrer uma nova ingestão (quando `id_ingestao` existir no esquema):

```sql
SELECT arquivo_origem, id_ingestao, arquivo_checksum,
       COUNT(*) AS quantidade, MAX(timestamp_ingestao) AS ultima_ingestao
FROM novarota.bronze.transacoes
GROUP BY arquivo_origem, id_ingestao, arquivo_checksum
ORDER BY arquivo_origem, ultima_ingestao;
```

## 6. Estratégia de particionamento
Durante o desenvolvimento, optei por não utilizar o partitionBy nas tabelas Delta. A massa sintética é pequena, e uma divisão por data, mês ou entidade criaria partições e arquivos pequenos. Isso acrescentaria complexidade sem benefício relevante de leitura. Para um volume maior, avaliaria os filtros mais usados emediria o custo das consultas antes de adotar organização física adicional, como o liquid clustering. Essa opção é consistente com a documentação da Databricks de evitar particionamento manual em tabelas pequenas.

## 7. Estratégia de schema evolution
Na camada Bronze, foi utilizado um schema explícito de campos de negócio como string para preservar os dados de origem. na escrita de novas versões, `.option("mergeSchema", "true")` permite adicionar colunas ao schema Delta da bronze, desde que elas estejam presentes no dataframe de entrada. Portanto, mudanças no cabeçalho do CSV não são automaticamente incorporadas, é necessário revisão e atualização de colunas e do schema de leitura.
A camada Prata não habilita evolução automática indiscriminada. As regras de tipagem e qualidade devem ser adaptadas antes de aceitar o novo esquema. Na camada Ouro, o processamento por snapshot usa `overwriteSchema`, compatível com reconstruções intencionais das tabelas analíticas, mediante validação prévia.

## 8. Evidências e versionamento

As evidências de execução foram mantidas em `docs/evidencias/`, com os HTMLs exportados dos cinco notebooks.

Utilizo a branch `dev` para desenvolvimento, testes e documentação, e a branch `main` para a versão revisada destinada à entrega.

