# 🎬 CineData Analytics — Pipeline de Dados End-to-End no Databricks

Pipeline ETL completo sobre um catálogo de filmes (base combinada **TMDB/IMDb**), construído no **Databricks Free Edition** seguindo a **Arquitetura Medalhão** (Bronze → Silver → Gold). O projeto entrega um **Star Schema** para o time de BI, uma **tabela de contexto para RAG** para o time de IA e um conjunto de **consultas analíticas** de negócio.

> Projeto desenvolvido no programa **Rocket Lab 2026 — Engenharia de Dados (Visagio)**.

---

## 📌 Contexto

A **CineData Analytics** é uma empresa fictícia de inteligência de mercado do setor audiovisual. Os dados brutos chegam **intencionalmente sujos e fragmentados** em 5 arquivos CSV, e o objetivo é transformá-los em dados confiáveis para:

- **BI:** modelagem dimensional (Star Schema) com métricas financeiras e de engajamento;
- **IA:** uma tabela de documentos em texto corrido para alimentar um **Vector Search** (RAG) de um assistente baseado em LLM;
- **Financeiro:** valores de orçamento e receita também em **Reais (BRL)**, usando a cotação PTAX do Banco Central.

---

## 🛠️ Tecnologias

| Tecnologia | Uso no projeto |
|---|---|
| **Databricks Free Edition** | Plataforma de desenvolvimento e execução |
| **Serverless Compute** | Execução dos notebooks e do Job (sem gerenciamento de clusters) |
| **Unity Catalog** | Governança em 3 níveis: `cinedata_analytics.{bronze, silver, gold}` |
| **Delta Lake** | Formato de todas as tabelas (append na Bronze, overwrite na Silver/Gold) |
| **PySpark / Spark SQL** | Transformações, limpeza, modelagem e analytics |
| **Databricks Workflows** | Orquestração do pipeline com dependências e agendamento |
| **API PTAX (Banco Central)** | Cotação do dólar para conversão USD → BRL |

---

## 📁 Estrutura do repositório

```
cinedata-analytics-databricks/
├── notebooks/
│   ├── Landing_to_Bronze.ipynb     # Ingestão dos CSVs e da cotação (Bronze)
│   ├── Bronze_to_Silver.ipynb      # Limpeza, tipagem e padronização (Silver)
│   └── Silver_to_Gold.ipynb        # Star Schema + contexto RAG + Analytics (Gold)
├── jobs/
│   └── job.yaml                    # Exportação do Job cinedata_etl_pipeline
├── docs/
│   ├── execucao_job.png            # Print da execução bem-sucedida do Job
│   ├── catalogo_bronze.png         # Tabelas da camada Bronze no Unity Catalog
│   ├── catalogo_silver.png         # Tabelas da camada Silver no Unity Catalog
│   └── catalogo_gold.png           # Tabelas da camada Gold no Unity Catalog
│   ├── dashboard_1_visao_geral.png # Dashboard: KPIs e top 10 receita
│   ├── dashboard_2_rankings.png    # Dashboard: gêneros, produtoras, popularidade, atores
│   ├── dashboard_3_tendencia.png   # Dashboard: receita por ano
│   ├── dashboard.pdf               # Painel executivo completo (PDF)
├── .gitignore
└── README.md
```

---

## 🏗️ Arquitetura Medalhão

```mermaid
flowchart LR
    A[5 CSVs + cotacao_dolar.json<br/>Volume raw_inputs] --> B[(Bronze<br/>dados brutos<br/>+ ingestion_datetime)]
    B --> C[(Silver<br/>limpo, tipado,<br/>em português)]
    C --> D[(Gold<br/>Star Schema)]
    C --> E[(Gold<br/>gold_genai_movies_context)]
    D --> F[Analytics / BI]
    E --> G[Vector Search / RAG]
```

### Estrutura no Unity Catalog

| Bronze | Silver | Gold |
|---|---|---|
| ![Camada Bronze](docs/catalogo_bronze.png) | ![Camada Silver](docs/catalogo_silver.png) | ![Camada Gold](docs/catalogo_gold.png) |

### 🥉 Bronze — `Landing_to_Bronze`
- Lê os 5 CSVs do Volume `/Volumes/cinedata_analytics/bronze/raw_inputs/` **sem alteração de conteúdo** (todas as colunas como `STRING`, para não perder valores sujos antes do tratamento).
- Adiciona a coluna de auditoria `ingestion_datetime` e grava em **Delta, modo Append** (histórico de cargas).
- Leitura robusta para o padrão CSV RFC 4180 (`escape='"'`, `multiLine`). Com as opções padrão do Spark, cerca de 5 mil filmes seriam perdidos.
- Ingestão da cotação do dólar (PTAX) em `tb_cotacao_dolar`, com widgets documentando o período consultado (`MM-DD-AAAA`).

| Arquivo | Tabela Bronze |
|---|---|
| `movies_info*.csv` | `bronze.tb_movies_info` |
| `movies_financials_IMDB_TMDB.csv` | `bronze.tb_movies_financials` |
| `movies_metrics_IMDB_TMDB.csv` | `bronze.tb_movies_metrics` |
| `credits_and_tags_IMDB_TMDB.csv` | `bronze.tb_credits_and_tags` |
| `movies_reviews.csv` | `bronze.tb_movies_reviews` |
| `cotacao_dolar.json` (API PTAX) | `bronze.tb_cotacao_dolar` |

### 🥈 Silver — `Bronze_to_Silver`
Colunas em português, tipagem correta e regras de qualidade. Toda conversão usa `try_cast`/`try_to_timestamp`, porque o Serverless roda com **ANSI mode** e um `cast` comum quebraria o pipeline com dados sujos.

| Tabela | Principais tratamentos |
|---|---|
| `tb_info_filmes` | Deduplicação pela ingestão mais recente; status normalizado e traduzido; datas em 3 formatos (`AAAA-MM-DD`, `DD/MM/AAAA`, `MM-DD-AAAA`); títulos em caixa alta/baixa corrigidos; `ano_lancamento` derivado |
| `tb_financeiro_filmes` | Tokens de ausência → NULL; remoção de moeda e milhar; expansão de `K/M/B` (`34.0M` → 34.000.000); zero/negativo → NULL; conversão BRL; lucro e margem null-safe |
| `tb_metricas_engajamento` | Vírgula decimal corrigida; column shift tratado com cast seguro; notas fora de 0–10 e contagens negativas → NULL |
| `tb_avaliacoes_usuarios` | Duplicatas exatas removidas; nota fora de 0–10 → NULL; comentário vazio → `"Sem comentário"` |
| `tb_generos` | Separadores `,` `;` `\|` padronizados; split + explode; validação contra os 19 gêneros oficiais do TMDB |
| `tb_pessoas_empresas` | 4 colunas unificadas (`Ator`, `Diretor`, `Roteirista`, `Produtora`); resíduos de column shift removidos; capitalização padronizada |
| `tb_cotacao_dolar` | Série temporal contínua com **forward fill** para fins de semana e feriados |

### 🥇 Gold — `Silver_to_Gold`

#### Star Schema

```mermaid
erDiagram
    fact_movies_performance }o--|| dim_movies : sk_movie_id
    dim_reviews }o--|| dim_movies : sk_movie_id
    bridge_movie_genre }o--|| dim_movies : sk_movie_id
    bridge_movie_genre }o--|| dim_genres : sk_genre_id
    bridge_movie_person }o--|| dim_movies : sk_movie_id
    bridge_movie_person }o--|| dim_people : sk_person_id
    bridge_movie_company }o--|| dim_movies : sk_movie_id
    bridge_movie_company }o--|| dim_companies : sk_company_id
```

| Tabela | Tipo | Descrição |
|---|---|---|
| `fact_movies_performance` | Fato | **1 linha por filme lançado**. Métricas financeiras (`DECIMAL(18,2)`, USD e BRL) e de engajamento (popularidade, notas e votos TMDB/IMDb) |
| `dim_movies` | Dimensão | Metadados descritivos de todos os filmes do catálogo |
| `dim_genres` | Dimensão | Catálogo único de gêneros |
| `dim_people` | Dimensão | Pessoas físicas (`Ator`, `Diretor`, `Roteirista`) |
| `dim_companies` | Dimensão | Produtoras/estúdios |
| `dim_reviews` | Dimensão | Resumo das avaliações de usuários por filme (quantidade e nota média) |
| `bridge_movie_genre` / `_person` / `_company` | Bridge | Relações N:N entre filmes e dimensões periféricas |

**Decisões de modelagem:**
- **Surrogate keys via `sha2`** convertido para `BIGINT`: são **determinísticas**, então o mesmo filme recebe sempre a mesma chave em qualquer execução. Com `row_number()` ou `monotonically_increasing_id()`, as chaves mudariam entre execuções.
- **Bridge tables** evitam a explosão de linhas na fato: somar a receita após um join direto com gêneros triplicaria o total (US$ 484 bi em vez de US$ 162 bi).
- O notebook **valida** a unicidade das chaves, o grão da fato e a integridade referencial (0 órfãos nas 8 FKs) antes de publicar.

#### Tabela de contexto para RAG — `gold_genai_movies_context`

| Coluna | Descrição |
|---|---|
| `movie_id` | Chave natural do filme |
| `title` | Título |
| `llm_context_document` | Documento em texto corrido para vetorização |

Exemplo gerado:

> O filme Dangal, lançado no ano de 2016, faturou US$ 311.000.000 e teve um custo de US$ 10.400.000. Estrelado por Aparshakti Khurana, Girish Kulkarni, Sanya Malhotra, Aamir Khan e Vivan Bhatena e dirigido por Nitesh Tiwari, o filme possui a seguinte sinopse: ...

**Tratamento de nulos:** `concat()` retorna `NULL` se qualquer campo for nulo, o que faria o filme sumir silenciosamente. Cada campo recebe `coalesce()` com um fallback (`"valor não divulgado"`, `"diretor não informado"`, `"Sinopse não disponível."`...) **antes** da concatenação. Resultado: **97.611 documentos para 97.611 filmes, 0 nulos**. A tabela tem **Change Data Feed** habilitado, requisito para um índice Delta Sync do Vector Search.

---

## 🔍 Achados de Qualidade de Dados

Além das regras previstas no escopo, a análise exploratória dos dados brutos e as validações ao longo do pipeline revelaram problemas **não documentados** que distorceriam os resultados se não fossem tratados.

| # | Problema | Como foi detectado | Como foi tratado | Impacto |
|---|---|---|---|---|
| 1 | **Perda silenciosa de registros na leitura do CSV.** Sinopses usam aspas no padrão RFC 4180 (`""`), mas o Spark usa `\` como escape por padrão | Comparação entre o número de linhas lidas pelo Spark e o número real de registros do arquivo | Leitura com `quote='"'`, `escape='"'` e `multiLine=True` | **~5 mil filmes** (101.571 vs 106.596) seriam perdidos já na Bronze, sem nenhum erro |
| 2 | **Terceiro separador de gêneros.** Além de `,` e `;` (citados no escopo), a base usa `\|` (`Comedy\|Drama`) | Contagem de linhas por tipo de separador na coluna `genres` | Os três separadores são padronizados antes do `split`, e os valores são validados contra o domínio oficial de 19 gêneros do TMDB | Sem o tratamento, milhares de combinações como `Comedy\|Drama` virariam "gêneros" falsos |
| 3 | **Duplicatas dentro da mesma carga.** O mesmo `id` aparece várias vezes no mesmo arquivo, com versões diferentes (ex.: receita `0` em uma linha e `Unknown` em outra) | Contagem de ids repetidos por arquivo e comparação das versões | Desempate pela `ingestion_datetime` (regra do escopo) e, em seguida, pela **versão mais completa** (mais campos não nulos) | Sem o desempate, a escolha entre as versões seria aleatória a cada execução |
| 4 | **Países, idiomas e keywords vazados para colunas de pessoas** (ex.: `English` e `United States of America` como diretores) por column shift | Ranking de diretores mais frequentes após a limpeza inicial | Descarte por **regra de frequência**: um valor só é removido se aparece mais vezes como país/idioma/keyword do que como pessoa. As colunas de país e idioma também estão contaminadas, então uma lista negra direta apagaria nomes válidos | `English` (~300 filmes como "diretor") removido; `Kevin Dunn` (77 filmes) e produtoras como `BBC` e `ZDF` preservados |
| 5 | **Anos vazados para a popularidade** (valores `2020.0`, `2019.0`, `1969.0`). Em *Battipaglia 1969*, o valor é o ano do título | Top 5 de popularidade com valores inteiros exatos, enquanto a popularidade real do TMDB tem casas decimais | Inteiros exatos entre 1870 e 2030 são tratados como resíduo de column shift → `NULL` | 3 filmes obscuros deixaram de aparecer no top 5 de popularidade |
| 6 | **Títulos em caixa baixa ou alta** (ex.: `spider-man: no way home`, `AVENGERS: INFINITY WAR`) | Inspeção dos rankings de receita e popularidade | Quando o `original_title` tem o mesmo texto com a capitalização correta, a grafia dele é usada | ~3,5 mil títulos corrigidos |
| 7 | **O mesmo filme cadastrado com vários ids** (ex.: *Die Hart 2: Die Harter* com 25 ids na mesma data; *Emesis Blue* com 19) | Resultado improvável na pergunta 5: um ator com 64 filmes em 2 anos e 20 atores empatados com 59 | Nas análises por pessoa/produtora, a contagem é feita por **obra** (título normalizado + data), e não por id | 223 filmes com 411 ids excedentes. O ranking do ator líder caiu de **64 para 13** filmes, o valor real |
| 8 | **Risco de dupla contagem nas relações N:N** entre filmes e gêneros/pessoas/produtoras | Comparação da receita total somada pela fato e após um join direto com a bridge de gêneros | Métricas somadas **apenas na fato** (1 linha por filme); bridges usadas só para filtrar e agrupar, com `COUNT(DISTINCT)` | Um join ingênuo com a bridge **triplicaria a receita**: US$ 162 bi → US$ 484 bi |

### Validações automáticas no pipeline

Além dos tratamentos, o pipeline **interrompe a execução** se alguma garantia for violada. Isso evita publicar dados incorretos na Gold:

- Unicidade das surrogate keys e da chave natural em todas as dimensões.
- Grão da fato: número de linhas igual ao número de filmes lançados.
- Integridade referencial: 0 registros órfãos nas 8 chaves estrangeiras.
- Tabela RAG: número de documentos igual ao número de filmes, com 0 documentos nulos.

---

## 🧠 Decisões Técnicas

Cada decisão abaixo tem uma alternativa mais simples que foi **descartada de propósito**.

### Surrogate keys com `sha2`, e não `row_number()` ou `monotonically_increasing_id()`
O hash da chave natural é **determinístico**: o mesmo filme recebe sempre a mesma `sk_movie_id`, em qualquer execução do Job. Com `row_number()`, a entrada de um filme novo deslocaria as chaves de todos os outros; com `monotonically_increasing_id()`, as chaves mudam conforme o particionamento dos dados. Nos dois casos, dashboards e consultas que guardam referências às chaves ficariam inconsistentes. O `sha2` gera texto, então os primeiros 60 bits do hash são convertidos para `BIGINT`, como exige o modelo. A unicidade é validada em todas as dimensões para descartar colisões.

### Bronze inteira como `STRING`
Com `inferSchema=True`, o Spark converteria valores como `"154,34"` ou `"$ 97000000"` em `NULL` **já na ingestão**, e a informação original se perderia antes de qualquer tratamento. Manter tudo como texto cumpre a regra "sem alteração de conteúdo" e deixa a tipagem para a Silver, onde cada sujeira é tratada de forma explícita e documentada.

### `try_cast` em vez de `cast`
O Databricks Serverless roda com **ANSI mode ligado**. Nesse modo, um `cast("abc" AS INT)` **lança erro** e derruba o pipeline inteiro, em vez de devolver `NULL`. Como o column shift espalha textos por colunas numéricas, `try_cast` e `try_to_timestamp` são a forma de converter o que é válido e transformar o resto em `NULL` sem interromper a execução.

### Lucro `NULL` quando falta orçamento ou receita (e não 0)
Tratar um valor ausente como zero **inventaria resultados**: um filme com receita conhecida e orçamento ausente apareceria com 100% de lucro, e o caso contrário com prejuízo total. A subtração com `NULL` devolve `NULL` sem erro, o que é o comportamento correto: o lucro é **desconhecido**, não zero. A margem percentual só é calculada quando a receita é maior que zero, evitando divisão por zero.

### Notas fora de 0–10 viram `NULL`, sem dividir por 10
Um valor como `79.97` sugere erro de escala ×10, mas não há como garantir que o fator seja sempre 10. Corrigir por suposição poderia gerar notas plausíveis e erradas. Seguindo o escopo, valores fora do intervalo são desconsiderados.

### Cotação mais recente para a conversão em BRL
Os filmes não têm data de transação, e a série PTAX cobre só os últimos dias. Um join pela data de lançamento não encontraria cotação para praticamente nenhum filme. A conversão usa a **cotação mais recente** (valor atual em reais), e a data e a taxa aplicadas ficam gravadas na tabela para rastreabilidade.

### Bronze em Append; Silver e Gold em Overwrite
A Bronze guarda o **histórico** de todas as cargas, como pede o escopo. Silver e Gold são **reconstruídas a partir da Bronze completa** a cada execução, com deduplicação pela carga mais recente. Isso torna o pipeline **idempotente**: rodar o Job duas vezes produz o mesmo resultado, sem duplicar registros.

### `dim_movies` com todos os filmes; fato só com os lançados
A dimensão descreve o **catálogo inteiro** (inclusive filmes planejados ou em produção), o que permite, por exemplo, que o assistente de IA responda sobre lançamentos futuros. O recorte "somente lançados" define o **grão da fato**, onde estão as métricas de desempenho.

### `LEFT JOIN` na construção da fato e da tabela RAG
Um `INNER JOIN` com as métricas financeiras eliminaria os filmes sem orçamento ou receita, que são a maioria. Com `LEFT JOIN`, todo filme lançado continua na fato com métricas `NULL`, e o grão é validado ao final (linhas da fato = filmes lançados).

### `coalesce` com fallback antes da concatenação (RAG)
`concat()` retorna `NULL` se qualquer campo for nulo, e o filme **sumiria silenciosamente** da base vetorial. Cada campo recebe um texto de fallback (`"valor não divulgado"`, `"diretor não informado"`) **antes** da concatenação. O fallback indica ao LLM que o dado não existe, em vez de induzi-lo a responder "faturou US$ 0".

### Janela de tempo relativa à base, e não à data atual
Nas perguntas de "últimos 2 e 5 anos", a referência é a **data de lançamento realizada mais recente da base**, e não `current_date()`. A base é um retrato histórico: usar a data de hoje poderia deixar a janela vazia se os dados fossem antigos. O cálculo usa `add_months`, que respeita anos bissextos e meses de tamanhos diferentes.

### `RANK()` com filtro de posição, e não `LIMIT`
`LIMIT 10` corta empates arbitrariamente. Com `RANK()` e `WHERE posicao <= 10`, filmes empatados na última posição aparecem todos, o que é o resultado justo.

## 📊 Analytics

Consultas em Spark SQL no final do `Silver_to_Gold`:

1. Receita total (R$) de todos os filmes
2. Top 5 filmes por popularidade
3. Quantidade de filmes por gênero
4. Top 10 filmes por receita com `RANK()`
5. Ator com mais participações nos últimos 2 anos
6. Produtora com maior lucro nos últimos 5 anos

Nas perguntas 5 e 6, a janela de tempo é relativa à **data de lançamento realizada mais recente da base** (e não à data atual), ignorando filmes não lançados e datas futuras.

---

### Painel executivo (Databricks AI/BI Dashboard)

As mesmas perguntas de negócio, publicadas como dashboard sobre a camada Gold:

📄 [Ver o painel completo em PDF](docs/dashboard.pdf)

**Visão geral:** KPIs de receita e top 10 filmes por receita
![Dashboard - Visão geral](docs/dashboard_1_visao_geral.png)

**Rankings:** gêneros, produtoras, popularidade e atores
![Dashboard - Rankings](docs/dashboard_2_rankings.png)

**Tendência:** receita por ano de lançamento, com o impacto da pandemia em 2020
![Dashboard - Tendência](docs/dashboard_3_tendencia.png)

## ⚙️ Orquestração

Job **`cinedata_etl_pipeline`** (`jobs/job.yaml`), executado em **Serverless**:

```
to_Bronze  ──►  to_Silver  ──►  to_Gold
```

- Dependências explícitas: cada task só inicia após o **sucesso** da anterior.
- Agendamento diário às **03:00 (America/Sao_Paulo)**, simulando uma rotina de produção.

![Execução do Job](docs/execucao_job.png)

---

## ▶️ Como executar

1. **Criar a estrutura no Unity Catalog** (SQL Editor ou notebook):
   ```sql
   CREATE CATALOG IF NOT EXISTS cinedata_analytics;
   CREATE SCHEMA IF NOT EXISTS cinedata_analytics.bronze;
   CREATE VOLUME IF NOT EXISTS cinedata_analytics.bronze.raw_inputs;
   ```
2. **Enviar os arquivos de entrada** para o Volume (`Catalog → cinedata_analytics → bronze → raw_inputs → Upload`): os 5 CSVs e o `cotacao_dolar.json`.
3. **Importar os notebooks** da pasta `notebooks/` no Workspace (`Import → .ipynb`).
4. **Executar** na ordem `Landing_to_Bronze` → `Bronze_to_Silver` → `Silver_to_Gold`, ou criar o Job a partir de `jobs/job.yaml`, ajustando os caminhos dos notebooks para o seu usuário.

---

## ⚠️ Limitações conhecidas

- **Cotação via arquivo:** o Free Edition restringe o acesso de saída à internet a partir do Serverless. A resposta da API PTAX foi obtida pelo endpoint oficial e salva como `cotacao_dolar.json` no Volume. A Silver estende a série até a data de execução com forward fill.
- **Conversão BRL:** os filmes não têm data de transação, então é aplicada a **cotação mais recente** disponível (valor atual em reais).
- **Filmes duplicados com ids diferentes:** a origem tem o mesmo filme cadastrado com vários ids (ex.: 25 ids para *Die Hart 2: Die Harter*). A deduplicação da Silver segue a regra do escopo (por id); nas perguntas 5 e 6 a contagem é feita por obra (título + data).
- **Atores principais no RAG:** a ordem de créditos não existe no modelo; são usados os 5 atores com mais filmes no catálogo como aproximação de relevância.
- **Column shift:** a limpeza remove os resíduos frequentes (textos, números, países e idiomas deslocados), mas ocorrências raras podem permanecer.
