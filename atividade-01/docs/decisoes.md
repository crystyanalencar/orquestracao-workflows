# Decisões de projeto

Como a DAG `ecommerce_metricas` foi construída e por quê. Foi feita em camadas: cada uma entra,
é testada isoladamente e só então a próxima começa. Para cada camada: o que entrou, por quê,
como foi provado e o que se observou.

## Objetivo

Todo dia às 06:00 (Brasília), buscar os produtos da [FakeStore API](https://fakestoreapi.com),
calcular por categoria preço médio, mínimo, máximo e quantidade, e gravar no Postgres sem duplicar.

```
buscar_produtos ─► separar_por_categoria ─► calcular_metricas[cat 1] ─┐
   (linear)            (fan-out)            calcular_metricas[cat 2] ─┼─► consolidar ─► gravar_postgres
                                            calcular_metricas[cat N] ─┘    (fan-in)
```

## A fonte de dados

- **Uma chamada só, `GET /products`.** As categorias saem dos próprios produtos. Chamar também
  `/products/categories` seria uma segunda requisição que pode falhar ou divergir da primeira.
- **Cuidados com o dado:** `price` vem como int ou float (normalizado com `float`);
  `men's clothing` tem apóstrofo, então o SQL usa só parâmetros, nunca concatenação; `jewelery`
  é grafado assim na fonte e não é corrigido; um 404 não vira exceção em `requests` sem
  `raise_for_status()`.

## Qual data gravar

`data_coleta` = `data_interval_end` no fuso de Brasília, não o `ds`.

O `ds` é o *início* do intervalo agendado: a execução que roda em 16/11 às 06:00 tem `ds` = 15/11.
A API devolve o retrato do momento da coleta, então a data certa é o fim do intervalo. Isso foi
confirmado na prática (camada 5.1): em execução manual a data informada vira o fim do intervalo e
o `ds` cai igual a ela, enquanto na agendada o `ds` é a véspera. O `ds` muda conforme a origem da
execução; o `data_interval_end` não. Re-executar a mesma data repete o valor e sobrescreve as
mesmas linhas, o que sustenta a idempotência.

## Ambiente

- Airflow 2.9.3 (LocalExecutor) e **dois Postgres**: um de metadados do Airflow e outro só para os
  dados (`postgres-ecommerce`). Dado de negócio não se mistura com o controle interno da ferramenta.
- O serviço `airflow-init` cria a Connection `postgres_ecommerce` e o pool `ecommerce_pool`
  (2 slots). Quem clona o repo sobe tudo sem configurar nada pela UI, e credencial não fica no
  código da DAG.

- `AIRFLOW__CORE__FERNET_KEY` fica vazio de propósito: a senha da Connection e as Variables
  ficam em texto puro no banco de metadados. Para um ambiente local com credencial descartável,
  isso é aceitável, e quem clona sobe tudo sem gerar chave. Num ambiente compartilhado, a chave
  iria num `.env` fora do git, com um `.env.example` no repo.
- Efeito colateral visto nos logs: o Airflow mascara a senha das Connections em todo log, e a
  senha aqui é `ecommerce`. Por isso a palavra aparece como `***` até no nome da DAG
  (`dag_id=***_metricas`), e `airflow` (senha do banco de metadados) vira `***` na linha de comando
  da task. O mascaramento funciona; senha igual a palavra comum só deixa o log mais difícil de ler.

### Problemas encontrados ao subir

Os três vieram da mesma escolha: rodar o Airflow com o UID do usuário (`user: ${AIRFLOW_UID}`), para
os logs em `./logs` saírem com o dono certo sem `chmod`.

1. **`Unable to configure handler 'processor'`.** Se `./logs` não existe, o Docker cria a pasta do
   bind mount como root, e o Airflow, rodando sem ser root, não consegue escrever nela. Correção:
   criar a pasta antes da primeira subida (`mkdir -p logs`).
2. **`getpwuid(): uid not found: 1000`.** O init usava `entrypoint: /bin/bash`, o que pula o
   entrypoint da imagem, que é quem registra o UID arbitrário no `/etc/passwd`. Correção: manter o
   entrypoint padrão e passar o script em `command: [bash, -c, ...]`.
3. **Sucesso falso no init.** Sem `set -e`, o script do init terminava com código 0 mesmo com o
   `airflow db migrate` quebrado, porque o último comando era um `echo`. O erro só aparecia depois,
   no scheduler (`You need to initialize the database`). Correção: `set -e` no início do script.
   Lição geral: "ok" impresso no fim de um script não prova nada se um passo do meio pode falhar
   em silêncio.

## Camadas

### 5.1 Esqueleto da DAG

- **O quê:** `@dag` com `schedule="0 6 * * *"`, `start_date` em `America/Sao_Paulo` (`zoneinfo`),
  `catchup=False`, e uma task provisória que imprime `ds`, `data_interval_start` e
  `data_interval_end`.
- **Por quê:** sem o fuso, o cron roda em UTC (06:00 UTC = 03:00 em Brasília). Sem
  `catchup=False`, o Airflow tentaria executar todos os dias desde o `start_date`.
  O fuso usa `zoneinfo`, da biblioteca padrão, em vez do `pendulum` que o enunciado cita. A
  troca é consciente: o `zoneinfo` foi comentado em aula como alternativa nativa.
  Testado no 2.9.3: o Airflow converte para pendulum por dentro, serializa a DAG e calcula a
  próxima execução às 06:00 de Brasília. O `pytz` foi descartado:
  `datetime(..., tzinfo=pytz.timezone(...))` pega o fuso histórico de 1914 (-03:06) em silêncio.
- **Teste:** `airflow tasks test ecommerce_metricas mostrar_datas <data>`.
- **Observado:** o comportamento de `ds` x `data_interval_end` descrito em "Qual data gravar".

### 5.2 Task `buscar_produtos`

- **O quê:** substitui a task provisória. Faz `GET /products` com `timeout=30`, chama
  `raise_for_status()` e devolve uma lista com só `{id, category, price}`, com `price` em `float`.
- **Por quê:** sem timeout, uma API travada prende o worker indefinidamente. Sem
  `raise_for_status()`, um 404/500 passaria como sucesso. O retorno vira XCom, guardado no banco
  de metadados, então leva só os 3 campos que o pipeline usa (sem título, descrição e imagem).
- **Teste:**
  ```bash
  docker compose exec airflow-scheduler airflow tasks test ecommerce_metricas buscar_produtos 2026-11-17
  ```
- **Observado:** `20 produtos recebidos`, `Marking task as SUCCESS`. Retorno com 4 categorias:
  `men's clothing` (4), `jewelery` (4), `electronics` (6), `women's clothing` (6); preços inteiros
  já convertidos (ex.: `695.0`).

### 5.3 Resiliência em `buscar_produtos`

- **O quê:** `retries=3` com `retry_delay=10s`, `retry_exponential_backoff=True` e
  `max_retry_delay=5min`; callbacks de sucesso, retry e falha. Os erros se dividem em dois tipos:
  - **temporário** (rede, DNS, timeout, HTTP 5xx e 429): loga e relança, e o retry cuida;
  - **permanente** (outro 4xx, JSON inválido, campo faltando, preço inválido, lista vazia):
    `AirflowFailException`, que falha na hora sem gastar os retries.

  Rede/HTTP e formato da resposta ficam em dois blocos `try`. A URL vem da Variable
  `fakestore_url`, com a URL real como padrão. Preço passa por `_preco()`, que rejeita `NaN`,
  infinito, negativo e booleano.
- **Por quê:**
  - Um `except` sem `raise` engoliria o erro, e a task terminaria verde sem dado.
  - O backoff dá tempo para a API voltar sem martelá-la. Já um 404 ou um campo renomeado não
    melhoram esperando: retentar só atrasa o alerta em ~2 min e faz o callback de retry tratar
    como passageiro um erro que não é.
  - Os dois blocos existem porque `resposta.json()` levanta `JSONDecodeError`, que é ao mesmo tempo
    `RequestException` e `ValueError`. Num bloco só, um corpo que não é JSON seria logado como
    falha de rede.
  - `float()` aceita `"NaN"`, `"inf"`, `True` e negativos, e qualquer um estragaria as métricas
    sem erro nenhum. Lista vazia gravaria um "dia sem produtos" como se fosse verdade.
  - A Variable permite forçar cada tipo de falha no teste sem editar o código.
- **Teste:** pela UI, não com `airflow tasks test`, que executa uma tentativa só e não exercita o
  retry.
  ```bash
  # permanente: 404
  docker compose exec airflow-scheduler airflow variables set fakestore_url https://fakestoreapi.com/produtosx
  # temporário: host que não existe (.invalid é reservado e nunca resolve)
  docker compose exec airflow-scheduler airflow variables set fakestore_url https://fakestoreapi.invalid/products
  # depois dos testes, senão a execução agendada falha:
  docker compose exec airflow-scheduler airflow variables delete fakestore_url
  ```
  A validação de preço foi testada chamando `_preco()` direto no container: `10`, `22.3` e `"5"`
  passam; `"NaN"`, `"inf"`, `-1` e `True` são rejeitados.
- **Observado:**
  - Sucesso: `[callback sucesso] buscar_produtos terminou bem na tentativa 1`.
  - Permanente: só `attempt=1.log`, `Erro permanente HTTP 404`,
    `Immediate failure requested. Marking task as FAILED` e
    `[callback falha] ... falhou de vez na tentativa 1`, sem nenhum retry.
  - Temporário: `attempt=1.log` a `attempt=4.log`, `NameResolutionError` em cada uma,
    `[callback retry]` nas tentativas 1, 2 e 3 e `[callback falha] ... na tentativa 4`. Espera
    entre o fim de uma tentativa e o início da seguinte: ~17 s, ~23 s e ~59 s. Uma rodada
    anterior deu ~18, ~25 e ~80 s. As duas caem nas faixas de 10-20, 20-40 e 40-80 s, somadas ao
    ciclo do scheduler. O Airflow escolhe o ponto dentro da faixa por um hash de dag, task, data e
    tentativa: tasks diferentes se espalham, mas a mesma execução e tentativa sempre esperam o
    mesmo tempo.
  - **Armadilha do `try_number` antes do Airflow 2.10.** O primeiro teste de sucesso logou
    "terminou bem na tentativa 2" numa execução que acertou de primeira: havia só
    `attempt=1.log`, nenhum retry no log, e `try_number = 1` na tabela `task_instance` do banco de
    metadados. Nessas versões, `ti.try_number` vale a tentativa atual só enquanto a task roda;
    terminada (sucesso, falha ou retry), passa a valer a próxima. Callbacks rodam depois do fim,
    então `_tentativa()` desconta 1, mas só se a versão for anterior à 2.10, que corrigiu isso.
  - Logs completos dos três testes (cópia de `logs/`, que fica fora do git) em
    [`evidencias/5.3/`](evidencias/5.3/): `1-sucesso.log`, `2-permanente-404.log` e
    `3-temporario-dns-tentativa-1.log` a `-4.log`.
  - Callbacks normalmente logam no log da task. Se o processo da task morre (falta de memória,
    heartbeat perdido), o Airflow roda o de falha no DagFileProcessor, e a mensagem vai para o
    log dele.

### 5.4 Fan-out por categoria, no pool

- **O quê:** `separar_por_categoria` agrupa os preços e devolve uma lista de pares
  `{categoria, precos}`, em ordem alfabética. `calcular_metricas.expand(par=...)` cria uma
  instância por par, com `pool="ecommerce_pool"` (2 slots), e cada uma devolve `qtd`, `media`
  (2 casas), `minimo` e `maximo`. O `map_index_template` faz a UI mostrar o nome da categoria em
  vez de `0..3`. A Variable `atraso_metricas_s` (padrão 0) faz cada instância dormir N segundos
  antes de calcular.
- **Por quê:**
  - Os pares já levam os preços, então nenhuma instância chama a API de novo: uma requisição só
    por execução, e as tasks mapeadas não dependem da rede.
  - O número de categorias vem do dado, não do código. Se a API ganhar uma quinta categoria, o
    `.expand` cria a quinta instância sozinho.
  - O pool limita quantas instâncias rodam ao mesmo tempo, qualquer que seja o número de
    categorias. É o recurso para proteger um banco ou uma API de excesso de conexões.
  - O cálculo leva milissegundos, então a fila do pool mal aparece. O atraso deixa as duas levas
    visíveis na UI. Segue o padrão do `fakestore_url`: o código entregue não espera nada, e o
    teste se repete com um comando, sem editar o código.
- **Teste:** pela UI.
  1. Disparar sem Variable e abrir `calcular_metricas` → **Mapped Tasks**.
  2. Pool:
     ```bash
     docker compose exec airflow-scheduler airflow variables set atraso_metricas_s 15
     # disparar e abrir Admin → Pools ou Browse → Task Instances; depois:
     docker compose exec airflow-scheduler airflow variables delete atraso_metricas_s
     ```
- **Observado:**
  - 4 instâncias, cada uma com o nome da sua categoria. Conferido à mão em `men's clothing`
    (109.95, 22.3, 55.99, 15.99): `qtd 4, media 51.06, minimo 15.99, maximo 109.95`.
  - Com atraso de 15 s: `electronics` e `jewelery` começam às 10:53:28 e terminam às 10:53:44;
    `men's clothing` e `women's clothing` ficam em `scheduled` até lá e começam às 10:53:44.9.
    Duas levas de 2, nunca mais de 2 ao mesmo tempo.
  - O limite já aparecia sem atraso: na primeira execução, a segunda dupla começou às 10:48:32,
    só depois de a primeira terminar (10:48:31). Com atraso, a diferença fica grande o bastante
    para ver na tela.
  - Evidências em [`evidencias/5.4/`](evidencias/5.4/): logs de `separar_por_categoria` e das 4
    instâncias, print das Mapped Tasks, print de Task Instances com 2 `running` e 2 `scheduled`,
    e os horários tirados da tabela `task_instance` (`5-task_instance-horarios.txt`, com
    `rendered_map_index` e `pool` gravados).

### 5.5 Fan-in: `consolidar`

- **O quê:** `consolidar(metricas)` recebe a saída das instâncias mapeadas de
  `calcular_metricas` numa lista. Acrescenta `data_coleta` (o `data_interval_end` convertido para
  Brasília, em texto ISO), ordena por categoria e devolve as linhas prontas para a gravação.
- **Por quê:**
  - É o ponto de encontro do fan-out: o Airflow só roda a task depois que todas as instâncias
    terminam. A gravação recebe um lote único e consistente, em vez de uma escrita por categoria.
  - `data_interval_end` chega pelo nome do parâmetro (o TaskFlow injeta variáveis do contexto).
    A conversão para Brasília vem antes de tirar a data, porque em UTC uma execução depois das
    21:00 cairia no dia seguinte. Fica em texto porque o XCom é JSON.
- **Topologias, de forma identificável:**
  - linear: `buscar_produtos` → `separar_por_categoria`;
  - fan-out: `separar_por_categoria` → `calcular_metricas` ×N, em paralelo, limitado pelo pool;
  - fan-in: `calcular_metricas` ×N → `consolidar`.

  Na Graph, a task mapeada aparece como um nó só, `calcular_metricas [ ]`, porque as instâncias
  só existem em tempo de execução: dependem de quantas categorias a API devolve. Com uma execução
  selecionada, o nó mostra a contagem. As instâncias aparecem em **Mapped Tasks** ou no Gantt.
- **Teste:** disparar pela UI e ler o log de `consolidar`.
- **Observado:** `4 linhas para 2026-11-22` (trigger manual: o fim do intervalo é o momento do
  disparo), com as mesmas métricas da 5.4. Evidências em
  [`evidencias/5.5/`](evidencias/5.5/): log de `consolidar` e print da Graph.

### 5.6 Gravação idempotente: `gravar_postgres`

- **O quê:** `PostgresHook(postgres_conn_id="postgres_ecommerce")` e uma transação só com três
  passos: `CREATE TABLE IF NOT EXISTS metricas_categoria` (chave primária
  `(data_coleta, categoria)`, preços em `NUMERIC(10,2)`); upsert com
  `INSERT ... ON CONFLICT (data_coleta, categoria) DO UPDATE`; e `DELETE` das categorias do dia que
  não vieram no lote. A coluna `atualizado_em` recebe `now()` a cada gravação.
- **Por quê:**
  - Chave primária + `ON CONFLICT`: re-rodar o mesmo dia sobrescreve em vez de duplicar. É o
    problema do enunciado ("quando alguém roda na mão, os números aparecem duplicados").
  - Valores sempre por parâmetro (`%(categoria)s`), nunca concatenados: `men's clothing` tem
    apóstrofo, e concatenar quebraria o SQL ou abriria espaço para injeção.
  - `DELETE` das sumidas: sem ele, se uma categoria some da API e o dia é reprocessado, a linha
    antiga fica para trás, e o dia gravado deixa de ser o retrato da última coleta.
  - Uma transação: ou o dia inteiro muda, ou nada muda. Uma falha no meio não deixa metade das
    categorias novas e metade antigas.
  - `NUMERIC` e não `float`: o banco guarda os centavos exatos.
  - `atualizado_em` prova, olhando a tabela, que a linha foi sobrescrita, não inserida de novo.
  - Credencial na Connection (criada pelo `airflow-init`), não no código.
- **Teste:**
  ```bash
  docker compose exec postgres-ecommerce psql -U ecommerce -d ecommerce -c "SELECT * FROM metricas_categoria ORDER BY categoria;"
  ```
  1. Disparar a DAG e consultar a tabela.
  2. **Clear** em `gravar_postgres` da mesma execução (mesmo XCom) e consultar de novo.
  3. Disparar outra execução no mesmo dia (mesma `data_coleta`) e consultar.
  4. Categoria sumida: inserir uma linha falsa no dia e dar Clear em `gravar_postgres`.
     ```bash
     docker compose exec postgres-ecommerce psql -U ecommerce -d ecommerce -c "INSERT INTO metricas_categoria VALUES ('2026-11-22', 'categoria-fantasma', 1, 1, 1, 1);"
     ```
- **Observado:** o dia ficou com 4 linhas depois de cada uma das gravações (11:16:39, Clear às
  11:17:13, outra execução às 11:18:47), com `atualizado_em` avançando a cada uma. No teste 4, o
  log disse `1 categorias sumidas removidas; o dia tem 4 linhas na tabela`, e a fantasma saiu.
  Evidências em [`evidencias/5.6/`](evidencias/5.6/): os logs de cada gravação e a tabela
  consultada antes e depois do teste 4.

## Próximas camadas

- 5.7 TaskGroups `ingestao`, `analise`, `carga`.
