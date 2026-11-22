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
  O fuso usa `zoneinfo`, da biblioteca padrão, em vez do `pendulum` dos exemplos do Airflow 2.
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
  - Callbacks normalmente logam no log da task. Se o processo da task morre (falta de memória,
    heartbeat perdido), o Airflow roda o de falha no DagFileProcessor, e a mensagem vai para o
    log dele.

## Próximas camadas

- 5.4 Fan-out: `separar_por_categoria` + `calcular_metricas.expand(...)` no `ecommerce_pool`.
- 5.5 Fan-in: `consolidar`.
- 5.6 Gravação idempotente com `PostgresHook` (upsert por `data_coleta` + `categoria`).
- 5.7 TaskGroups `ingestao`, `analise`, `carga`.
