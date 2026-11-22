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

- **O quê:** `@dag` com `schedule="0 6 * * *"`, `start_date` em `America/Sao_Paulo` (pendulum),
  `catchup=False`, e uma task provisória que imprime `ds`, `data_interval_start` e
  `data_interval_end`.
- **Por quê:** sem o fuso, o cron roda em UTC (06:00 UTC = 03:00 em Brasília). Sem
  `catchup=False`, o Airflow tentaria executar todos os dias desde o `start_date`.
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

- **O quê:** `try/except` que loga e relança (`raise`) erros de rede/HTTP e de formato da
  resposta; lista vazia vira erro; `retries=3` com `retry_delay=10s`,
  `retry_exponential_backoff=True` e `max_retry_delay=5min`; callbacks de sucesso, retry e falha.
  A URL vem da Variable `fakestore_url`, com a URL real como padrão.
- **Por quê:** o `except` sem `raise` engoliria o erro e a task terminaria verde sem dado. O
  backoff dá tempo para a API voltar sem martelá-la. Lista vazia passaria como "dia sem produtos"
  e gravaria métricas falsas. A Variable permite forçar uma falha no teste sem editar o código.
- **Teste:** pela UI, não com `airflow tasks test`, que executa uma tentativa só e não exercita o
  retry.
  1. Sucesso: despausar a DAG e conferir `[callback sucesso]` no log da task.
  2. Falha: apontar a Variable para uma URL que não existe, disparar e acompanhar as 4 tentativas.
     ```bash
     docker compose exec airflow-scheduler airflow variables set fakestore_url https://fakestoreapi.com/produtosx
     # depois do teste, senão a execução agendada falha:
     docker compose exec airflow-scheduler airflow variables delete fakestore_url
     ```
- **Observado:**
  - Falha: um arquivo por tentativa (`attempt=1.log` a `attempt=4.log`), `404 Client Error` em
    cada uma, `[callback retry] ... tentativa 1`, `2` e `3`, e por fim
    `[callback falha] ... esgotou as tentativas (4)` com a task em `FAILED`. Espera entre o fim de
    uma tentativa e o início da seguinte: ~18 s, ~25 s e ~80 s, dentro das faixas que o Airflow
    sorteia (10-20 s, 20-40 s, 40-80 s; o último no limite, somado ao ciclo do scheduler).
  - **Armadilha do `try_number` no Airflow 2.9.** O primeiro teste de sucesso logou "terminou bem
    na tentativa 2" numa execução que acertou de primeira: havia só `attempt=1.log`, nenhum retry
    no log, e `try_number = 1` na tabela `task_instance` do banco de metadados. No 2.9,
    `ti.try_number` vale a tentativa atual só enquanto a task roda; terminada (sucesso, falha ou
    retry), passa a valer a próxima. Callbacks rodam depois do fim, então os três usam
    `try_number - 1` (função `_tentativa`). O Airflow 2.10 corrige o comportamento: ao atualizar a
    imagem, tirar o `-1`.

## Próximas camadas

- 5.4 Fan-out: `separar_por_categoria` + `calcular_metricas.expand(...)` no `ecommerce_pool`.
- 5.5 Fan-in: `consolidar`.
- 5.6 Gravação idempotente com `PostgresHook` (upsert por `data_coleta` + `categoria`).
- 5.7 TaskGroups `ingestao`, `analise`, `carga`.
