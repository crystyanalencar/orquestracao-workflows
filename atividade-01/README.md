# Atividade 01: métricas de produtos por categoria com Airflow

Pipeline em Apache Airflow 2.9.3 que substitui um script de cron. Todo dia às 06:00 (Brasília)
ele busca os produtos da [FakeStore API](https://fakestoreapi.com), calcula por categoria o preço
médio, mínimo, máximo e a quantidade de produtos, e grava no PostgreSQL sem duplicar. Também
guarda um histórico em append para acompanhar a evolução dos preços.

```
ingestao                                analise                                                  carga
buscar_produtos ──► validar_produtos ──► separar_por_categoria ──► calcular_metricas ×N ──► consolidar ──► gravar_postgres
   (linear)       (operador customizado)     (fan-out: .expand por categoria, pool de 2)   (fan-in)
```

O porquê de cada decisão, como cada parte foi testada e o que se observou estão em
[`docs/decisoes.md`](docs/decisoes.md). As evidências (logs, prints, consultas ao banco) estão em
[`docs/evidencias/`](docs/evidencias/).

## Como rodar

Requisito: Docker com Docker Compose v2. Portas livres: 8080 (UI) e 5434 (Postgres de destino).

```bash
git clone https://github.com/crystyanalencar/orquestracao-workflows.git
cd orquestracao-workflows/atividade-01
docker compose up -d
docker compose ps        # esperar scheduler e webserver "healthy" (~1 min)
```

- UI: <http://localhost:8080>, usuário `admin`, senha `admin`.
- A DAG `ecommerce_metricas` nasce pausada. Despausar no botão ao lado do nome e disparar com ▶.
- O serviço `airflow-init` cria o banco, o usuário, a Connection `postgres_ecommerce` e o pool
  `ecommerce_pool` (2 slots). Nada precisa ser configurado pela UI.
- Opcional, em Linux/WSL: `echo "AIRFLOW_UID=$(id -u)" > .env` antes de subir, para os arquivos
  de log saírem com o seu usuário como dono.

Parar sem perder dados: `docker compose stop`. Apagar tudo, inclusive os bancos:
`docker compose down -v`.

Ver o resultado:

```bash
docker compose exec postgres-ecommerce psql -U ecommerce -d ecommerce -c "SELECT * FROM metricas_categoria ORDER BY data_coleta, categoria;"
```

## Requisitos

| Requisito do enunciado | Onde está | Evidência |
|---|---|---|
| TaskFlow (`@dag`/`@task`), dependências pelas chamadas, XCom só com dado pequeno | [`dags/ecommerce_metricas.py`](dags/ecommerce_metricas.py) | 5.2 |
| Topologias linear, fan-out e fan-in identificáveis | grupo `analise`: `separar_por_categoria` → `calcular_metricas.expand(...)` → `consolidar` | [5.5](docs/evidencias/5.5/), [5.7](docs/evidencias/5.7/) |
| 06:00 em `America/Sao_Paulo`, `start_date`, `catchup=False` | `@dag(...)`; fuso com `zoneinfo` (nativo do Python) em vez de pendulum, ver [decisoes](docs/decisoes.md#51-esqueleto-da-dag) | 5.1 |
| Retry com exponential backoff | `buscar_produtos`: `retries=3`, `retry_exponential_backoff=True` | [5.3](docs/evidencias/5.3/) |
| try/except + raise | [`dags/ecommerce/api.py`](dags/ecommerce/api.py): temporário relança (retry), permanente falha direto | [5.3](docs/evidencias/5.3/) |
| Callbacks de sucesso, retry e falha | [`dags/ecommerce/callbacks.py`](dags/ecommerce/callbacks.py) | [5.3](docs/evidencias/5.3/) |
| Dynamic Task Mapping por categoria | `calcular_metricas.expand(par=...)` | [5.4](docs/evidencias/5.4/) |
| Pool `ecommerce_pool` com 2 slots | `@task(pool="ecommerce_pool")`, criado pelo `airflow-init` | [5.4](docs/evidencias/5.4/) |
| Pelo menos 2 TaskGroups | 3: `ingestao`, `analise`, `carga` | [5.7](docs/evidencias/5.7/) |
| `PostgresHook` + Connection, gravação idempotente | [`dags/ecommerce/banco.py`](dags/ecommerce/banco.py): upsert por `(data_coleta, categoria)` | [5.6](docs/evidencias/5.6/) |
| Projeto dockerizado | [`docker-compose.yml`](docker-compose.yml) | esta seção |

Opcionais:

| Opcional | Situação |
|---|---|
| Tabela de histórico em append | feito: `metricas_categoria_historico`, na mesma transação do snapshot ([evidência](docs/evidencias/opcional-historico/)) |
| SLA / alerta | `on_failure_callback` simula o envio do alerta (log `[callback falha]`); um alerta real (e-mail, Slack) entraria nesse ponto |
| `ValidarProdutosOperator` | feito: [`dags/ecommerce/operadores.py`](dags/ecommerce/operadores.py), entre `buscar_produtos` e `separar_por_categoria` ([evidência](docs/evidencias/opcional-operador/)) |

Além do pedido: a gravação recusa execução de outro dia (a API só devolve o catálogo atual, então
um reprocessamento antigo gravaria os preços de hoje por cima do histórico), a validação rejeita
preço inválido e categoria vazia, e `max_active_runs=1` impede duas gravações ao mesmo tempo.

## Como testar

Todos os comandos rodam na pasta `atividade-01`. As Variables abaixo existem só para teste: sem
elas, a DAG usa os valores reais. Apagar cada uma depois do teste, senão a execução agendada é
afetada.

| O que testar | Como | O que esperar |
|---|---|---|
| Execução normal | disparar a DAG na UI | tudo verde; 4 linhas no dia em `metricas_categoria` |
| Idempotência | **Clear** em `carga.gravar_postgres`, ou disparar de novo no mesmo dia | continua com 4 linhas no dia; `atualizado_em` avança |
| Erro temporário (retry com backoff) | `docker compose exec airflow-scheduler airflow variables set fakestore_url https://fakestoreapi.invalid/products` e disparar | 4 tentativas, `[callback retry]` nas 3 primeiras, `[callback falha]` no fim |
| Erro permanente (sem retry) | `... variables set fakestore_url https://fakestoreapi.com/produtosx` e disparar | falha na 1ª tentativa, `[callback falha]` |
| Schema inválido (operador) | `... variables set fakestore_url https://fakestoreapi.com/users` e disparar | `buscar_produtos` ok, `validar_produtos` falha com `10 de 10 produtos inválidos`, `[callback falha]` |
| Pool limitando a 2 | `... variables set atraso_metricas_s 15` e disparar; olhar Browse → Task Instances | 2 instâncias `running`, 2 `scheduled`, em duas levas |
| Execução de outro dia recusada | `docker compose exec airflow-scheduler airflow dags test ecommerce_metricas 2026-01-10` | `execução do dia ... mas hoje é ...`, nada gravado |

Apagar as Variables de teste: `docker compose exec airflow-scheduler airflow variables delete <nome>`.

## Estrutura

```
atividade-01/
  docker-compose.yml        Airflow 2.9.3 (LocalExecutor) + Postgres de metadados + Postgres de destino
  dags/
    ecommerce_metricas.py   só a DAG: tasks, TaskGroups e a ligação entre eles
    ecommerce/              a lógica, testável sem Airflow
      api.py                busca na API; erro temporário x permanente
      validacao.py          preço e categoria
      operadores.py         ValidarProdutosOperator
      calculo.py            agrupamento e métricas
      banco.py              SQL, gravação idempotente e histórico
      callbacks.py          callbacks de sucesso, retry e falha
      datas.py              fuso e data da coleta
  docs/
    decisoes.md             decisões, testes e observações, camada a camada
    evidencias/             logs, prints e consultas de cada teste
```

As credenciais do compose (`admin/admin`, `ecommerce/ecommerce`) são só para desenvolvimento local.
