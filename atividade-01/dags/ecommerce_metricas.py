"""Atividade 01 - métricas de produtos por categoria (FakeStore API).

Todo dia às 06:00 (Brasília) busca os produtos, calcula preço médio, mínimo,
máximo e quantidade por categoria e grava no Postgres sem duplicar. Tasks em 3
TaskGroups: ingestao, analise e carga. Decisões e testes em docs/decisoes.md.
"""
import logging
import math
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from airflow import __version__ as AIRFLOW_VERSION
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from packaging.version import Version

TZ = ZoneInfo("America/Sao_Paulo")
URL_PRODUTOS = "https://fakestoreapi.com/products"

log = logging.getLogger(__name__)

# ── SQL ──────────────────────────────────────────────────────────────────────
# Chave primária (data_coleta, categoria): uma linha por categoria por dia. É
# ela que torna a gravação idempotente, junto com o ON CONFLICT abaixo.
SQL_CRIAR_TABELA = """
CREATE TABLE IF NOT EXISTS metricas_categoria (
    data_coleta   DATE          NOT NULL,
    categoria     TEXT          NOT NULL,
    qtd_produtos  INTEGER       NOT NULL,
    preco_medio   NUMERIC(10,2) NOT NULL,
    preco_minimo  NUMERIC(10,2) NOT NULL,
    preco_maximo  NUMERIC(10,2) NOT NULL,
    atualizado_em TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (data_coleta, categoria)
)
"""

# Upsert: se a linha do dia já existe, sobrescreve em vez de duplicar.
# Valores sempre por parâmetro (%(nome)s), nunca concatenados na string:
# "men's clothing" tem apóstrofo, e concatenar abriria espaço para SQL injection.
SQL_UPSERT = """
INSERT INTO metricas_categoria
    (data_coleta, categoria, qtd_produtos, preco_medio, preco_minimo, preco_maximo)
VALUES
    (%(data_coleta)s, %(categoria)s, %(qtd)s, %(media)s, %(minimo)s, %(maximo)s)
ON CONFLICT (data_coleta, categoria) DO UPDATE SET
    qtd_produtos  = EXCLUDED.qtd_produtos,
    preco_medio   = EXCLUDED.preco_medio,
    preco_minimo  = EXCLUDED.preco_minimo,
    preco_maximo  = EXCLUDED.preco_maximo,
    atualizado_em = now()
"""

# Reprocessar um dia em que uma categoria sumiu da API deixaria a linha antiga
# dela para trás. Apaga as categorias do dia que não vieram neste lote, para o
# dia gravado ser exatamente o retrato da última coleta.
SQL_LIMPAR_SUMIDAS = """
DELETE FROM metricas_categoria
WHERE data_coleta = %(data_coleta)s
  AND NOT (categoria = ANY(%(categorias)s))
"""


# ── Callbacks ────────────────────────────────────────────────────────────────
# O Airflow chama cada um com o "context" da execução (o mesmo dicionário que
# preenche ds, data_interval_end etc.). Normalmente rodam no processo da task,
# então o que logam aparece no log da própria task na UI. Exceção: se o processo
# da task morre (falta de memória, heartbeat perdido), o de falha roda no
# DagFileProcessor e loga lá, não no log da task.

# Antes do 2.10, ti.try_number só vale a tentativa atual enquanto a task roda.
# Depois que ela termina (sucesso, falha ou retry), passa a valer a PRÓXIMA.
# Os callbacks rodam depois do fim, então nessas versões é preciso -1.
_TRY_NUMBER_ADIANTADO = Version(AIRFLOW_VERSION) < Version("2.10")


def _tentativa(ti):
    n = ti.try_number - 1 if _TRY_NUMBER_ADIANTADO else ti.try_number
    # `airflow dags test` e `tasks test` rodam em modo de teste, que não conta
    # tentativas (try_number fica 0 no banco); sem o max, o log diria "tentativa 0".
    return max(n, 1)


def _ao_ter_sucesso(context):
    ti = context["ti"]
    log.info("[callback sucesso] %s terminou bem na tentativa %s", ti.task_id, _tentativa(ti))


def _ao_tentar_de_novo(context):
    ti = context["ti"]
    log.warning(
        "[callback retry] %s falhou na tentativa %s e vai tentar de novo. Erro: %s",
        ti.task_id,
        _tentativa(ti),
        context.get("exception"),
    )


def _ao_falhar(context):
    # Aqui entraria um alerta de verdade (e-mail, Slack). Para a atividade, log.
    ti = context["ti"]
    log.error(
        "[callback falha] %s falhou de vez na tentativa %s. Erro final: %s",
        ti.task_id,
        _tentativa(ti),
        context.get("exception"),
    )


# ── Validação ────────────────────────────────────────────────────────────────

# Maior valor que cabe na coluna NUMERIC(10,2) da tabela de destino.
PRECO_MAXIMO = 99_999_999.99


def _preco(valor):
    # float() aceita "NaN", "inf", True e negativos; qualquer um deles estragaria
    # média, mínimo e máximo das camadas seguintes sem erro nenhum. Acima do
    # limite da coluna, só estouraria lá no fim, na gravação.
    if isinstance(valor, bool):
        raise TypeError(f"preço booleano: {valor!r}")
    preco = float(valor)
    if not math.isfinite(preco) or not 0 <= preco <= PRECO_MAXIMO:
        raise ValueError(f"preço inválido: {valor!r}")
    return preco


def _categoria(valor):
    # Categoria nula ou vazia quebraria separar_por_categoria (sorted, join), uma
    # task sem retry nem callback; aqui vira erro de formato com alerta.
    if not isinstance(valor, str) or not valor.strip():
        raise ValueError(f"categoria inválida: {valor!r}")
    return valor


def _data_coleta(data_interval_end):
    # Data que a execução grava: o fim do intervalo, em Brasília. Em execução
    # agendada é o dia em que ela roda (06:00). Em execução manual, o Airflow
    # recua o fim para o último 06:00 antes do disparo: disparar às 05:30 cai
    # no dia anterior. Convertido antes de tirar a data porque em UTC uma
    # execução depois das 21:00 cairia no dia seguinte. Texto ISO: XCom é JSON.
    return data_interval_end.astimezone(TZ).date().isoformat()


@dag(
    dag_id="ecommerce_metricas",
    schedule="0 6 * * *",  # todo dia às 06:00, no fuso do start_date
    start_date=datetime(2026, 11, 1, tzinfo=TZ),
    catchup=False,
    # Uma execução por vez: uma manual não grava junto com a agendada.
    max_active_runs=1,
    tags=["atividade-01"],
)
def ecommerce_metricas():
    @task(
        retries=3,
        retry_delay=timedelta(seconds=10),
        # Espera dobra a cada falha: dá tempo de a API se recuperar sem
        # martelar. O Airflow espalha cada espera entre o mínimo e o dobro
        # (10-20s, 20-40s, 40-80s) por um hash de dag, task, data e tentativa:
        # tasks diferentes não voltam no mesmo segundo, mas a mesma execução e
        # tentativa sempre esperam o mesmo tempo (não é sorteio).
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        on_success_callback=_ao_ter_sucesso,
        on_retry_callback=_ao_tentar_de_novo,
        on_failure_callback=_ao_falhar,
    )
    def buscar_produtos(data_interval_end=None):
        # A API só devolve o catálogo de agora. Se a execução é de outro dia
        # (Clear de uma execução antiga, disparo manual antes das 06:00), os
        # preços de hoje seriam gravados como se fossem daquele dia e
        # sobrescreveriam o histórico verdadeiro. Esperar não resolve: falha
        # direto, sem retry, e o callback de falha avisa.
        data_execucao = _data_coleta(data_interval_end)
        hoje = datetime.now(TZ).date().isoformat()
        if data_execucao != hoje:
            raise AirflowFailException(
                f"execução do dia {data_execucao}, mas hoje é {hoje}: a API só "
                "devolve o catálogo atual, que não pode ser gravado como outro dia"
            )

        # URL vem de uma Variable com padrão: permite forçar uma falha no teste
        # (apontando para uma URL errada) sem mexer no código.
        url = Variable.get("fakestore_url", default_var=URL_PRODUTOS)

        # Erro temporário (rede, timeout, 5xx, 408, 425, 429): relança e o retry cuida.
        # Erro permanente (outro 4xx, formato errado, lista vazia): esperar não
        # resolve, então AirflowFailException, que falha sem gastar os retries.
        try:
            # timeout: sem ele, uma API travada prende o worker para sempre.
            resposta = requests.get(url, timeout=30)
            # 404/500 não viram exceção sozinhos; isto faz a task falhar de verdade.
            resposta.raise_for_status()
        except requests.HTTPError as erro:
            status = erro.response.status_code
            if status in (408, 425, 429) or status >= 500:
                log.error("Erro temporário HTTP %s em %s", status, url)
                raise
            log.error("Erro permanente HTTP %s em %s", status, url)
            raise AirflowFailException(f"HTTP {status} em {url}") from erro
        except requests.RequestException as erro:
            # Conexão recusada, DNS, timeout.
            log.error("Falha de rede ao chamar %s: %s", url, erro)
            raise

        # Bloco separado: resposta.json() levanta JSONDecodeError, que é ao mesmo
        # tempo RequestException e ValueError; junto do bloco acima, seria
        # confundido com falha de rede.
        try:
            produtos = resposta.json()
            # XCom guarda no banco de metadados: levar só os 3 campos usados.
            enxuto = [
                {
                    "id": p["id"],
                    "category": _categoria(p["category"]),
                    "price": _preco(p["price"]),
                }
                for p in produtos
            ]
        except (ValueError, KeyError, TypeError) as erro:
            # Resposta veio, mas não é o JSON esperado (campo faltando, formato errado).
            log.error("Resposta inesperada de %s: %r", url, erro)
            raise AirflowFailException(f"resposta inesperada de {url}: {erro!r}") from erro

        # Lista vazia não é erro de rede, mas calcular métricas sobre nada
        # gravaria um dia "sem produtos" como se fosse verdade.
        if not enxuto:
            raise AirflowFailException(f"{url} devolveu 0 produtos")

        log.info("%d produtos recebidos", len(enxuto))
        return enxuto

    @task
    def separar_por_categoria(produtos):
        # Agrupa os preços por categoria. Cada item da lista vira uma instância
        # de calcular_metricas no .expand, e já leva os preços junto: nenhuma
        # instância precisa chamar a API de novo.
        precos_por_categoria = {}
        for p in produtos:
            precos_por_categoria.setdefault(p["category"], []).append(p["price"])
        pares = [
            {"categoria": categoria, "precos": precos}
            for categoria, precos in sorted(precos_por_categoria.items())
        ]
        log.info("%d categorias: %s", len(pares), ", ".join(p["categoria"] for p in pares))
        return pares

    @task(
        # 2 slots: com 4 categorias, no máximo 2 instâncias rodam ao mesmo
        # tempo; as outras esperam na fila do pool.
        pool="ecommerce_pool",
        # Na UI, cada instância aparece com o nome da categoria em vez de 0..3.
        map_index_template="{{ categoria }}",
    )
    def calcular_metricas(par):
        get_current_context()["categoria"] = par["categoria"]

        # Só para teste: o cálculo leva milissegundos, e a fila do pool nunca
        # apareceria. Com a Variable atraso_metricas_s, cada instância espera
        # N segundos antes de calcular. Sem ela, 0.
        valor = Variable.get("atraso_metricas_s", default_var="0")
        try:
            atraso = max(float(valor), 0.0)
        except ValueError:
            log.warning("atraso_metricas_s inválido (%r), usando 0", valor)
            atraso = 0.0
        if atraso:
            log.info("Atraso de teste: %s s", atraso)
            time.sleep(atraso)

        precos = par["precos"]
        metricas = {
            "categoria": par["categoria"],
            "qtd": len(precos),
            "media": round(sum(precos) / len(precos), 2),
            "minimo": min(precos),
            "maximo": max(precos),
        }
        log.info("Métricas: %s", metricas)
        return metricas

    @task
    def consolidar(metricas, data_interval_end=None):
        # Fan-in: recebe a saída de todas as instâncias mapeadas, numa lista.
        # O Airflow só roda esta task depois que todas terminam.
        #
        # data_interval_end vem do contexto (o Airflow preenche pelo nome do
        # parâmetro). É o fim do intervalo, não o ds: a execução das 06:00 de
        # 23/11 grava 23/11, que é quando a API foi consultada. buscar_produtos
        # já garantiu que essa data é a de hoje.
        data_coleta = _data_coleta(data_interval_end)
        linhas = [{"data_coleta": data_coleta, **m} for m in metricas]
        linhas.sort(key=lambda linha: linha["categoria"])
        log.info("%d linhas para %s", len(linhas), data_coleta)
        for linha in linhas:
            log.info("  %s", linha)
        return linhas

    # Retry seguro: a gravação é idempotente (upsert + limpeza numa transação).
    # Uma queda rápida do banco às 06:00 não perde o dia.
    @task(retries=2, retry_delay=timedelta(seconds=30))
    def gravar_postgres(linhas):
        # Connection criada pelo airflow-init: credencial fora do código.
        hook = PostgresHook(postgres_conn_id="postgres_ecommerce")
        data_coleta = linhas[0]["data_coleta"]
        categorias = [linha["categoria"] for linha in linhas]

        conn = hook.get_conn()
        try:
            # Uma transação só: ou o dia inteiro é gravado, ou nada muda. O
            # "with conn" faz commit no fim e rollback se algo levantar erro.
            with conn, conn.cursor() as cur:
                cur.execute(SQL_CRIAR_TABELA)
                cur.executemany(SQL_UPSERT, linhas)
                cur.execute(
                    SQL_LIMPAR_SUMIDAS,
                    {"data_coleta": data_coleta, "categorias": categorias},
                )
                removidas = cur.rowcount
                cur.execute(
                    "SELECT count(*) FROM metricas_categoria WHERE data_coleta = %s",
                    (data_coleta,),
                )
                total_dia = cur.fetchone()[0]
        finally:
            conn.close()

        log.info(
            "%d linhas gravadas (upsert) para %s; %d categorias sumidas removidas; "
            "o dia tem %d linhas na tabela",
            len(linhas), data_coleta, removidas, total_dia,
        )

    # ── TaskGroups ───────────────────────────────────────────────────────────
    # Agrupam as tasks na UI por etapa do ETL (extrair, transformar, carregar).
    # Só organização: as dependências continuam saindo das chamadas. Efeito
    # colateral: o id de cada task ganha o prefixo do grupo
    # (ex.: ingestao.buscar_produtos), inclusive em `airflow tasks test`.

    @task_group(group_id="ingestao")
    def ingestao():
        return buscar_produtos()

    @task_group(group_id="analise")
    def analise(produtos):
        # linear -> fan-out (.expand) -> fan-in (consolidar), tudo neste grupo
        metricas = calcular_metricas.expand(par=separar_por_categoria(produtos))
        return consolidar(metricas)

    @task_group(group_id="carga")
    def carga(linhas):
        gravar_postgres(linhas)

    carga(analise(ingestao()))


ecommerce_metricas()
