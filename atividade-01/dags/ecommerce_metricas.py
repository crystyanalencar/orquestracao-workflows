"""Atividade 01 - métricas de produtos por categoria (FakeStore API).

Camada 5.3: buscar_produtos ganha resiliência - erro temporário tenta de novo
com backoff exponencial, erro permanente falha direto, e callbacks de sucesso,
retry e falha.
"""
import logging
import math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from airflow import __version__ as AIRFLOW_VERSION
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from packaging.version import Version

TZ = ZoneInfo("America/Sao_Paulo")
URL_PRODUTOS = "https://fakestoreapi.com/products"

log = logging.getLogger(__name__)


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
    return ti.try_number - 1 if _TRY_NUMBER_ADIANTADO else ti.try_number


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

def _preco(valor):
    # float() aceita "NaN", "inf", True e negativos; qualquer um deles estragaria
    # média, mínimo e máximo das camadas seguintes sem erro nenhum.
    if isinstance(valor, bool):
        raise TypeError(f"preço booleano: {valor!r}")
    preco = float(valor)
    if not math.isfinite(preco) or preco < 0:
        raise ValueError(f"preço inválido: {valor!r}")
    return preco


@dag(
    dag_id="ecommerce_metricas",
    schedule="0 6 * * *",  # todo dia às 06:00, no fuso do start_date
    start_date=datetime(2026, 11, 1, tzinfo=TZ),
    catchup=False,
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
    def buscar_produtos():
        # URL vem de uma Variable com padrão: permite forçar uma falha no teste
        # (apontando para uma URL errada) sem mexer no código.
        url = Variable.get("fakestore_url", default_var=URL_PRODUTOS)

        # Erro temporário (rede, timeout, 5xx, 429): relança e o retry cuida.
        # Erro permanente (outro 4xx, formato errado, lista vazia): esperar não
        # resolve, então AirflowFailException, que falha sem gastar os retries.
        try:
            # timeout: sem ele, uma API travada prende o worker para sempre.
            resposta = requests.get(url, timeout=30)
            # 404/500 não viram exceção sozinhos; isto faz a task falhar de verdade.
            resposta.raise_for_status()
        except requests.HTTPError as erro:
            status = erro.response.status_code
            if status == 429 or status >= 500:
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
                {"id": p["id"], "category": p["category"], "price": _preco(p["price"])}
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

    buscar_produtos()


ecommerce_metricas()
