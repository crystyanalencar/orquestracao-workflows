"""Atividade 01 - métricas de produtos por categoria (FakeStore API).

Camada 5.3: buscar_produtos ganha resiliência - try/except com raise, retry com
backoff exponencial e callbacks de sucesso, retry e falha.
"""
import logging
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable

TZ = pendulum.timezone("America/Sao_Paulo")
URL_PRODUTOS = "https://fakestoreapi.com/products"

log = logging.getLogger(__name__)


# ── Callbacks ────────────────────────────────────────────────────────────────
# O Airflow chama cada um com o "context" da execução (o mesmo dicionário que
# preenche ds, data_interval_end etc.). Rodam no processo da task, então o que
# logam aparece no log da própria task na UI.

def _tentativa(ti):
    # No Airflow 2.9, ti.try_number só vale a tentativa atual enquanto a task
    # roda. Depois que ela termina (sucesso, falha ou retry), passa a valer a
    # PRÓXIMA tentativa. Os callbacks rodam depois do fim, então -1. O 2.10
    # corrige isso: ao atualizar a imagem, tirar o -1.
    return ti.try_number - 1


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
        "[callback falha] %s esgotou as tentativas (%s). Erro final: %s",
        ti.task_id,
        _tentativa(ti),
        context.get("exception"),
    )


@dag(
    dag_id="ecommerce_metricas",
    schedule="0 6 * * *",  # todo dia às 06:00, no fuso do start_date
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,
    tags=["atividade-01"],
)
def ecommerce_metricas():
    @task(
        retries=3,
        retry_delay=timedelta(seconds=10),
        # Espera dobra a cada falha: dá tempo de a API se recuperar sem
        # martelar. O Airflow sorteia entre o mínimo e o dobro dele (10-20s,
        # 20-40s, 40-80s), para várias tasks não voltarem no mesmo segundo.
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
        try:
            # timeout: sem ele, uma API travada prende o worker para sempre.
            resposta = requests.get(url, timeout=30)
            # 404/500 não viram exceção sozinhos; isto faz a task falhar de verdade.
            resposta.raise_for_status()
            produtos = resposta.json()
            # XCom guarda no banco de metadados: levar só os 3 campos usados.
            # price vem int ou float na API.
            enxuto = [
                {"id": p["id"], "category": p["category"], "price": float(p["price"])}
                for p in produtos
            ]
        except requests.RequestException as erro:
            # Rede, timeout ou HTTP 4xx/5xx.
            log.error("Falha ao chamar %s: %s", url, erro)
            raise
        except (ValueError, KeyError, TypeError) as erro:
            # Resposta veio, mas não é o JSON esperado (campo faltando, formato errado).
            log.error("Resposta inesperada de %s: %r", url, erro)
            raise

        # Lista vazia não é erro de rede, mas calcular métricas sobre nada
        # gravaria um dia "sem produtos" como se fosse verdade.
        if not enxuto:
            raise ValueError(f"{url} devolveu 0 produtos")

        log.info("%d produtos recebidos", len(enxuto))
        return enxuto

    buscar_produtos()


ecommerce_metricas()
