"""Atividade 01 - métricas de produtos por categoria (FakeStore API).

Camada 5.4: fan-out. separar_por_categoria agrupa os preços e
calcular_metricas roda uma instância por categoria (.expand), limitada pelo
pool ecommerce_pool (2 slots).
"""
import logging
import math
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from airflow import __version__ as AIRFLOW_VERSION
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
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
        atraso = int(Variable.get("atraso_metricas_s", default_var=0))
        if atraso:
            log.info("Atraso de teste: %d s", atraso)
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

    produtos = buscar_produtos()
    calcular_metricas.expand(par=separar_por_categoria(produtos))


ecommerce_metricas()
