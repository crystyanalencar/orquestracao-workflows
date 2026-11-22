"""Callbacks de ciclo de vida da task crítica (buscar_produtos).

O Airflow chama cada um com o "context" da execução (o mesmo dicionário que
preenche ds, data_interval_end etc.). Normalmente rodam no processo da task,
então o que logam aparece no log da própria task na UI. Exceção: se o processo
da task morre (falta de memória, heartbeat perdido), o de falha roda no
DagFileProcessor e loga lá, não no log da task.
"""
import logging

from airflow import __version__ as AIRFLOW_VERSION
from packaging.version import Version

log = logging.getLogger(__name__)

# Antes do 2.10, ti.try_number só vale a tentativa atual enquanto a task roda.
# Depois que ela termina (sucesso, falha ou retry), passa a valer a PRÓXIMA.
# Os callbacks rodam depois do fim, então nessas versões é preciso -1.
_TRY_NUMBER_ADIANTADO = Version(AIRFLOW_VERSION) < Version("2.10")


def tentativa(ti):
    n = ti.try_number - 1 if _TRY_NUMBER_ADIANTADO else ti.try_number
    # `airflow dags test` e `tasks test` rodam em modo de teste, que não conta
    # tentativas (try_number fica 0 no banco); sem o max, o log diria "tentativa 0".
    return max(n, 1)


def ao_ter_sucesso(context):
    ti = context["ti"]
    log.info("[callback sucesso] %s terminou bem na tentativa %s", ti.task_id, tentativa(ti))


def ao_tentar_de_novo(context):
    ti = context["ti"]
    log.warning(
        "[callback retry] %s falhou na tentativa %s e vai tentar de novo. Erro: %s",
        ti.task_id,
        tentativa(ti),
        context.get("exception"),
    )


def ao_falhar(context):
    # Aqui entraria um alerta de verdade (e-mail, Slack). Para a atividade, log.
    ti = context["ti"]
    log.error(
        "[callback falha] %s falhou de vez na tentativa %s. Erro final: %s",
        ti.task_id,
        tentativa(ti),
        context.get("exception"),
    )
