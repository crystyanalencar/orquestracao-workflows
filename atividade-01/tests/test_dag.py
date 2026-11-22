"""Integridade da DAG: carrega sem erro e tem a configuração que o enunciado pede."""
from datetime import timedelta

import pytest
from airflow.models import DagBag
from airflow.models.mappedoperator import MappedOperator

from conftest import DAGS


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.get_dag("ecommerce_metricas")


def test_nenhum_erro_de_import(dagbag):
    assert dagbag.import_errors == {}


def test_agendamento(dag):
    assert dag.schedule_interval == "0 6 * * *"
    assert str(dag.timezone.name) == "America/Sao_Paulo"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_tasks_nos_taskgroups(dag):
    assert set(dag.task_ids) == {
        "ingestao.buscar_produtos",
        "ingestao.validar_produtos",
        "analise.separar_por_categoria",
        "analise.calcular_metricas",
        "analise.consolidar",
        "carga.gravar_postgres",
    }


def test_buscar_produtos_resiliente(dag):
    task = dag.get_task("ingestao.buscar_produtos")
    assert task.retries == 3
    assert task.retry_exponential_backoff is True
    assert task.retry_delay == timedelta(seconds=10)
    assert task.on_success_callback and task.on_retry_callback and task.on_failure_callback


def test_calcular_metricas_mapeada_no_pool(dag):
    task = dag.get_task("analise.calcular_metricas")
    assert task.pool == "ecommerce_pool"
    # .expand cria um MappedOperator (com @task, a subclasse DecoratedMappedOperator)
    assert isinstance(task, MappedOperator)


def test_ordem_linear_fan_out_fan_in(dag):
    def depois(task_id):
        return dag.get_task(task_id).downstream_task_ids

    assert depois("ingestao.buscar_produtos") == {"ingestao.validar_produtos"}
    assert depois("ingestao.validar_produtos") == {"analise.separar_por_categoria"}
    assert depois("analise.separar_por_categoria") == {"analise.calcular_metricas"}
    assert depois("analise.calcular_metricas") == {"analise.consolidar"}
    assert depois("analise.consolidar") == {"carga.gravar_postgres"}


def test_toda_task_alerta_na_falha(dag):
    assert all(task.on_failure_callback for task in dag.tasks)
