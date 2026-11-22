"""Atividade 01 - métricas de produtos por categoria (FakeStore API).

Todo dia às 06:00 (Brasília) busca os produtos, calcula preço médio, mínimo,
máximo e quantidade por categoria e grava no Postgres sem duplicar.

Este arquivo só monta a DAG: tasks, TaskGroups e a ligação entre eles. A lógica
mora no pacote ecommerce/ ao lado. Decisões e testes em docs/decisoes.md.

    ingestao                analise                                     carga
    buscar_produtos ──► separar_por_categoria ──► calcular_metricas ×N ──► consolidar ──► gravar_postgres
       (linear)              (fan-out, .expand no ecommerce_pool)          (fan-in)
"""
import logging
import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ecommerce import api, banco, calculo, datas
from ecommerce.callbacks import ao_falhar, ao_tentar_de_novo, ao_ter_sucesso

log = logging.getLogger(__name__)


@dag(
    dag_id="ecommerce_metricas",
    schedule="0 6 * * *",  # todo dia às 06:00, no fuso do start_date
    start_date=datetime(2026, 11, 1, tzinfo=datas.TZ),
    catchup=False,
    # Uma execução por vez: uma manual não grava junto com a agendada.
    max_active_runs=1,
    tags=["atividade-01"],
)
def ecommerce_metricas():
    # ── Tasks ────────────────────────────────────────────────────────────────

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
        on_success_callback=ao_ter_sucesso,
        on_retry_callback=ao_tentar_de_novo,
        on_failure_callback=ao_falhar,
    )
    def buscar_produtos(data_interval_end=None):
        # A API só devolve o catálogo de agora. Se a execução é de outro dia
        # (Clear de uma execução antiga, disparo manual antes das 06:00), os
        # preços de hoje seriam gravados como se fossem daquele dia e
        # sobrescreveriam o histórico verdadeiro. Esperar não resolve: falha
        # direto, sem retry, e o callback de falha avisa.
        data_execucao, hoje = datas.data_coleta(data_interval_end), datas.hoje()
        if data_execucao != hoje:
            raise AirflowFailException(
                f"execução do dia {data_execucao}, mas hoje é {hoje}: a API só "
                "devolve o catálogo atual, que não pode ser gravado como outro dia"
            )

        # URL vem de uma Variable com padrão: permite forçar uma falha no teste
        # (apontando para uma URL errada) sem mexer no código.
        url = Variable.get("fakestore_url", default_var=api.URL_PRODUTOS)
        try:
            return api.buscar_produtos(url)
        except api.ErroPermanente as erro:
            # Erro temporário sobe como veio e aciona o retry; o permanente vira
            # AirflowFailException, que falha sem gastar as tentativas.
            raise AirflowFailException(str(erro)) from erro

    @task
    def separar_por_categoria(produtos):
        pares = calculo.separar_por_categoria(produtos)
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
        _atraso_de_teste()
        metricas = calculo.calcular_metricas(par["categoria"], par["precos"])
        log.info("Métricas: %s", metricas)
        return metricas

    @task
    def consolidar(metricas, data_interval_end=None):
        # Fan-in: recebe a saída de todas as instâncias mapeadas, numa lista.
        # O Airflow só roda esta task depois que todas terminam.
        # data_interval_end vem do contexto (preenchido pelo nome do parâmetro);
        # buscar_produtos já garantiu que essa data é a de hoje.
        data_coleta = datas.data_coleta(data_interval_end)
        linhas = calculo.consolidar(metricas, data_coleta)
        log.info("%d linhas para %s", len(linhas), data_coleta)
        for linha in linhas:
            log.info("  %s", linha)
        return linhas

    # Retry seguro: a gravação é idempotente (upsert + limpeza numa transação).
    # Uma queda rápida do banco às 06:00 não perde o dia.
    @task(retries=2, retry_delay=timedelta(seconds=30))
    def gravar_postgres(linhas):
        # Connection criada pelo airflow-init: credencial fora do código.
        conn = PostgresHook(postgres_conn_id="postgres_ecommerce").get_conn()
        removidas, total_dia = banco.gravar_metricas(conn, linhas)
        log.info(
            "%d linhas gravadas (upsert) para %s; %d categorias sumidas removidas; "
            "o dia tem %d linhas na tabela",
            len(linhas), linhas[0]["data_coleta"], removidas, total_dia,
        )

    # ── TaskGroups e ligação ─────────────────────────────────────────────────
    # Agrupam as tasks na UI por etapa do ETL (extrair, transformar, carregar).
    # Só organização: as dependências saem das chamadas. Efeito colateral: o id
    # de cada task ganha o prefixo do grupo (ex.: ingestao.buscar_produtos).

    @task_group(group_id="ingestao")
    def ingestao():
        return buscar_produtos()

    @task_group(group_id="analise")
    def analise(produtos):
        # linear -> fan-out (.expand) -> fan-in (consolidar)
        metricas = calcular_metricas.expand(par=separar_por_categoria(produtos))
        return consolidar(metricas)

    @task_group(group_id="carga")
    def carga(linhas):
        gravar_postgres(linhas)

    carga(analise(ingestao()))


def _atraso_de_teste():
    # Só para teste: o cálculo leva milissegundos, e a fila do pool nunca
    # apareceria. Com a Variable atraso_metricas_s, cada instância espera N
    # segundos antes de calcular. Sem ela, ou com valor inválido, 0.
    valor = Variable.get("atraso_metricas_s", default_var="0")
    try:
        atraso = max(float(valor), 0.0)
    except ValueError:
        log.warning("atraso_metricas_s inválido (%r), usando 0", valor)
        atraso = 0.0
    if atraso:
        log.info("Atraso de teste: %s s", atraso)
        time.sleep(atraso)


ecommerce_metricas()
