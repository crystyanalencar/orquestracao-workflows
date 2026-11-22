from types import SimpleNamespace

from ecommerce import callbacks


def _ti(try_number=2, map_index=-1):
    # try_number "adiantado", como o Airflow 2.9 entrega depois do fim da task
    return SimpleNamespace(
        dag_id="ecommerce_metricas",
        task_id="carga.gravar_postgres",
        run_id="manual__2026-11-22",
        try_number=try_number,
        map_index=map_index,
        log_url="http://localhost:8080/log?x=1",
    )


def test_tentativa_desconta_o_adiantamento_e_nunca_e_zero():
    assert callbacks.tentativa(_ti(try_number=4)) == 3
    assert callbacks.tentativa(_ti(try_number=0)) == 1  # dags test não conta tentativas


def test_alerta_tem_o_necessario_para_agir():
    alerta = callbacks.montar_alerta({"ti": _ti(), "exception": RuntimeError("banco fora")})
    assert alerta.startswith(f"[ALERTA SIMULADO] para {callbacks.CANAL_ALERTA}")
    for trecho in ("ecommerce_metricas", "carga.gravar_postgres", "manual__2026-11-22",
                   "banco fora", "http://localhost:8080/log?x=1"):
        assert trecho in alerta


def test_alerta_de_instancia_mapeada_diz_qual():
    alerta = callbacks.montar_alerta({"ti": _ti(map_index=2), "exception": None})
    assert "[instância 2]" in alerta
