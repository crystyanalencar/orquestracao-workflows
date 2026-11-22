from datetime import datetime, timezone

from ecommerce import datas


def test_execucao_agendada_grava_o_proprio_dia():
    # 06:00 em Brasília = 09:00 UTC
    assert datas.data_coleta(datetime(2026, 11, 22, 9, 0, tzinfo=timezone.utc)) == "2026-11-22"


def test_converte_para_brasilia_antes_de_tirar_a_data():
    # 01:00 UTC de 23/11 ainda é 22:00 de 22/11 em Brasília
    assert datas.data_coleta(datetime(2026, 11, 23, 1, 0, tzinfo=timezone.utc)) == "2026-11-22"
