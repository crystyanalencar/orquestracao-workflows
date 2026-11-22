"""Gravação com uma conexão simulada: confere a ordem, os parâmetros e a transação."""
from unittest import mock

import pytest

from ecommerce import banco

LINHAS = [
    {"data_coleta": "2026-11-22", "categoria": "a", "qtd": 1, "media": 1.0, "minimo": 1.0, "maximo": 1.0},
    {"data_coleta": "2026-11-22", "categoria": "men's clothing", "qtd": 2, "media": 2.0, "minimo": 1.0, "maximo": 3.0},
]


def _conexao(removidas=0, total=2):
    conn = mock.MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.rowcount = removidas
    cur.fetchone.return_value = (total,)
    return conn, cur


def test_grava_snapshot_limpeza_e_historico_com_parametros():
    conn, cur = _conexao(removidas=1, total=2)
    assert banco.gravar_metricas(conn, LINHAS, "run_x") == (1, 2)

    sqls = [chamada.args[0] for chamada in cur.execute.call_args_list + cur.executemany.call_args_list]
    assert banco.SQL_CRIAR_TABELA in sqls and banco.SQL_CRIAR_HISTORICO in sqls

    upsert, historico = cur.executemany.call_args_list
    # valores vão como parâmetro, nunca dentro da string: o apóstrofo não quebra nada
    assert upsert.args == (banco.SQL_UPSERT, LINHAS)
    assert "men's" not in banco.SQL_UPSERT
    assert historico.args[0] == banco.SQL_INSERIR_HISTORICO
    assert all(linha["run_id"] == "run_x" for linha in historico.args[1])

    limpeza = next(c for c in cur.execute.call_args_list if c.args[0] == banco.SQL_LIMPAR_SUMIDAS)
    assert limpeza.args[1] == {"data_coleta": "2026-11-22", "categorias": ["a", "men's clothing"]}


def test_usa_a_conexao_como_transacao_e_fecha():
    conn, _ = _conexao()
    banco.gravar_metricas(conn, LINHAS, "run_x")
    conn.__enter__.assert_called_once()   # "with conn": commit no fim
    conn.close.assert_called_once()


def test_fecha_a_conexao_mesmo_com_erro():
    conn, cur = _conexao()
    cur.executemany.side_effect = RuntimeError("banco caiu")
    with pytest.raises(RuntimeError):
        banco.gravar_metricas(conn, LINHAS, "run_x")
    conn.close.assert_called_once()
