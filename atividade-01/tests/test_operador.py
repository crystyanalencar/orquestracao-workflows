import pytest
from airflow.exceptions import AirflowFailException

from ecommerce.operadores import ValidarProdutosOperator


def _executar(produtos, **kwargs):
    return ValidarProdutosOperator(task_id="validar", produtos=produtos, **kwargs).execute({})


def test_lista_valida_sai_com_preco_em_float():
    saida = _executar([{"id": 1, "category": "a", "price": 10}, {"id": 2, "category": "b", "price": "5.5"}])
    assert saida == [
        {"id": 1, "category": "a", "price": 10.0},
        {"id": 2, "category": "b", "price": 5.5},
    ]


@pytest.mark.parametrize("produtos", [[], None, {"id": 1}])
def test_entrada_que_nao_e_lista_nao_vazia_falha(produtos):
    with pytest.raises(AirflowFailException):
        _executar(produtos)


def test_junta_todos_os_erros_antes_de_falhar():
    ruins = [
        {"id": 1, "category": None, "price": 1},
        "não é objeto",
        {"id": True, "category": "a", "price": 1},
        {"id": 5, "category": "a", "price": "NaN"},
    ]
    with pytest.raises(AirflowFailException, match="4 de 4 produtos inválidos") as erro:
        _executar(ruins)
    for trecho in ("categoria inválida", "não é um objeto", "id inválido", "preço inválido"):
        assert trecho in str(erro.value)


def test_id_repetido_e_erro():
    with pytest.raises(AirflowFailException, match="id repetido: 1"):
        _executar([{"id": 1, "category": "a", "price": 1}, {"id": 1, "category": "b", "price": 2}])


def test_limita_quantos_erros_aparecem_na_mensagem():
    ruins = [{"id": i, "category": "", "price": 1} for i in range(8)]
    with pytest.raises(AirflowFailException, match="e mais 6"):
        _executar(ruins, max_erros_no_log=2)


def test_produtos_e_campo_templated():
    # é isso que faz o Airflow resolver o XComArg de buscar_produtos
    assert "produtos" in ValidarProdutosOperator.template_fields
