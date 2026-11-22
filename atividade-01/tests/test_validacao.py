import pytest

from ecommerce import validacao


@pytest.mark.parametrize("valor, esperado", [(10, 10.0), (22.3, 22.3), ("5", 5.0), (0, 0.0)])
def test_preco_valido_vira_float(valor, esperado):
    assert validacao.preco(valor) == esperado


def test_preco_no_limite_da_coluna_passa():
    assert validacao.preco(validacao.PRECO_MAXIMO) == validacao.PRECO_MAXIMO


@pytest.mark.parametrize("valor", ["NaN", "inf", "-inf", -1, 1e9, True, None, "abc"])
def test_preco_invalido_e_rejeitado(valor):
    with pytest.raises((ValueError, TypeError)):
        validacao.preco(valor)


def test_categoria_valida_volta_igual():
    assert validacao.categoria("men's clothing") == "men's clothing"


@pytest.mark.parametrize("valor", [None, "", "   ", 3, ["a"]])
def test_categoria_invalida_e_rejeitada(valor):
    with pytest.raises(ValueError):
        validacao.categoria(valor)
