"""Regra de erro da api.py, com o requests simulado (sem rede)."""
from unittest import mock

import pytest
import requests

from ecommerce import api


def _resposta(status=200, corpo=None, json_invalido=False):
    resposta = requests.Response()
    resposta.status_code = status
    resposta.url = api.URL_PRODUTOS
    resposta._content = b"<html>" if json_invalido else __import__("json").dumps(corpo).encode()
    return resposta


def _buscar_com(resposta=None, erro=None):
    with mock.patch("ecommerce.api.requests.get") as get:
        if erro:
            get.side_effect = erro
        else:
            get.return_value = resposta
        return api.buscar_produtos(api.URL_PRODUTOS)


def test_sucesso_extrai_so_os_tres_campos():
    corpo = [{"id": 1, "category": "a", "price": 10, "title": "x", "image": "y"}]
    assert _buscar_com(_resposta(corpo=corpo)) == [{"id": 1, "category": "a", "price": 10}]


def test_campo_ausente_vira_none_para_o_operador_apontar():
    assert _buscar_com(_resposta(corpo=[{"id": 1}])) == [{"id": 1, "category": None, "price": None}]


@pytest.mark.parametrize("status", [500, 503, 408, 425, 429])
def test_http_temporario_sobe_como_httperror_para_o_retry(status):
    with pytest.raises(requests.HTTPError):
        _buscar_com(_resposta(status=status, corpo={}))


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_http_permanente_vira_erro_permanente(status):
    with pytest.raises(api.ErroPermanente):
        _buscar_com(_resposta(status=status, corpo={}))


@pytest.mark.parametrize("erro", [requests.ConnectionError("dns"), requests.Timeout("lento")])
def test_falha_de_rede_sobe_para_o_retry(erro):
    with pytest.raises(type(erro)):
        _buscar_com(erro=erro)


def test_corpo_que_nao_e_json_e_permanente_nao_falha_de_rede():
    # JSONDecodeError é RequestException e ValueError ao mesmo tempo
    with pytest.raises(api.ErroPermanente, match="não é JSON"):
        _buscar_com(_resposta(json_invalido=True))


@pytest.mark.parametrize("corpo", [[], {"produtos": []}])
def test_lista_vazia_ou_objeto_e_permanente(corpo):
    with pytest.raises(api.ErroPermanente):
        _buscar_com(_resposta(corpo=corpo))
