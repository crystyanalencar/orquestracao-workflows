"""Busca na FakeStore API, separando erro temporário de permanente."""
import logging

import requests

URL_PRODUTOS = "https://fakestoreapi.com/products"

# HTTP que pode melhorar esperando: o retry cuida. Além destes, todo 5xx.
STATUS_TEMPORARIOS = (408, 425, 429)

log = logging.getLogger(__name__)


class ErroPermanente(Exception):
    """Erro que não melhora esperando: a DAG falha direto, sem gastar retries."""


# Só os campos que o pipeline usa: o retorno vira XCom, guardado no banco de
# metadados (sem título, descrição e imagem).
CAMPOS = ("id", "category", "price")


def buscar_produtos(url=URL_PRODUTOS):
    """Devolve [{id, category, price}] da API, ainda sem validar os valores.

    Erro temporário (rede, timeout, 5xx, 408, 425, 429): a exceção do requests
    sobe como veio, e o retry da task cuida. Erro permanente (outro 4xx, corpo
    que não é JSON, lista vazia): ErroPermanente. Tipo e valor de cada campo são
    conferidos depois, pelo ValidarProdutosOperator.
    """
    try:
        # timeout: sem ele, uma API travada prende o worker para sempre.
        resposta = requests.get(url, timeout=30)
        # 404/500 não viram exceção sozinhos; isto faz a task falhar de verdade.
        resposta.raise_for_status()
    except requests.HTTPError as erro:
        status = erro.response.status_code
        if status in STATUS_TEMPORARIOS or status >= 500:
            log.error("Erro temporário HTTP %s em %s", status, url)
            raise
        log.error("Erro permanente HTTP %s em %s", status, url)
        raise ErroPermanente(f"HTTP {status} em {url}") from erro
    except requests.RequestException as erro:
        # Conexão recusada, DNS, timeout.
        log.error("Falha de rede ao chamar %s: %s", url, erro)
        raise

    # Bloco separado: resposta.json() levanta JSONDecodeError, que é ao mesmo
    # tempo RequestException e ValueError; junto do bloco acima, seria
    # confundido com falha de rede.
    try:
        produtos = resposta.json()
    except ValueError as erro:
        log.error("Resposta de %s não é JSON: %r", url, erro)
        raise ErroPermanente(f"resposta de {url} não é JSON: {erro!r}") from erro

    if not isinstance(produtos, list):
        raise ErroPermanente(f"{url} devolveu {type(produtos).__name__}, esperava lista")
    # Lista vazia não é erro de rede, mas calcular métricas sobre nada
    # gravaria um dia "sem produtos" como se fosse verdade.
    if not produtos:
        raise ErroPermanente(f"{url} devolveu 0 produtos")

    # Campo ausente vira None e item que não é objeto passa como veio: o
    # operador de validação aponta o problema produto a produto.
    enxuto = [
        {campo: p.get(campo) for campo in CAMPOS} if isinstance(p, dict) else p
        for p in produtos
    ]

    log.info("%d produtos recebidos", len(enxuto))
    return enxuto
