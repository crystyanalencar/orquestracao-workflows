"""Busca na FakeStore API, separando erro temporário de permanente."""
import logging

import requests

from ecommerce import validacao

URL_PRODUTOS = "https://fakestoreapi.com/products"

# HTTP que pode melhorar esperando: o retry cuida. Além destes, todo 5xx.
STATUS_TEMPORARIOS = (408, 425, 429)

log = logging.getLogger(__name__)


class ErroPermanente(Exception):
    """Erro que não melhora esperando: a DAG falha direto, sem gastar retries."""


def buscar_produtos(url=URL_PRODUTOS):
    """Devolve [{id, category, price}] da API.

    Erro temporário (rede, timeout, 5xx, 408, 425, 429): a exceção do requests
    sobe como veio, e o retry da task cuida. Erro permanente (outro 4xx, formato
    errado, lista vazia): ErroPermanente.
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
        # XCom guarda no banco de metadados: levar só os 3 campos usados.
        enxuto = [
            {
                "id": p["id"],
                "category": validacao.categoria(p["category"]),
                "price": validacao.preco(p["price"]),
            }
            for p in produtos
        ]
    except (ValueError, KeyError, TypeError) as erro:
        # Resposta veio, mas não é o JSON esperado (campo faltando, formato errado).
        log.error("Resposta inesperada de %s: %r", url, erro)
        raise ErroPermanente(f"resposta inesperada de {url}: {erro!r}") from erro

    # Lista vazia não é erro de rede, mas calcular métricas sobre nada
    # gravaria um dia "sem produtos" como se fosse verdade.
    if not enxuto:
        raise ErroPermanente(f"{url} devolveu 0 produtos")

    log.info("%d produtos recebidos", len(enxuto))
    return enxuto
