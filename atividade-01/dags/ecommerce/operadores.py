"""Operador customizado: ValidarProdutosOperator (requisito opcional).

Um operador é a forma de empacotar lógica reutilizável em várias DAGs: qualquer
DAG que receba produtos (de outra API, de um arquivo) pode validá-los com ele,
sem copiar código. As regras de cada campo moram em validacao.py; o operador
aplica as regras à lista inteira e decide se a execução segue.
"""
import logging

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator

from ecommerce import validacao

log = logging.getLogger(__name__)


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema dos produtos antes do processamento.

    Recebe a lista bruta [{id, category, price}] (normalmente o XCom de
    buscar_produtos) e devolve a lista validada, com price em float. Confere:
    lista não vazia, cada item é um objeto, id inteiro e sem repetição,
    categoria texto não vazio, preço finito entre 0 e o limite da coluna.

    Junta todos os erros antes de falhar, em vez de parar no primeiro: quem for
    investigar vê de uma vez quantos produtos vieram ruins e por quê. Falha com
    AirflowFailException: dado inválido não melhora com retry.

    :param produtos: lista de produtos ou XComArg que resolve para ela.
    :param max_erros_no_log: quantos erros listar na mensagem de falha.
    """

    # Campo templated: é o que faz o Airflow resolver um XComArg (a saída de
    # outra task) para o valor real antes do execute.
    template_fields = ("produtos",)
    ui_color = "#fff3cd"

    def __init__(self, *, produtos, max_erros_no_log=5, **kwargs):
        super().__init__(**kwargs)
        self.produtos = produtos
        self.max_erros_no_log = max_erros_no_log

    def execute(self, context):
        produtos = self.produtos
        if not isinstance(produtos, list) or not produtos:
            raise AirflowFailException(
                f"esperava uma lista não vazia de produtos, veio {type(produtos).__name__}"
            )

        validos, erros, ids_vistos = [], [], set()
        for posicao, produto in enumerate(produtos):
            try:
                validos.append(self._validar(produto, ids_vistos))
            except (ValueError, TypeError) as erro:
                erros.append(f"produto na posição {posicao}: {erro}")

        if erros:
            amostra = "; ".join(erros[: self.max_erros_no_log])
            resto = len(erros) - self.max_erros_no_log
            if resto > 0:
                amostra += f"; e mais {resto}"
            raise AirflowFailException(
                f"{len(erros)} de {len(produtos)} produtos inválidos: {amostra}"
            )

        log.info("%d produtos válidos", len(validos))
        return validos

    @staticmethod
    def _validar(produto, ids_vistos):
        if not isinstance(produto, dict):
            raise TypeError(f"não é um objeto: {produto!r}")
        id_ = produto.get("id")
        # bool é subclasse de int em Python: True passaria como id 1.
        if not isinstance(id_, int) or isinstance(id_, bool):
            raise TypeError(f"id inválido: {id_!r}")
        if id_ in ids_vistos:
            raise ValueError(f"id repetido: {id_}")
        ids_vistos.add(id_)
        return {
            "id": id_,
            "category": validacao.categoria(produto.get("category")),
            "price": validacao.preco(produto.get("price")),
        }
