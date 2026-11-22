"""Regras de validação dos produtos. Levantam ValueError/TypeError."""
import math

# Maior valor que cabe na coluna NUMERIC(10,2) da tabela de destino.
PRECO_MAXIMO = 99_999_999.99


def preco(valor):
    """Converte o preço para float, rejeitando o que estragaria as métricas.

    float() aceita "NaN", "inf", True e negativos; qualquer um deles estragaria
    média, mínimo e máximo sem erro nenhum. Acima do limite da coluna, o erro só
    apareceria lá no fim, na gravação.
    """
    if isinstance(valor, bool):
        raise TypeError(f"preço booleano: {valor!r}")
    convertido = float(valor)
    if not math.isfinite(convertido) or not 0 <= convertido <= PRECO_MAXIMO:
        raise ValueError(f"preço inválido: {valor!r}")
    return convertido


def categoria(valor):
    """Exige texto não vazio.

    Categoria nula ou vazia quebraria o agrupamento (sorted, join) numa task sem
    retry nem callback; validada aqui, vira erro de formato com alerta.
    """
    if not isinstance(valor, str) or not valor.strip():
        raise ValueError(f"categoria inválida: {valor!r}")
    return valor
