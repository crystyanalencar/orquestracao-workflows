"""Agrupamento por categoria e métricas. Funções puras, sem Airflow."""


def separar_por_categoria(produtos):
    """[{id, category, price}] -> [{categoria, precos}], em ordem alfabética.

    Cada par vira uma instância mapeada e já leva os preços junto: nenhuma
    instância precisa chamar a API de novo.
    """
    precos_por_categoria = {}
    for p in produtos:
        precos_por_categoria.setdefault(p["category"], []).append(p["price"])
    return [
        {"categoria": categoria, "precos": precos}
        for categoria, precos in sorted(precos_por_categoria.items())
    ]


def calcular_metricas(categoria, precos):
    """Quantidade, média (2 casas), mínimo e máximo de uma categoria."""
    return {
        "categoria": categoria,
        "qtd": len(precos),
        "media": round(sum(precos) / len(precos), 2),
        "minimo": min(precos),
        "maximo": max(precos),
    }


def consolidar(metricas, data_coleta):
    """Junta as métricas de todas as categorias, com a data, em ordem de categoria."""
    linhas = [{"data_coleta": data_coleta, **m} for m in metricas]
    linhas.sort(key=lambda linha: linha["categoria"])
    return linhas
