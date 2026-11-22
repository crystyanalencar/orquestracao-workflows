from ecommerce import calculo

PRODUTOS = [
    {"id": 1, "category": "men's clothing", "price": 109.95},
    {"id": 2, "category": "men's clothing", "price": 22.3},
    {"id": 3, "category": "electronics", "price": 64.0},
    {"id": 4, "category": "men's clothing", "price": 55.99},
    {"id": 5, "category": "men's clothing", "price": 15.99},
]


def test_separar_agrupa_e_ordena_por_categoria():
    pares = calculo.separar_por_categoria(PRODUTOS)
    assert [p["categoria"] for p in pares] == ["electronics", "men's clothing"]
    assert pares[1]["precos"] == [109.95, 22.3, 55.99, 15.99]


def test_separar_nova_categoria_vira_novo_par():
    # "escalar sozinho": categoria nova não exige mudar código
    pares = calculo.separar_por_categoria(PRODUTOS + [{"id": 9, "category": "toys", "price": 1.0}])
    assert len(pares) == 3


def test_metricas_conferidas_a_mao():
    # mesmos números da evidência da camada 5.4
    m = calculo.calcular_metricas("men's clothing", [109.95, 22.3, 55.99, 15.99])
    assert m == {
        "categoria": "men's clothing",
        "qtd": 4,
        "media": 51.06,
        "minimo": 15.99,
        "maximo": 109.95,
    }


def test_consolidar_poe_a_data_e_ordena():
    metricas = [
        calculo.calcular_metricas("b", [1.0]),
        calculo.calcular_metricas("a", [2.0]),
    ]
    linhas = calculo.consolidar(metricas, "2026-11-22")
    assert [linha["categoria"] for linha in linhas] == ["a", "b"]
    assert all(linha["data_coleta"] == "2026-11-22" for linha in linhas)
