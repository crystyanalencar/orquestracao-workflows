"""Atividade 01 - métricas de produtos por categoria (FakeStore API).

Camada 5.2: esqueleto da DAG + task buscar_produtos, que traz os produtos da API
e devolve só o que o resto do pipeline usa (id, categoria, preço).
"""
import pendulum
import requests
from airflow.decorators import dag, task

TZ = pendulum.timezone("America/Sao_Paulo")
URL_PRODUTOS = "https://fakestoreapi.com/products"


@dag(
    dag_id="ecommerce_metricas",
    schedule="0 6 * * *",  # todo dia às 06:00, no fuso do start_date
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,
    tags=["atividade-01"],
)
def ecommerce_metricas():
    @task
    def buscar_produtos():
        # timeout: sem ele, uma API travada prende o worker para sempre.
        resposta = requests.get(URL_PRODUTOS, timeout=30)
        # 404/500 não viram exceção sozinhos; isto faz a task falhar de verdade.
        resposta.raise_for_status()
        produtos = resposta.json()

        # XCom guarda no banco de metadados: levar só os 3 campos usados,
        # não título, descrição e imagem. price vem int ou float na API.
        enxuto = [
            {"id": p["id"], "category": p["category"], "price": float(p["price"])}
            for p in produtos
        ]
        print(f"{len(enxuto)} produtos recebidos")
        return enxuto

    buscar_produtos()


ecommerce_metricas()
