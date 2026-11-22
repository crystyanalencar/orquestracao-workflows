"""Lógica da DAG ecommerce_metricas, separada da montagem da DAG.

- api: busca na FakeStore e separa erro temporário de permanente
- validacao: regras de preço e categoria
- operadores: ValidarProdutosOperator, que aplica as regras à lista inteira
- calculo: agrupamento e métricas por categoria
- banco: SQL e gravação idempotente no Postgres
- callbacks: callbacks de sucesso, retry e falha
- datas: fuso e data da coleta

O que depende do Airflow (Variables, contexto, exceções que controlam retry)
fica na DAG; aqui, só o necessário. O .airflowignore impede o scheduler de
varrer esta pasta procurando DAGs.
"""
