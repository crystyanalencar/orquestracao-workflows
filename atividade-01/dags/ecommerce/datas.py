"""Fuso e data da coleta."""
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Sao_Paulo")


def data_coleta(data_interval_end):
    """Data que a execução grava: o fim do intervalo, em Brasília, como texto ISO.

    Em execução agendada é o dia em que ela roda (06:00). Em execução manual, o
    Airflow recua o fim para o último 06:00 antes do disparo: disparar às 05:30
    cai no dia anterior. Convertido antes de tirar a data porque em UTC uma
    execução depois das 21:00 cairia no dia seguinte. Texto porque o XCom é JSON.
    """
    return data_interval_end.astimezone(TZ).date().isoformat()


def hoje():
    """Data de hoje em Brasília, como texto ISO."""
    return datetime.now(TZ).date().isoformat()
