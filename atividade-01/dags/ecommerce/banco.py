"""SQL e gravação: snapshot idempotente (metricas_categoria) e histórico em
append (metricas_categoria_historico), na mesma transação."""

# Chave primária (data_coleta, categoria): uma linha por categoria por dia. É
# ela que torna a gravação idempotente, junto com o ON CONFLICT abaixo.
SQL_CRIAR_TABELA = """
CREATE TABLE IF NOT EXISTS metricas_categoria (
    data_coleta   DATE          NOT NULL,
    categoria     TEXT          NOT NULL,
    qtd_produtos  INTEGER       NOT NULL,
    preco_medio   NUMERIC(10,2) NOT NULL,
    preco_minimo  NUMERIC(10,2) NOT NULL,
    preco_maximo  NUMERIC(10,2) NOT NULL,
    atualizado_em TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (data_coleta, categoria)
)
"""

# Upsert: se a linha do dia já existe, sobrescreve em vez de duplicar.
# Valores sempre por parâmetro (%(nome)s), nunca concatenados na string:
# "men's clothing" tem apóstrofo, e concatenar abriria espaço para SQL injection.
SQL_UPSERT = """
INSERT INTO metricas_categoria
    (data_coleta, categoria, qtd_produtos, preco_medio, preco_minimo, preco_maximo)
VALUES
    (%(data_coleta)s, %(categoria)s, %(qtd)s, %(media)s, %(minimo)s, %(maximo)s)
ON CONFLICT (data_coleta, categoria) DO UPDATE SET
    qtd_produtos  = EXCLUDED.qtd_produtos,
    preco_medio   = EXCLUDED.preco_medio,
    preco_minimo  = EXCLUDED.preco_minimo,
    preco_maximo  = EXCLUDED.preco_maximo,
    atualizado_em = now()
"""

# Reprocessar um dia em que uma categoria sumiu da API deixaria a linha antiga
# dela para trás. Apaga as categorias do dia que não vieram neste lote, para o
# dia gravado ser exatamente o retrato da última coleta.
SQL_LIMPAR_SUMIDAS = """
DELETE FROM metricas_categoria
WHERE data_coleta = %(data_coleta)s
  AND NOT (categoria = ANY(%(categorias)s))
"""

SQL_CONTAR_DIA = "SELECT count(*) FROM metricas_categoria WHERE data_coleta = %s"

# Histórico (requisito opcional): só INSERT, nunca atualiza nem apaga. Cada
# gravação acrescenta uma linha por categoria, para acompanhar a evolução dos
# preços, inclusive dentro do mesmo dia. É o oposto do snapshot de propósito: o
# snapshot responde "qual é o número do dia", o histórico "o que foi gravado e
# quando". run_id liga cada linha à execução do Airflow que a gravou.
SQL_CRIAR_HISTORICO = """
CREATE TABLE IF NOT EXISTS metricas_categoria_historico (
    id            BIGSERIAL     PRIMARY KEY,
    executado_em  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    run_id        TEXT          NOT NULL,
    data_coleta   DATE          NOT NULL,
    categoria     TEXT          NOT NULL,
    qtd_produtos  INTEGER       NOT NULL,
    preco_medio   NUMERIC(10,2) NOT NULL,
    preco_minimo  NUMERIC(10,2) NOT NULL,
    preco_maximo  NUMERIC(10,2) NOT NULL
)
"""

SQL_INSERIR_HISTORICO = """
INSERT INTO metricas_categoria_historico
    (run_id, data_coleta, categoria, qtd_produtos, preco_medio, preco_minimo, preco_maximo)
VALUES
    (%(run_id)s, %(data_coleta)s, %(categoria)s, %(qtd)s, %(media)s, %(minimo)s, %(maximo)s)
"""


def gravar_metricas(conn, linhas, run_id):
    """Grava as linhas de um dia numa transação só e fecha a conexão.

    Snapshot (upsert + limpeza das sumidas) e histórico (append) entram juntos:
    nunca existe histórico de uma gravação que o snapshot não recebeu, nem o
    contrário.

    conn: conexão DB-API (psycopg2), como a de PostgresHook.get_conn().
    Devolve (categorias sumidas removidas, total de linhas do dia na tabela).
    """
    data_coleta = linhas[0]["data_coleta"]
    categorias = [linha["categoria"] for linha in linhas]
    try:
        # Ou o dia inteiro é gravado, ou nada muda. O "with conn" faz commit no
        # fim e rollback se algo levantar erro.
        with conn, conn.cursor() as cur:
            cur.execute(SQL_CRIAR_TABELA)
            cur.executemany(SQL_UPSERT, linhas)
            cur.execute(
                SQL_LIMPAR_SUMIDAS,
                {"data_coleta": data_coleta, "categorias": categorias},
            )
            removidas = cur.rowcount
            cur.execute(SQL_CONTAR_DIA, (data_coleta,))
            total_dia = cur.fetchone()[0]

            cur.execute(SQL_CRIAR_HISTORICO)
            cur.executemany(
                SQL_INSERIR_HISTORICO,
                [{"run_id": run_id, **linha} for linha in linhas],
            )
    finally:
        conn.close()
    return removidas, total_dia
