"""Connections to DB"""

import pandas as pd
import sqlite3 as sql


def conn_bd_oemz():
    """Подключение к базе sqlite"""

    conn_ = sql.connect(
        r'W:\Analytics\Database\bd_oemz.bd3',
        detect_types=sql.PARSE_COLNAMES | sql.PARSE_DECLTYPES
    )
    return conn_


def load_inputs_all(conn_):
    """Загрузка поступлений из базы bd_oemz.bd3.
    Только столбцы (дата, код_номенклатуры, кол-во).
    Выходные данные в DataFrame.

    :arg conn_ - объект подключения к базе (sqlite3.connection)"""

    cur1 = conn_.cursor()
    cur1.execute("""SELECT date, nom_code, amount FROM inputs""")
    inputs1 = pd.DataFrame(
        cur1.fetchall(),
        columns=['date', 'nom_code', 'amount']
    )
    return inputs1


def load_nomenclature(conn_):
    """Загрузка справочника по номенклатуре из базы bd_oemz.bd3. 
    Все столбцы.

    :arg conn_ - объект подключения к базе (sqlite3.connection)"""

    cur1 = conn_.cursor()
    cur1.execute("""SELECT * FROM nomenclature""")
    nom1 = pd.DataFrame(
        cur1.fetchall(),
        columns=['code', 'name', 'unite', 'indicator']
    )
    return nom1
