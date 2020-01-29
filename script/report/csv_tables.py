"""Preparing csv tables"""


from script.etl.database import (
    conn_bd_oemz, load_inputs_all, load_nomenclature
)
from script.etl.data import (
    load_req_plus_inputs, reshape_summary_table
)


def prepare_csv():
    """Preparing csv tables"""

    CONN = conn_bd_oemz()
    INPUTS_ALL = load_inputs_all(CONN)
    N_DAYS = 7
    NOM = load_nomenclature(CONN)
    DONE_TABLE = load_req_plus_inputs(N_DAYS, INPUTS_ALL, NOM)
    DETAIL_TABLE, SUMMARY_TABLE = reshape_summary_table(DONE_TABLE, N_DAYS, NOM)

    DETAIL_TABLE.to_csv(r"..\tables\detail.csv", sep=';', encoding='ansi')
    SUMMARY_TABLE.to_csv(r"..\tables\summary.csv", sep=';', encoding='ansi')

