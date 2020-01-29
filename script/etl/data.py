"""Reorganizing data"""

import pandas as pd
import glob
import datetime as dt

from script.common.func import (check_none_sotnam, start_date)
from script.etl.database import (
    conn_bd_oemz, load_inputs_all, load_nomenclature
)


def prepare_csv():
    """Prepare csv tables"""

    CONN = conn_bd_oemz()
    INPUTS_ALL = load_inputs_all(CONN)
    N_DAYS = 7
    NOM = load_nomenclature(CONN)
    DONE_TABLE = load_req_plus_inputs(N_DAYS, INPUTS_ALL, NOM)
    DETAIL_TABLE, SUMMARY_TABLE = reshape_summary_table(DONE_TABLE, N_DAYS, NOM)

    DETAIL_TABLE.to_csv(r".\tables\detail.csv", sep=';', encoding='ansi')
    SUMMARY_TABLE.to_csv(r".\tables\summary.csv", sep=';', encoding='ansi')


def reshape_inputs(inp1, nom1, date1, n_days1):
    """Построение таблицы с поступлениями:
        - только металл
        - по заданному интервалу дат
        - итоговая гру
    :arg inp1 - поступления
    :arg nom1 - словарь номенклатуры из bd_oemz.bd3
    :arg date1 - нижняя дата интервала
    :arg n_days1 - для расчета верхней границы интервала (date1 + n_days1)"""

    inp1 = inp1[
        (inp1.date >= date1) &
        (inp1.date <= date1 + dt.timedelta(days=n_days1))
    ]
    inp1 = inp1.merge(
        nom1[['code', 'indicator']],
        how='left',
        left_on='nom_code',
        right_on='code'
    )
    inp1 = inp1[inp1.indicator == 'Металлопрокат'].drop(columns=['indicator', 'code'])
    return inp1


def load_req_plus_inputs(n_days1, inputs_, nom_):
    """Загрузка потребностей из файлов типа ask_date.csv
    (то есть на каждую дату создания файла),
    мерж к них поступлений через код номекнлатуры.
    На выходе таблица потребностей и поступлений и дат, где даты создания файлов.
    Выход в формате DataFrame
    p.s. Inputs загружаются один раз из базы, так как занимают мало памяти.
    :arg n_days1 - для расчета верхней границы интервала (strt_date + n_days1)
    :arg inputs_ - table with inputs
    :arg nom_ - table with nomenclatures"""

    goal_path = r"W:\Analytics\Илья\Задание 14 Расчет потребности для МТО\data\*.csv"
    output_table1 = pd.DataFrame()
    for i_path in glob.glob(goal_path):  # проход по всех файлам
        """Подготовка поотребностей"""
        path = i_path
        req = pd.read_csv(
            path, sep=';',
            parse_dates=['Дата запуска'],
            dayfirst=True, encoding='ansi',
            usecols=[
                'Дата запуска', 'Код', 'Заказ обеспечен', 'Пометка удаления',
                'Списание из Поступлений', 'Остаток дефицита'
            ]
        )
        req = req[
            (req['Заказ обеспечен'] == 0) &
            (req['Пометка удаления'] == 0)
        ]  # оставляем строки без индикации обеспеченности и удаления
        req['all_req'] = (
                req['Остаток дефицита'] + req['Списание из Поступлений']
        )  # складываем остаток дефицита и поступления
        req = req[['Дата запуска', 'Код', 'all_req']]
        req = req.rename(
            columns={'Дата запуска': 'date', 'Код': 'nom_code', 'all_req': 'req'}
        )
        req = req[req['req'] != 0]  # остаются строки != 0

        """Выборка по нужным датам и мерж поступлений с потребностями"""
        strt_date = start_date(path)
        need_inputs = reshape_inputs(inputs_, nom_, strt_date, n_days1)  # need_inputs - это поступления за нужный промежуток времени
        req = req[(req.date <= strt_date + dt.timedelta(days=n_days1))]  # выбираем потребность для нужного промежутка времени
        need_inputs = need_inputs[
            ['nom_code', 'amount']
        ].groupby(by=['nom_code']).sum().reset_index()
        req = req[['nom_code', 'req']].groupby(by=['nom_code']).sum().reset_index()
        req = req.merge(
            need_inputs, how='outer', on='nom_code'
        ).replace({None: 0})  # тут получаем сравнение потребности и поступлений
        req = req.rename(columns={'amount': 'inputs'})
        req['date_start'] = strt_date
        output_table1 = pd.concat([output_table1, req])
    return output_table1


def reshape_summary_table(table1, n_days1, nom_):
    """Преборазовывает таблицу потребностей и поступлений из load_req_plus_inputs():
        - мерж с сортаментом
        - группировка по дате и сортаменту
        - расчет in_plan, out_plan, out_nom
    На выходе 2 таблрицы table1, summary_table:
        - table1 = таблица по сортаментам
        - summary_table = table1, схлопнутая по датам
    :arg table1 - таблица из load_req_plus_inputs(n_days1)
    :arg n_days1 - для расчета верхней границы интервала (strt_date + n_days1)
    :arg nom_ - table with nomenclatures"""

    nom_sortam = pd.read_excel(
        r"W:\Analytics\Илья\Задание 14 Расчет потребности для МТО\dicts\dict_nom.xlsx",
        usecols=['Номенклатура', 'Сортамент']
    ).drop_duplicates()
    code_sortam = nom_.merge(nom_sortam, how='left', left_on='name', right_on='Номенклатура')
    code_sortam = code_sortam[['code', 'Сортамент']].rename(
        columns={'Сортамент': 'sortam', 'code': 'nom_code'}
    )
    table1 = table1.merge(code_sortam, how='left', on='nom_code')  # добавляем сортамент

    check_none_sotnam(table1)  # проверка отсутствия None в столбце с сортаментом

    table1 = table1.drop(columns=['nom_code'])
    table1 = table1.groupby(by=['date_start', 'sortam']).sum().reset_index()

    """Рассчеты in_plan, out_plan, out_nom"""
    table1['in_plan'] = table1['inputs'].where(
        table1['req'] > table1['inputs'],
        table1['req']
    )
    table1['out_plan'] = 0
    table1['out_plan'] = table1['out_plan'].where(
        ~((table1['req'] != 0) & (table1['inputs'] > table1['req'])),
        table1['inputs'] - table1['req']
    )
    table1['out_nom'] = 0
    table1['out_nom'] = table1['out_nom'].where(
        ~((table1['req'] == 0) & (table1['inputs'] > table1['req'])),
        table1['inputs'] - table1['req']
    )
    table1['date_end'] = table1.date_start + dt.timedelta(days=n_days1)
    table1 = table1[[
        'date_start', 'date_end', 'sortam', 'req',
        'in_plan', 'out_plan', 'out_nom'
    ]]

    summary_table1 = table1.groupby(by='date_start').sum().reset_index()
    summary_table1['percent'] = summary_table1.in_plan / summary_table1.req * 100
    summary_table1['date_end'] = summary_table1.date_start + dt.timedelta(days=n_days1)
    summary_table1 = summary_table1[[
        'date_start', 'date_end', 'req', 'in_plan',
        'percent', 'out_plan', 'out_nom'
    ]]
    return table1, summary_table1
