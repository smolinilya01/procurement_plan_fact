"""Common functions"""

import datetime as dt
import re
import sys


def start_date(path_):
    """Рассчет стартовой даты - дата расчета файла потребности, которая будет
    использоваться как начальная, для отсчета 7-дневного периода.
    :arg path_ - путь к файлу"""

    st_date = re.search(r'ask_(\d{8}).csv', path_)
    st_date = st_date.group(1)
    st_date = date_par(st_date)
    return st_date


def date_par(x):
    """Парсер даты формата 20190101
    :arg х - значение типа '20190101'"""

    return dt.datetime(int(x[0:4]), int(x[4:6]), int(x[6:]))


def check_none_sotnam(table_):
    """Checking existing None sortament in table
    :arg table_ - pandas DataFrame"""

    if len(table_.sortam[table_.sortam.isna()]) > 0:
        print("It's necessary to download next nomenclature in DB:")
        for i in table_[table_.sortam.isna()]['nom_code'].values:
            print('Code nomenclature', i)
        sys.exit()
