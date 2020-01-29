"""Run script"""

from script.report.excel_tables import build_report
from script.report.csv_tables import prepare_csv


if __name__ == '__main__':
    prepare_csv()
    build_report()
