"""Building excel report"""

import os
import win32com.client


def build_report():
    """Building excel report with excel macros"""

    path = os.path.abspath(r"..\tables\_Статистика_выполнения_планов_закупа.xlsm")

    if os.path.exists(path):
        excel_macro = win32com.client.DispatchEx("Excel.Application")  # DispatchEx is required in the newest versions of Python.
        excel_path = os.path.expanduser(path)
        workbook = excel_macro.Workbooks.Open(Filename=excel_path, ReadOnly=1)
        excel_macro.Application.Run(
            "_Статистика_выполнения_планов_закупа.xlsm!Module1.load_data"
        )
        workbook.Save()
        excel_macro.Application.Quit()
        del excel_macro
