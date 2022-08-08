from tools.data_tool import GetPriceData, GetLabelDate
import pandas as pd

if __name__ == '__main__':
    # GetPriceData(data_base='tushare_daily').down_all_kline('zyyx', 'rpt_e_a', 'stockcode')
    label = GetLabelDate('zyyx', 'rpt_e_a', ['stockcode', 'ann_date', 'title', 'report_type'], [-1, 0, 1, 5])
    label.get_csv()
