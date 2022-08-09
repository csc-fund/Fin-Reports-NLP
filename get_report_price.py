from tools.data_tool import GetPriceData, GetLabelData
import pandas as pd

if __name__ == '__main__':
    # GetPriceData(data_base='tushare_daily').down_all_kline('zyyx', 'rpt_e_a', 'stockcode')
    GetLabelData('zyyx', 'rpt_e_a', ['stockcode', 'ann_date', 'title'], [-1, 0, 1, 5]).get_tag()
