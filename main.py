from tools.data_tool import GetPriceData, GetLabelData
import pandas as pd

if __name__ == '__main__':
    # GetPriceData(data_base='tushare_daily').down_all_kline('zyyx', 'rpt_e_a', 'stockcode')
    label = GetLabelData('zyyx', 'rpt_e_a', ['stockcode', 'ann_date', 'title', 'report_type',
                                             'report_year', 'organ_name', 'author'], [-1, 0, 1, 5])
    # label.get_tag( )
    label.get_csv()

