from tools.data_tool import GetPriceData

if __name__ == '__main__':
    GetPriceData(data_base='tushare_daily').down_all_kline('zyyx', 'rpt_e_a', 'stockcode')
