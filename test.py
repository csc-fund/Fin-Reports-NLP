import re
from tools.data_tool import GetLabelData

GetLabelData('zyyx', 'rpt_earnings_adjust', ['stockcode', 'ann_date', 'title']).get_price([-1, 0, 1])
