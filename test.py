import re
from tools.data_tool import GetLabelDate

GetLabelDate('zyyx', 'rpt_earnings_adjust', ['stockcode', 'ann_date', 'title']).get_price([-1, 0, 1])
