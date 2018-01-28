"""
download data using tushare
Yu Mu
Dec. 27th, 2017
"""


import tushare as ts
import glob
import pandas as pd


def get_start_date(start_int):
    """
    transform int to str
    """
    date_str = str(start_int)
    return date_str[0:4]+'-'+date_str[4:6]+'-'+date_str[6:]


input_file_address = '/Users/yumu/Desktop/Internship/predictor_data/renamed_data/'
output_file_address = '/Users/yumu/Desktop/Internship/predictor_data/tushare_data_same_start_with_pred/'
hs_stocks_data = glob.glob(input_file_address+'sh*') + glob.glob(input_file_address+'sz*')
stock_codes = [s[-10:-4] for s in hs_stocks_data]
numStocks = len(stock_codes)
for i in range(numStocks):
    print i
    if stock_codes[i][0] == '6':
        pred_df = pd.read_csv(input_file_address+'sh'+stock_codes[i]+'.csv', index_col='dates')
    else:
        pred_df = pd.read_csv(input_file_address+'sz'+stock_codes[i]+'.csv', index_col='dates')
    start_date = str(pred_df.index[-1])
    end_date = str(pred_df.index[0])
    df = ts.get_k_data(stock_codes[i], start=start_date, end=end_date)          # it's qfq data
    df.to_csv(output_file_address+stock_codes[i]+'.csv')

print "done"
