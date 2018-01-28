"""
This module is for transforming tushare data to
data frame
Yu Mu
Jan. 25th, 2018
"""
import pandas as pd
import glob
import data_utils


input_file_address = '/Users/yumu/Desktop/Internship/predictor_data/tushare_data_same_start_with_pred/'
output_file_address = '/Users/yumu/Desktop/Internship/predictor_data/'
all_data = glob.glob(input_file_address+'*')
num_stocks = len(all_data)
cleaned_data = []
column_name = 'close'
for i in range(num_stocks):
    print i
    price = data_utils.read_single_column_tushare(all_data[i], column_name)
    cleaned_data.append(price)
pd.concat(cleaned_data, axis=1).to_csv(output_file_address+'tushare_all_price_frame.csv')
print "done"
