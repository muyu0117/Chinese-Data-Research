"""
Getting dollar volume
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
dollar_volumes = []
column_name = 'volume'
for i in range(num_stocks):
    print i
    dollar_volume = data_utils.read_single_column_tushare(all_data[i], column_name)
    dollar_volumes.append(dollar_volume)
# output data
pd.concat(dollar_volumes, axis=1).to_csv(output_file_address+'all_dollar_volumes_tushare.csv')
print "done"
