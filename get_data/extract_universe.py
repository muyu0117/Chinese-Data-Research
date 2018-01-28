"""
This module is for extract stock universe
Yu Mu
Dec. 26th, 2017
"""

import pandas as pd
import glob
import data_utils
import matplotlib.pyplot as plt


# input_file = '/Users/yumu/Desktop/Internship/predictor_data/all_dollar_volumes.csv'
input_file = '/Users/yumu/Desktop/Internship/predictor_data/all_dollar_volumes_tushare.csv'
output_file = '/Users/yumu/Desktop/Internship/predictor_data/'
output_results = '/Users/yumu/Desktop/Internship/results/'
all_dollar_volumes = pd.read_csv(input_file, index_col=0)
all_dates = all_dollar_volumes.index.tolist()
start_date = '2000-01-04'
end_date = '2017-12-11'
numDays = all_dates.index(end_date) - all_dates.index(start_date) + 1
window_size = 120
threshold = 90
every_day_universe = [[]]*numDays
for i in range(numDays):
    print i
    daily_dvolume = all_dollar_volumes.iloc[all_dates.index(start_date)+i, :]
    first_day = data_utils.find_first_day_of_month(daily_dvolume.name, all_dates)
    every_day_universe[i] = data_utils.extract_universe(first_day, window_size, all_dollar_volumes, all_dates, threshold)
every_day_universe_df = pd.DataFrame(every_day_universe, index=all_dates[all_dates.index(start_date):all_dates.index(end_date)+1])
every_day_universe_df.to_csv(output_file+'tushare_thresh90_universe_by_dollar_volume.csv')
# plot number of stocks we need to trade
x = pd.to_datetime(every_day_universe_df.index.tolist())
y = every_day_universe_df.notnull().sum(axis=1).values
plt.plot(x, y, 'b')
plt.title('number of stocks in trading universe')
plt.savefig(output_results+'tushare_thresh90_num_stocks.png')
print "done"
