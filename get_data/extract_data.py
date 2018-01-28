"""
This module defines every thing needed to process raw data
Yu Mu
Jan. 8th, 2018
"""
import numpy as np
import pandas as pd
import glob


def list_intersection(a, b):
    return list(set(a) & set(b))


def _read_single_column(address, column_name):
    """
    read single column of one stock, change the column name to ticker name
    Parameters
    ----------
    address: file full address; str
    column_name: str
    Return
    ------
    price: DataFrame
    """
    one_stock_data = pd.read_csv(address, encoding='GBK')          # encoding GBK is for reading chinese in dataframe
    ticker_name = str(one_stock_data['ticker code'].unique()[0])
    column_series = one_stock_data[[column_name]].rename(index=one_stock_data['dates'],
                                                         columns={column_name: ticker_name})         # extract column series from data
    return column_series


def _pick_columns(df, threshold):
    """
    pick columns of dataframe where the number of missing values is less than threshold
    """
    good_df = df.isnull().sum(axis=0)<5
    return df.loc[:, df.columns[good_df]]


def _pick_universe(universe, percentile):
    """
    pick universe from all existing stocks with dollar volume larger than percentile of all dollar volumes
    Parameters
    ----------
    universe: universe of stock without much missing values for past days; dataframe
    percentile: float number
    """
    average_dollar_volumes = np.mean(universe, axis=0)           # numpy mean compute the mean of just the values that are available.
    return average_dollar_volumes[average_dollar_volumes>np.percentile(average_dollar_volumes, percentile)].index


class extract_data(object):

    def __init__(self, data_file_address):
        self._data_file_address = data_file_address
        self._stock_names = glob.glob(self._data_file_address+'sh*') + glob.glob(self._data_file_address+'sz*')
        self._numStocks = len(self._stock_names)

    def get_dollar_volumes_df(self):
        dollar_volumes = []
        for i in range(self._numStocks):
            dollar_volume = _read_single_column(self._stock_names[i], 'dollar volume')
            dollar_volumes.append(dollar_volume)
        self._dollar_volumes_df = pd.concat(dollar_volumes, axis=1)
        return self._dollar_volumes_df

    def extract_universe_by_dollar_volumes(self, dollar_volumes_df, start_date, end_date, window_size):
        """
        extract every day trading universe by past dollar volumes
        Parameters
        ----------
        dollar_volumes_df: dollar volumes data frame
        start_date: start date of trading strategy; str in format 'YYYY-MM-DD'
        end_date: end date of trading strategy; str in format 'YYYY-MM-DD'
        """
        self._all_dates = dollar_volumes_df.index.tolist()
        s, e = self._all_dates.index(start_date), self._all_dates.index(end_date)
        self._numDays = e - s + 1
        universes = []
        for i in range(s, e+1):
            print i
            window_sized_dollar_volumes_df = dollar_volumes_df.loc[self._all_dates[i-window_size]:self._all_dates[i], :]
            daily_universe = _pick_columns(window_sized_dollar_volumes_df, 5)           # pick columns having missing values less than 5
            trimed_universe = _pick_universe(daily_universe.iloc[:-1, :], 50)                        # pick columns having adv larger than 50 percentile; adv should be calculated by previous 90 days
            trimed_universe_df = pd.DataFrame(data=[trimed_universe], index=[self._all_dates[i]])
            universes.append(trimed_universe_df)
        return pd.concat(universes, axis=0)

    def extract_universe_by_adv_and_length(self, dollar_volumes_df, start_date, end_date, window_size):
        """
        extract every day trading universe by adv and length
        Parameters
        ----------
        """
        self._all_dates = dollar_volumes_df.index.tolist()
        s, e = self._all_dates.index(start_date), self._all_dates.index(end_date)
        self._numDays = e - s + 1
        universes = []
        for i in range(s, e+1):
            print i
            window_sized_dollar_volumes_df = dollar_volumes_df.loc[self._all_dates[i-window_size]:self._all_dates[i], :]
            all_past_df = dollar_volumes_df.loc[:self._all_dates[i], :]
            filter_df = all_past_df.isnull().sum(axis=0) < all_past_df.shape[0]/3.0
            long_universe_columns = filter_df.index[filter_df.values]
            daily_universe = _pick_columns(window_sized_dollar_volumes_df, 5)           # pick columns having missing values less than 5
            trimed_universe = _pick_universe(daily_universe.iloc[:-1, :], 50)                        # pick columns having adv larger than 50 percentile; adv should be calculated by previous 90 days
            final_universe = list_intersection(long_universe_columns.tolist(), trimed_universe.tolist())
            trimed_universe_df = pd.DataFrame(data=[final_universe], index=[self._all_dates[i]])
            universes.append(trimed_universe_df)
        return pd.concat(universes, axis=0)


if __name__ == '__main__':
    data_file_address = '/Users/yumu/Desktop/Internship/predictor_data/renamed_data/'
    output_data_file = '/Users/yumu/Desktop/Internship/predictor_data/'
    inst = extract_data(data_file_address)
    import pdb; pdb.set_trace()  # breakpoint 866621af //
    ######################
    # get dollar volumes #
    ######################
    # dollar_volumes_df = inst.get_dollar_volumes_df()
    # dollar_volumes_df.to_csv(output_data_file+'dollar_volumes.csv')
    input_file = '/Users/yumu/Desktop/Internship/predictor_data/dollar_volumes.csv'
    dollar_volumes_df = pd.read_csv(input_file, index_col=0)
    ################
    # get universe #
    ################
    start_date = '2005-01-04'
    end_date = '2017-12-11'
    window_size = 90                   # window size for picking trading universe
    # extracted_universe = inst.extract_universe_by_dollar_volumes(dollar_volumes_df, start_date, end_date, window_size)
    extracted_universe = inst.extract_universe_by_adv_and_length(dollar_volumes_df, start_date, end_date, window_size)
    extracted_universe.to_csv(output_data_file+'extract_universe_50_percentile_adv_long_length.csv')
    import pdb; pdb.set_trace()  # breakpoint cf24dec9 //
    print "done"

