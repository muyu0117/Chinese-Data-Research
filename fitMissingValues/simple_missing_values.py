"""
This module defines several simple methods handling missing values
Yu Mu
Jan. 3rd, 2017
"""
import numpy as np
import missingno as msno


class MissingValues(object):
    """Handling missing values"""
    def __init__(self, data, num_missed):
        """
        Parameters
        ----------
        data: all past returns data frame
        num_missed: number of missing values allowed crossectionally
        """
        self._data = data
        self._num_missed = num_missed

    def get_longest_non_missing_data(self):
        """
        This function simply get longest dataframe
        that doesnt' have too much missing values
        crossectionally
        """
        tmp = ~(self._data.isnull().sum(axis=0)==self._data.shape[0])
        # remove those stocks have all nan values, the reason is although we pick up daily universe correctly, for some data sources, adjusted close price is not available yet.
        not_all_missed = tmp.index[tmp]
        cleaned_data = self._data.loc[:, not_all_missed]
        # calculate number of missing values crossectionally
        cross_missed = cleaned_data.isnull().sum(axis=1)
        # only use those days crossectional missing values less than num_missed
        good_days = cross_missed[cross_missed < self._num_missed]
        return cleaned_data.loc[good_days.index[0]:, :]        # back to first day that satisfy this criterion

    def missing_values_visualizations(self, data, address, name):
        fig = msno.matrix(data, inline=False)
        fig.savefig(address+name+'.png')

    def find_appropriate_length(self, data):
        """
        find appropriate length of data has only 15% or less missing values
        """
        total_length = data.shape[0]
        for i in range(total_length):
            trunc_data = data[i:, :]
            total_missing_values = np.isnan(trunc_data).sum(axis=0).sum()
            missing_percentage = float(total_missing_values)/(trunc_data.shape[0]*trunc_data.shape[1])
            print "missing percentage is %f" % missing_percentage
            if missing_percentage <= 0.15:
                print "it's good length"
                break
        return trunc_data

    def impute_one_dim_data_in_history(self, one_dim_data):
        first_day_trade = np.where(~np.isnan(one_dim_data))[0][0]
        in_history_data = one_dim_data[first_day_trade:]
        in_history_data[np.isnan(in_history_data)] = np.nanmedian(in_history_data)
        one_dim_data[first_day_trade:] = in_history_data
        return one_dim_data

    def simple_fill_missing_values_in_history(self, data):
        """
        fill missing values within stocks' history
        using simple methods given by python
        """
        imputed_data = map(self.impute_one_dim_data_in_history, data)
        return np.array(imputed_data)

    def get_non_missing_trunc_data(self, data):
        first_non_missing_day = np.where(np.isnan(data).sum(axis=1))[0][-1]
        return data[first_non_missing_day+1:, :]

    def fit_missing_values_EMFP(self):
        data = self._data.values
        imputed_data_T = self.simple_fill_missing_values_in_history(data.T)
        trunc_data = self.get_non_missing_trunc_data(imputed_data_T.T)
        return trunc_data
