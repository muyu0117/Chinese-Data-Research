"""
Handle raw data from different data vendor and produce
data that are used in trading strategy

Jan. 16th, 2018
Yu Mu
"""
import pandas as pd
import numpy as np
import data_utils


def _list_intersection(a, b):
    return list(set(a) & set(b))


class DataHandling(object):
    """
    base class of Data Handling
    """
    def __init__(self):
        pass

    def extract_data(self):
        """
        extract data from draw data format
        """
        pass

    def generate_trading_universe(self):
        """
        generate trading universe from data
        """
        pass

    def run_everything(self):
        """
        run everything to produce data used in building strategies
        """
        pass


class WindDataHandling(DataHandling):
    """
    Data Handling module for Wind data
    """
    def __init__(self, price_address, dv_address, start_date, end_date, threshold, window_size):
        """
        Parameters
        ----------
        start_date: start date of the output data
        end_date: end date of the output data
        threshold: threshold for determining whether the stock should be included in the universe

        Returns
        -------
        return price data and universe data for building stats arb strategy
        """
        self._price_address = price_address
        self._dv_address = dv_address
        self._start_date = start_date
        self._end_date = end_date
        self._threshold = threshold
        self._window_size = window_size

    def extract_data(self):
        price_df = pd.read_csv(self._price_address, index_col=0, header=3)
        dv_df = pd.read_csv(self._dv_address, index_col=0, header=3)
        return price_df, dv_df

    def get_current_tradable_assets(self, dvolumes):
        tmp = dvolumes[~dvolumes.isnull()]
        return tmp[tmp>1.0].index.tolist()

    def generate_trading_universe(self, dv_df):
        """
        generate trading universe based on percentile threshold
        of daily volumn
        """
        all_dates = dv_df.index.tolist()
        numDays = all_dates.index(self._end_date) - all_dates.index(self._start_date) + 1
        every_day_universe = [[]]*numDays
        for i in range(numDays):
            daily_dvolume = dv_df.iloc[all_dates.index(self._start_date)+i, :]
            first_day = data_utils.find_first_day_of_month(daily_dvolume.name, all_dates)
            every_day_universe_tmp = data_utils.extract_universe(first_day, self._window_size, dv_df, all_dates, self._threshold)
            tradable_assets = self.get_current_tradable_assets(daily_dvolume)                     # some stock might be suspended, we don't trade those stocks
            every_day_universe[i] = _list_intersection(every_day_universe_tmp, tradable_assets)
        every_day_universe_df = pd.DataFrame(every_day_universe,
                                             index=all_dates[all_dates.index(self._start_date):all_dates.index(self._end_date)+1])
        return every_day_universe_df

    def run_everything(self):
        price_df, dv_df = self.extract_data()
        every_day_universe_df = self.generate_trading_universe(dv_df)
        return price_df, every_day_universe_df


if __name__ == '__main__':
    price_address = '/Users/yumu/Desktop/Internship/predictor_data/wind data/All A listed Stocks Values 01052018.csv'
    dv_address = '/Users/yumu/Desktop/Internship/predictor_data/wind data/All A listed stocks dollar volum 01052018.csv'
    price_output = '/Users/yumu/Desktop/Internship/predictor_data/wind data/'
    dv_output = '/Users/yumu/Desktop/Internship/predictor_data/wind data/'
    start_date = '2014-08-01'
    end_date = '2018-01-05'
    window_size = 90
    threshold = 80
    inst = WindDataHandling(price_address, dv_address, start_date, end_date, threshold, window_size)
    import pdb; pdb.set_trace()  # breakpoint 229e0c75 //
    price_df, every_day_universe_df = inst.run_everything()
    price_df.to_csv(price_output+'all_price.csv')
    every_day_universe_df.to_csv(dv_output+'everyday_universe.csv')
    import pdb; pdb.set_trace()  # breakpoint 2ce7d173 //
    print "done"
