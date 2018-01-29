"""
tushare data handling
Yu Mu
Jan. 29th, 2018
"""
import glob
import pandas as pd
import tushare as ts
import data_utils


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


class TushareDataHandling(DataHandling):
    """
    Data Handling module for Tushare data
    """
    def __init__(self, predictor_data_address, output_data_address):
        """
        Parameters
        ----------
        Returns
        -------
        return price data and universe data for building stats arb strategy
        """
        self._pd_address = glob.glob(predictor_data_address+'sh*') + glob.glob(predictor_data_address+'sz*')
        self._ticker_list = self.get_ticker_list()
        self._numStocks = len(self._ticker_list)
        self._output_data_address = output_data_address

    def get_ticker_list(self):
        """
        geting ticker list from predictor data
        """
        return [s[-10:-4] for s in self._pd_address]

    def extract_data(self):
        """
        extract tushare data
        """
        for i in range(self._numStocks):
            # making sure starting date is the same as predictor data, since tushare is
            # not setting default starting values correctly
            pred_df = pd.read_csv(self._pd_address[i], index_col='dates')
            start_date = str(pred_df.index[-1])
            assert(self._ticker_list[i] == self._pd_address[i][-10:-4])
            print "downloading stock %d" % int(i)
            df = ts.get_k_data(self._ticker_list[i], start=start_date, retry_count=10)             # setting retry_count more than 3 to prevent timedout error
            df.to_csv(self._output_data_address+self._ticker_list[i]+'.csv')

    def generate_price_frame_and_volume_frame(self):
        price_frame = []
        volume_frame = []
        all_individual_data = glob.glob(self._output_data_address+'*')
        for i in range(self._numStocks):
            print "processing %dth stock:" % int(i)
            price = data_utils.read_single_column_tushare(all_individual_data[i], 'close')
            price_frame.append(price)
            volume = data_utils.read_single_column_tushare(all_individual_data[i], 'volume')
            volume_frame.append(volume)
        pd.concat(price_frame, axis=1).to_csv(self._output_data_address+'price_frame.csv')
        pd.concat(volume_frame, axis=1).to_csv(self._output_data_address+'volume_frame.csv')

    def generate_trading_universe(self, window_size1, window_size2, threshold, start_date, end_date):
        """
        generate trading universe based only on volume
        """
        all_volumes = pd.read_csv(self._output_data_address+'volume_frame.csv', index_col=0)
        all_dates = all_volumes.index.tolist()
        numDays = all_dates.index(end_date) - all_dates.index(start_date) + 1
        trading_universes = [[]]*numDays
        for i in range(numDays):
            print "extract universe of day :%d" % int(i)
            window_size1 += 1                 # growling window size
            daily_info = all_volumes.iloc[all_dates.index(start_date)+i, :]                       # all the volumes including nan values
            daily_volume = daily_info[~daily_info.isnull()]                                       # today's tradable stocks
            first_day = data_utils.find_first_day_of_month(daily_volume.name, all_dates)          # first day of this month
            trading_universes[i] = data_utils.extract_universe_tushare(first_day, window_size1, window_size2,
                                                                       all_volumes.loc[:, daily_volume.index],
                                                                       all_dates, threshold)
        trading_universes_df = pd.DataFrame(trading_universes, index=all_dates[all_dates.index(start_date):all_dates.index(end_date)+1])
        trading_universes_df.to_csv(self._output_data_address+'tushare_thresh'+str(threshold)+'_window_size2_'+str(window_size2)+'_universe.csv')


if __name__ == "__main__":
    predictor_data_address = '/Users/yumu/Desktop/Internship/predictor_data/renamed_data/'
    output_data_address = '/Users/yumu/Desktop/Chinese-Data-Research/tushare data old/'
    inst = TushareDataHandling(predictor_data_address, output_data_address)
    # download data
    # inst.extract_data()
    # convert data to frame
    # inst.generate_price_frame_and_volume_frame()
    # generate universe
    start_date = '2002-01-04'
    end_date = '2017-11-01'
    window_size1 = 500
    window_size2 = 30
    threshold = 50
    inst.generate_trading_universe(window_size1, window_size2,
                                   threshold, start_date, end_date)

    print "done"
