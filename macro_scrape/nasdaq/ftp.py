import logging
log = logging.getLogger(__name__)

import datetime
import pandas as pd
import pytz

from ..ftp import FTPResource, robust_read_csv







class NasdaqFTPResource(FTPResource):
    '''
    For more info, visit
    https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs
    '''
    tzinfo=pytz.utc
    col_process_config = {
        'MP Type': 'expand_mpid_type',
        'NASDAQ Member': 'expand_y_n',
        'FINRA Member': 'expand_y_n',
        'NASDAQ BX Member': 'expand_y_n',
        'PSX Participant': 'expand_y_n',
        'Test Issue': 'expand_y_n',
        'ETF': 'expand_y_n',
        'Market Category': 'expand_market_category',
        'NextShares': 'expand_y_n',
        'Options Closing Type': 'expand_option_closing_type',
        'Pending': 'expand_y_n',
        'Options Type': 'expand_option_type',
        'Expiration Date': 'format_date',
        'Listing Exchange': 'expand_exchange',
        'Exchange': 'expand_exchange',
        'PHLX Traded': 'expand_y_n',
        'Trigger Time': 'format_date'
    }


    @staticmethod
    def expand_y_n(dx: pd.Series):
        return dx.replace({'Y':True,'N':False})

    @staticmethod
    def expand_mpid_type(dx: pd.Series):
        return dx.replace({
            'A':'agency quote',
            'C': 'ecn',
            'E': 'exchange',
            'M': 'market maker',
            'N': 'misc',
            'O': 'order entry firm',
            'P': 'nasdaq participant',
            'Q': 'query only firm',
            'S': 'specialist'
        })
    
    @staticmethod
    def expand_exchange(dx: pd.Series):
        return dx.replace({
            'A': 'NYSE MKT',
            'N': 'NYSE',
            'P': 'ARCA',
            'Z': 'BATS',
            'V': 'IEXG',
        })

    @staticmethod
    def expand_market_category(dx: pd.Series):
        return dx.replace({
            'Q': 'global select market-sm',
            'G': 'global market-sm',
            'S': 'capital market',
        })
    
    @staticmethod
    def expand_option_closing_type(dx: pd.Series):
        return dx.replace({ 'N': 'normal', 'L': 'late' })

    @staticmethod
    def expand_option_type(dx: pd.Series):
        return dx.replace({ 'P': 'put', 'C': 'call' })

    @staticmethod
    def format_date(dx: pd.Series):
        return pd.to_datetime(dx).dt.strftime('%Y-%m-%d')

    @staticmethod
    def expand_financial_status(dx: pd.Series):
        deficient_vals = 'DGHK'
        delinquent_vals = 'EHJK'
        bankrupt_vals = 'QGJK'

        return pd.DataFrame({
            'deficient': dx.apply(lambda x: (x in deficient_vals) if isinstance(x,str) else None),
            'delinquent': dx.apply(lambda x: (x in delinquent_vals) if isinstance(x,str) else None),
            'bankrupt': dx.apply(lambda x: (x in bankrupt_vals) if isinstance(x,str) else None),
        })


    @staticmethod
    def parse_file_create_time(row: pd.Series):
        try:
            desc_str = row.values[0].split(' ')[-1]
            dt = datetime.datetime.strptime(desc_str, '%m%d%Y%H:%M')
        except:
            try:
                desc_str = row.values[0]
                dt = datetime.datetime.strptime(desc_str, '%Y%m%d%H%M%S')
            except:
                raise RuntimeError('Failed to parse file timestamp')

        dt = dt.replace(
            tzinfo=pytz.timezone('US/Eastern')
        )
        return dt


    def _process(self, path: str):
        df = robust_read_csv(
            path
        )
        log.debug('example FTP df row: {0}'.format(str(dict(df.iloc[0]))))

        df, last_row = df.iloc[:-1], df.iloc[-1]
        file_create_datetime = NasdaqFTPResource.parse_file_create_time(last_row)

        df['File Create Date'] = file_create_datetime

        for col in df:
            fn_name = NasdaqFTPResource.col_process_config.get(col, None)
            if fn_name is not None:
                fn = getattr(NasdaqFTPResource, fn_name)
                df[col] = fn(df[col])

        if 'Financial Status' in df.columns:
            df = pd.concat(
                [
                    df.drop(columns=['Financial Status']),
                    NasdaqFTPResource.expand_financial_status(df['Financial Status'])
                ],
                axis=1
            )

        return df



nasdaq_resource_lookup = {
     'listed_tickers': 'nasdaqlisted.txt',
     'other_listed': 'otherlisted.txt',
     'traded_tickers': 'nasdaqtraded.txt',
     'market_participants': 'mpidlist.txt',
     'options': 'options.txt',
     'bxoptions': 'bxoptions.txt',
     'psx_traded': 'psxtraded.txt',
}




ftp_clients = {}



def get_nasdaq_ftp_client (
    name: str,
    # tmp_dir: str ='/tmp/data_cache'
    ) -> NasdaqFTPResource:

    name = name.lower()

    file_name = nasdaq_resource_lookup.get(name)
    client = NasdaqFTPResource(
        uri = 'ftp://ftp.nasdaqtrader.com/symboldirectory/{}'.format(file_name),
        user = '',
        password = '',
    )

    return client



ftp_dated_clients = {}









if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    with get_nasdaq_ftp_client(
            name = 'other_listed',
            # name = 'listed_tickers',
            # name = 'options',
            # name = 'bxoptions',
            # name = 'psx_traded',
        ) as client:

        print(client.last_modified().astimezone(pytz.timezone('US/Eastern')))

        if not client.local_copy_exists():
            print('downloading file')
            client.download_file()

        df = client.load_from_local()

    print(df.head())
    print(df.iloc[0].to_dict())
    ticker_col = next(filter(lambda x: x in df.columns, [
        'CQS Symbol',
        'Symbol',
    ]))
    print(df.loc[df[ticker_col].isin([
        # 'SPY',
        'VXX',
        'SVIX',
    ])].T)