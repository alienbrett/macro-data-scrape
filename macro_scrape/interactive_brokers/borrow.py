import logging
import asyncio
import pandas as pd
import numpy as np
import pytz
# from quote.db_utils import insert_quote

from ..ftp import FTPResource

log = logging.getLogger(__name__)

##############
class IBBorrowFTPResource(FTPResource):
    tzinfo = pytz.timezone('US/Eastern')

    def _process(self, fname: str) -> pd.DataFrame:
        df = pd.read_csv(fname, sep='|', header=1).drop(columns=['Unnamed: 8']).rename(columns={
            '#SYM': 'symbol',
            'NAME': 'desc',
            'REBATERATE': 'lend',
            'FEERATE': 'borrow'
        }).set_index('symbol').iloc[:-1]
        df.columns = list(map(lambda x: x.lower(), df.columns))

        df['lend'] = -1 * df['lend'] / 100
        df['borrow'] = df['borrow'] / 100

        df['available'] = pd.to_numeric(df['available'], errors='coerce').fillna(np.inf)

        return df

def get_ib_borrow_ftp_client (
    country: str ='usa',
    ) -> IBBorrowFTPResource:

    country = country.lower()
    log.info('making new IB FTP client for country {0}'.format(country))

    client = IBBorrowFTPResource(
        uri = 'ftp://ftp3.interactivebrokers.com/{}.txt'.format(country),
        user = 'shortstock',
        password = '',
    )

    return client