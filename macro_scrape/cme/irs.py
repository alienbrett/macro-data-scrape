import logging
log = logging.getLogger(__name__)

import pandas as pd
import datetime
import pytz

from ..ftp import FTPResource


class CMEIRSResource(FTPResource):

    def _process(self, fname: str) -> pd.DataFrame:
        df = pd.read_csv(fname)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        return df
    

    @classmethod
    def get_atm_vol_on_date(cls, dt : datetime.date) -> pd.DataFrame:
        uri = 'ftp://ftp.cmegroup.com/irs/CME_ATM_VolCube_{0:%Y%m%d}.csv'.format(dt)
        with CMEIRSResource( uri = uri, ) as client:
            log.info('file last modified: {0}'.format(client.last_modified().astimezone(pytz.timezone('US/Eastern'))))

            if not client.local_copy_exists():
                log.info('downloading file {0}'.format(uri))
                client.download_file()
            else:
                log.info('already got file {0}'.format(uri))

            df = client.load_from_local()
        return df
