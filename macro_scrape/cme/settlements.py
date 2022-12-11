import logging
log = logging.getLogger(__name__)

import numpy as np
import typing
import dataclasses
import datetime
import pandas as pd

from ..ftp import FTPResource


class CMESettleResource(FTPResource):

    def _process(self, fname: str) -> pd.DataFrame:
        df = pd.read_csv(fname)
        # df['BizDt'] = pd.to_datetime(df['BizDt'])

        return df
    

    @classmethod
    def get_settlements(cls, dt : datetime.date, exch : str = 'CME', compressed:bool=True) -> pd.DataFrame:
        uri = 'ftp://ftp.cmegroup.com/settle/{exch}.settle.{dt:%Y%m%d}.s.csv{comp_str}'.format(
            dt = dt,
            exch = exch.lower(),
            comp_str = '.zip' if compressed else ''
        )
        with CMESettleResource( uri = uri, ) as client:

            if not client.local_copy_exists():
                log.info('downloading file {0}'.format(uri))
                client.download_file()
            else:
                log.info('already got file {0}'.format(uri))

            df = client.load_from_local()
        return df
