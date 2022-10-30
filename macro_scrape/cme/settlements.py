import logging
log = logging.getLogger(__name__)

import numpy as np
import typing
import dataclasses
import datetime
import pandas as pd
import pytz
import urllib

from ..ftp import FTPResource, robust_read_csv



@dataclasses.dataclass
class DFMuncher:
    lines : typing.List[str]

    dt : typing.Optional[datetime.datetime] = None
    df : typing.Optional[pd.DataFrame] = None

    header_config = [
        ('strike_month', 0, None),

        ('open', 15, None),
        ('open_bid_ask', 16, 1),

        ('high', 25, None),
        ('high_bid_ask', 26, 1),

        ('low', 35, None),
        ('high_bid_ask', 26, 1),

        ('last', 45, None),
        ('high_bid_ask', 26, 1),

        ('settlement', 55, None ),
        ('high_bid_ask', 26, 1),

        ('change', 63, None ),
        ('estimated_volume', 75, None),
        ('prior_settlement', 86, None),
        ('prior_volume', 98, None),
        ('prior_open_interest', 110, None),
    ]

    def munch_file_create_dt(self,) -> datetime.datetime:
        line = self.lines.pop(0)
        tokens = line.split(' OF ')[1].split(' ')
        if tokens[-1] == '(CST)':
            tz = pytz.timezone('US/Central')
        else:
            raise RuntimeError('Unknown timezone [{0}]'.format(tokens[-1]))
        return tz.localize(
            pd.to_datetime(' '.join(tokens[:-1])).to_pydatetime()
        )

    def munch_data_row(self, line) -> typing.Optional[typing.List[str]]:
        return {
            cfg[0] : DFMuncher.get_token_by_rjust(line, pos=cfg[1], width=cfg[2])
            for cfg in self.header_config
        }


    @staticmethod
    def get_token_by_rjust(line:str, pos:int, width:typing.Optional[int]=None, sep=' ') -> typing.Optional[str]:
        if pos == 0:
            res = line.split(sep)[0]
            if width is None:
                return res
            else:
                return res[:width]
        else:
            res = line[:pos].split(' ')[-1]
            if width is None:
                return res
            else:
                return res[-width:]

    @staticmethod
    def is_meta_line(line:str):
        return ('TOTAL' in line) or ('END OF REPORT' in line)

    def munch_df(self,):
        data_lines = []

        # Get header row
        header = self.lines.pop(0)
        # print('got header: [{0}]'.format(header))
        while len(self.lines):

            ## Conditions
            is_meta_row = DFMuncher.is_meta_line(self.lines[0])
            is_data_row_by_length = len(self.lines[0]) in (110,86,98)

            if (not is_meta_row) and is_data_row_by_length:
                data_lines.append( self.lines.pop(0) )

            else:
                ### Consume/destroy all total rows
                while len(self.lines) and DFMuncher.is_meta_line(self.lines[0]):
                    self.lines.pop(0)

                break

        js = list(filter(len,header.split(' ')))
        # print('using js: [{0}]'.format(js))
        product_code = js[0]
        description  = ' '.join(js[1:])

        rows = [
            {
                **self.munch_data_row(line),
                'product_code': product_code,
                'product_name': description,
            }
            for line in data_lines
        ]

        # print()
        return rows



    def munch_all(self,):
        self.lines = list(filter(len,self.lines))

        if len(self.lines):
            self.dt = self.munch_file_create_dt()
            self.lines.pop(0)
            self.lines.pop(0)

            rows = []
            while len(self.lines):
                rows += self.munch_df()
            
            self.df = pd.DataFrame(rows)
        else:
            print('Already munched')

        return self.df




class CMEFTPResource(FTPResource):
    '''
    For more info, visit
    '''
    tzinfo=pytz.utc
    col_process_config = {
        # 'MP Type': 'expand_mpid_type',
    }

    @staticmethod
    def standardize_future_code(dj:pd.Series):
        year_str = dj['strike_month'].str.slice(start=-2)
        month_str = dj['strike_month'].str.slice(stop=-2)
        ending = month_str.replace({
            'JAN':'F',
            'FEB':'G',
            'MAR':'H',
            'APR':'J',
            'MAY':'K',
            'JUN':'M',
            'JLY':'N',
            'AUG':'Q',
            'SEP':'U',
            'OCT':'V',
            'NOV':'X',
            'DEC':'Z',
        }).str.cat(year_str.values, sep='')
        return dj['product_code'].str.cat(ending,sep='')

    @staticmethod
    def process_future(df: pd.DataFrame) -> pd.DataFrame:
        df['standard_future_code'] = CMEFTPResource.standardize_future_code(df)
        return df


    def process_option(df: pd.DataFrame) -> pd.DataFrame:
        return df



    def _process_df(self,df : pd.DataFrame) -> pd.DataFrame:
        dj = df.loc[ ~df['product_code'].isin({'00N','0GE'}) ].copy()

        dj['is_call'] = dj['product_name'].str.contains('CALL')
        dj['is_put'] = dj['product_name'].str.contains('PUT')
        dj['is_future'] = ~(dj['is_call'] | dj['is_put'])

        return pd.concat([
            CMEFTPResource.process_option(dj.loc[~dj['is_future']]),
            CMEFTPResource.process_future(dj.loc[dj['is_future']]),
        ], axis=0)

    def _process(self, path: str):
        with open(path, 'r') as f:
            rows = f.read().split('\n')

        muncher = DFMuncher(rows)
        muncher.munch_all()
        df = muncher.df

        df = df.replace({
            'UNCH':0.,
            '----':np.nan,
        })
        df = self._process_df(df)
        return df



cme_resource_lookup = {
     'settlements': '/pub/settle/stlint',
}


def get_cme_ftp_client (
    name: str,
    ) -> CMEFTPResource:

    name = name.lower()

    file_name = cme_resource_lookup.get(name)
    client = CMEFTPResource(
        uri = urllib.parse.urljoin(
            'ftp://ftp.cmegroup.com',
            file_name,
        ),
        user = '',
        password = '',
    )

    return client



ftp_dated_clients = {}









if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

    with get_cme_ftp_client(
            name = 'settlements',
        ) as client:

        print(client.last_modified().astimezone(pytz.timezone('US/Eastern')))

        if not client.local_copy_exists():
            print('downloading file')
            client.download_file()

        df = client.load_from_local()

    print(df.head())
    print(df.iloc[0].to_dict())
    # ticker_col = next(filter(lambda x: x in df.columns, [
    #     'CQS Symbol',
    #     'Symbol',
    # ]))
    # print(df.loc[df[ticker_col].isin([
    #     # 'SPY',
    #     'VXX',
    #     'SVIX',
    # ])].T)