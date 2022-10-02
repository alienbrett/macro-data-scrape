import logging
import os
import datetime
import pytz

from .ftp import NasdaqFTPResource

log = logging.getLogger(__name__)


class NasdaqFolderFTPResource(NasdaqFTPResource):
    '''
    For more info, visit
    https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs
    '''
    tzinfo=pytz.utc

    def __init__(self,
        uri: str,
        user: str ='',
        password: str ='',
        fname_format: str = '',
        tzinfo=None
    ) -> None:
        super().__init__(
            uri=uri, user=user, password=password,
            tzinfo=tzinfo
        )
        self.fname_format = fname_format


    def fname_to_date(self, path):
        fname = os.path.basename(path)
        return datetime.datetime.strptime( fname, self.fname_format )

    def date_to_fname(self, date):
        fname = datetime.datetime.strftime( date, self.fname_format )
        return os.path.join( self.resource.path, fname )

    

    def list_files(self):
        self.login()
        print(self.resource.path)
        return self.ftp.nlst(self.resource.path)
    

    def list_dates_available(self):
        for path in self.list_files():
            yield self.fname_to_date(path)




nasdaq_resource_lookup = {
     'short_halts': 'shorthalts/',
}









if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    remote_folder = nasdaq_resource_lookup.get(
        'short_halts',
    )

    with NasdaqFolderFTPResource(
            uri = 'ftp://ftp.nasdaqtrader.com/symboldirectory/{}'.format(remote_folder),
            user = '',
            password = '',
            fname_format='shorthalts%Y%m%d.txt',
        ) as client:
    
        dt = client.last_modified('shorthalts20220121.txt').astimezone(pytz.timezone('US/Eastern'))
        print(dt)

        dates = list(client.list_dates_available())

        latest_date = max(dates)
        print(latest_date)


        file_path = client.date_to_fname( latest_date )
        fname = os.path.basename(file_path)
        print(file_path)

    if not client.local_copy_exists( suffix = fname ):
        print('downloading file')
        client.download_file( suffix = fname )

    df = client.load_from_local( suffix = fname )

    print(df.head())
    print(df.iloc[0].to_dict())
    print(df['Market Category'].unique())