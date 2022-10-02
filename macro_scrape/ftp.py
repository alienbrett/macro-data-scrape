import logging
from collections import namedtuple
from typing import Optional
import urllib

from dateutil import parser
import datetime
import csv
from ftplib import FTP
import pandas as pd
import pathlib
import os
import typing

import pytz

import tempfile

log = logging.getLogger(__name__)

def login(
    resource: namedtuple,
    user: str ='',
    password: str =''
    ) -> FTP:

    host = resource.netloc
    log.info('Logging into remote resource {0}'.format(host))

    ftp = FTP(host)
    ftp.login(user, password)
    
    log.info('Success, I think')
    return ftp


def last_modified(ftp: FTP, file_path: str , method: str ='mdtm', tzinfo=None):
    '''Method should be one of mdtm or mlsd
    '''
    if method == 'mdtm':
        timestamp = ftp.voidcmd("MDTM {}".format(file_path))[4:].strip()
        dt = parser.parse(timestamp)
    elif method == 'mlsd':
        tst = '{}'.format(file_path)
        for file in ftp.mlsd(''):
            if file[0] == tst:
                timestamp = file[1]['modify']
                dt = parser.parse(timestamp)
    else:
        raise RuntimeError('method invalid: {0}'.format(method))
    
    if tzinfo is not None:
        dt = dt.replace(tzinfo=tzinfo)
    log.info('remote file {0} last modified {1}'.format(file_path, dt))
    return dt
    
    


class FTPResource:

    def __init__(self,
        uri     : str,
        user    : str       = '',
        password: str       = '',
        tzinfo  : typing.Optional[pytz.tzinfo.BaseTzInfo]   = None
    ) -> None:
        self._tmp_dir_obj   = tempfile.TemporaryDirectory()

        self.ftp        = None
        self.uri        = uri
        self.user       = user
        self.tmp_dir    = None
        self.password   = password
        self.resource   = urllib.parse.urlparse(self.uri)

        # Allow for class overrides
        if getattr(self, 'tzinfo', None) is None:
            self.tzinfo = tzinfo

    def __enter__(self,):
        self.tmp_dir = self._tmp_dir_obj.__enter__()
        log.debug('Using temp directory {0}'.format(self.tmp_dir))
        return self

    def __exit__(self,*args, **kwargs):
        log.debug('Cleaning up temp directory {0}'.format(self.tmp_dir))
        return self._tmp_dir_obj.__exit__(*args,**kwargs)
        

    def login(self) -> None:
        if self.ftp is None:
            self.ftp = login(self.resource, self.user, self.password)


    def last_modified(self, suffix: Optional[str]= None) -> datetime.datetime:
        self.login()
        fname = self.resource.path
        if suffix is not None:
            fname = os.path.join(fname, suffix)
        return last_modified(self.ftp, file_path=fname, tzinfo = self.tzinfo)
    
    def get_fname(self, suffix: Optional[str] = None):
        fname = os.path.join(
            self.tmp_dir, self.resource.netloc,
            self.resource.path[1:] 
        )
        if suffix is not None:
            fname = os.path.join(fname, suffix)
        return fname
    
    def download_file(self, suffix: Optional[str] = None ) -> None:
        self.login()
        fname = self.get_fname(suffix)

        self.login()
        # Ensure path exists
        pathlib.Path(os.path.dirname(fname)).mkdir(parents=True, exist_ok=True)

        remote_path = self.resource.path
        if suffix is not None:
            remote_path = os.path.join( remote_path, suffix )

        remote_cmd = 'RETR {0}'.format( remote_path )
        with open(fname, 'wb') as local_file:
            log.info('Downloading remote file {0}'.format(remote_path))
            t0 = datetime.datetime.now()
            self.ftp.retrbinary(remote_cmd, local_file.write)
            t1 = datetime.datetime.now()
            log.info('Finished Downloading remote file {0} in {1:,.3f}s'.format(remote_path, (t1-t0).total_seconds()))
            
        
        if not self.local_copy_exists(suffix):
            raise ConnectionError('No file downloaded: no local copy exists')
        else:
            log.exception('local file found')
        
        return fname

    def local_copy_exists(self, suffix: Optional[str] = None) -> bool:
        fname = self.get_fname(suffix)
        exists = pathlib.Path(fname).exists()
        return exists

    def load_from_local(self, suffix: Optional[str] = None) -> pd.DataFrame:
        if self.local_copy_exists(suffix):
            fname = self.get_fname(suffix)
            return self._process(fname)
        else:
            raise FileNotFoundError('No local copy exists')


    def _process(self, fname: str) -> pd.DataFrame:
        pass




def detect_csv_dialect(file_path, n_lines_sniff=3):
    sniffer = csv.Sniffer()
    text = ''
    with open(file_path, 'r') as f:
        for i, line in enumerate(f):
            if i < n_lines_sniff:
                text += line
            else:
                break

    return sniffer.sniff(text)


def robust_read_csv(file_path, **kwargs):
    dialect = detect_csv_dialect(file_path)

    df = pd.read_csv(
        file_path,
        sep = dialect.delimiter,
        engine = 'c',
        **kwargs
    )
    return df