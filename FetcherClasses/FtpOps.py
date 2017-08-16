'''
Created on May 10, 2016

@author: David R. Darling
'''

import logging
import os
import time

import ftplib
import ftputil
from ftputil.error import FTPError
import pysftp

from ensure import ensure_annotations

from FetcherClasses.Classes.EnumCommons import FtpLibNameEnum
from FetcherClasses.Classes.LoggingCommons import LoggingUtilities
from FetcherClasses.Classes.UtilityCommons import DynamicUtilities

host_url = 'localhost'
host_type = FtpLibNameEnum.FTPUTIL
host_port = 22 if host_type == FtpLibNameEnum.PYSFTP else 21
username = 'fetch_dlz'
folder_path_prefix = '/'
folder_paths_list = ['TEST1','TEST2']
recurse_remote_folders = True
remove_remote_file_on_download = True
scan_interval_seconds = 5
output_path_name = 'temp'


class FtpScanner(object):


    host_url = 'localhost'
    host_type = FtpLibNameEnum.PYSFTP
    host_port = 22 if host_type == FtpLibNameEnum.PYSFTP else 21
    username = 'anonymous'
    password = 'anonymous'
    folder_path_prefix = '/'
    initial_folder_path = '.'
    scan_interval_seconds = 60
    prcnt_download_amount = 5
    recursively = False
    close_connection = False
    is_test_mode = False

    target_file = None
    target_full_name = None
    
    output_path_name = 'temp'

    bytes_xfered_sofar = 0
    bytes_xfered_total = 0
    bytes_xfered_prcnt = 0
    
    loggingUtilities = LoggingUtilities()
    dynamicUtilities = DynamicUtilities()

    logger = logging
    
    file_dict = {}
    
    ftp_conn = None
    
    first_report = True
    not_yet_reported = True


    @ensure_annotations
    def __init__(self,
                 host_url: str='localhost',
                 host_type: int=FtpLibNameEnum.PYSFTP,
                 host_port: int=22,
                 username: str='anonymous',
                 folder_path_prefix: str='',
                 initial_folder_path: str='.',
                 scan_interval_seconds: int=60,
                 is_test_mode: bool=False):

        self.host_url = host_url
        self.host_port = host_port
        self.host_type = host_type
        self.username = username
        self.folder_path_prefix = folder_path_prefix
        self.initial_folder_path = initial_folder_path
        self.scan_interval_seconds = scan_interval_seconds
        self.is_test_mode = is_test_mode


        self.logger, _allLoggerFH, _errLoggerFH, err_str = self.loggingUtilities.get_logger(dft_msg_format=self.loggingUtilities.dft_msg_format,
                                                                                            dft_date_format=self.loggingUtilities.dft_date_format,
                                                                                            log_file_mode=self.loggingUtilities.log_file_mode)

        return err_str


    @ensure_annotations
    def get_ftputil_callback(self,
                             data_block):
        self.bytes_xfered_sofar += len(data_block)
        if self.bytes_xfered_total > 0:
            bytes_xfered_prcnt = int(self.bytes_xfered_sofar * 100.0 / self.bytes_xfered_total)
        else:
            bytes_xfered_prcnt = 0
        if self.first_report:
            self.logger.info('Bytes xfered: %d of total bytes: %d, pct transferred: %d', self.bytes_xfered_sofar, self.bytes_xfered_total, bytes_xfered_prcnt)
            self.first_report = False
        if bytes_xfered_prcnt == 100 or ((bytes_xfered_prcnt - self.bytes_xfered_prcnt) >= self.prcnt_download_amount):
            self.bytes_xfered_prcnt = bytes_xfered_prcnt
            self.logger.info('Bytes xfered: %d of total bytes: %d, pct transferred: %d', self.bytes_xfered_sofar, self.bytes_xfered_total, bytes_xfered_prcnt)
        return


    @ensure_annotations
    def get_pysftp_callback(self,
                            bytes_xfered_sofar: int,
                            bytes_xfered_total: int):
        if bytes_xfered_total > 0:
            bytes_xfered_prcnt = int(bytes_xfered_sofar * 100.0 / bytes_xfered_total)
        else:
            bytes_xfered_prcnt = 0
        if self.first_report:
            self.logger.info('Bytes xfered: %d of total bytes: %d, pct transferred: %d', bytes_xfered_sofar, bytes_xfered_total, bytes_xfered_prcnt)
            self.first_report = False
        if (bytes_xfered_prcnt == 100 and self.not_yet_reported) or ((bytes_xfered_prcnt - self.bytes_xfered_prcnt) >= self.prcnt_download_amount):
            self.bytes_xfered_prcnt = bytes_xfered_prcnt
            self.logger.info('Bytes xfered: %d of total bytes: %d, pct transferred: %d', bytes_xfered_sofar, bytes_xfered_total, bytes_xfered_prcnt)
        if bytes_xfered_prcnt == 100:
            self.not_yet_reported = False
        return


    @ensure_annotations
    def get_connection(self,
                       logger=None,
                       is_test_mode: bool=False):

        ftp_conn = None
        
        if logger is None:
            logger = self.logger
            
        if is_test_mode is None:
            is_test_mode = self.is_test_mode

        # obtain password to FTP site via user's password keyring
        password, err_str = self.dynamicUtilities.get_pwd_via_keyring(key=host_url,
                                                                      login=self.username,
                                                                      redact_passwords=True,
                                                                      log_results=True,
                                                                      logger=logger,
                                                                      is_test_mode=is_test_mode)
        
        if not err_str:
            if self.host_type == FtpLibNameEnum.PYSFTP:
                try:
                    logger.info('Connecting to FTP server "%s" via PYSFTP, please wait...', host_url)
                    ftp_conn = pysftp.Connection(host=self.host_url, username=self.username, password=password)
                    logger.info('Connected to FTP server "%s" via PYSFTP', host_url)
                except Exception as err:
                    err_str = str(err)
                    logger.error(err_str)
            elif self.host_type == FtpLibNameEnum.FTPUTIL:
                try:
                    logger.info('Connecting to FTP server "%s" via FTPUTIL, please wait...', host_url)
                    ftp_conn = ftputil.FTPHost(host=self.host_url, user=self.username, passwd=password)
                    logger.info('Connected to FTP server "%s" via FTPUTIL!', host_url)
                except Exception as err:
                    err_str = str(err)
                    logger.error(err_str)

        return ftp_conn, err_str


    @ensure_annotations
    def get_remote_file(self,
                        remote_path_name: str,
                        remote_file_name: str,
                        target_path_name: str,
                        target_file_name: str,
                        ftp_conn=None,
                        close_connection: bool=False,
                        remove_remote_file_on_download: bool=False,
                        logger=None,
                        is_test_mode: bool=False):
        
        err_str = None
        
        if logger is None:
            logger = self.logger
            
        if is_test_mode is None:
            is_test_mode = self.is_test_mode

        if self.target_file is not None:
            self.target_file.close()
            self.target_file = None

        if remote_path_name is None:
            remote_path_name = self.folder_path_prefix + self.initial_folder_path
            
        remote_dirname = remote_path_name
        if remote_dirname.startswith('/'):
            remote_dirname = remote_dirname[1:]

        self.target_full_name = self.dynamicUtilities.expand_path(path_fldr=target_path_name,
                                                                  path_name=target_file_name,
                                                                  logger=logger,
                                                                  is_test_mode=is_test_mode)

        target_path_name = os.path.dirname(self.target_full_name)
        if not os.path.exists(target_path_name):
            try:
                logger.info('Make directory ATTEMPT: "%s"', target_path_name)
                _dirname, err_str = self.dynamicUtilities.create_path(path_name=target_path_name,
                                                                      contains_file_name=False,
                                                                      logger=logger,
                                                                      is_test_mode=is_test_mode)
                if err_str is not None:
                    logger.error('Make directory FAILURE: "%s"', target_path_name)
                    logger.error(err_str)
                else:
                    logger.info('Make directory SUCCESS: "%s"', target_path_name)
            except Exception as err:
                err_str = str(err)
                logger.error('Make directory FAILURE: "%s"', target_path_name)
                logger.error(err_str)

        if err_str is None:
            if ftp_conn is None:
                ftp_conn, err_str = self.get_connection()
                close_connection = True

    #         if err_str is None:
    #             logger.info('Navigating to remote folder: "%s"' % remote_path_name)
    #             ftp_conn.chdir(remote_path_name)
    #             logger.info('Navigated to remote folder: "%s"' % remote_path_name)

        if err_str is None:
            try:
                _resp_code = None
                if self.host_type == FtpLibNameEnum.PYSFTP:
                    # pass because download handled
                    # in the walktreeFcallback method
                    if remote_path_name is not None and remote_path_name != '':
                        entity_path = remote_path_name + '/' + remote_file_name
                    else:
                        entity_path = remote_file_name
                    # derive target path name for use with folder creation if so needed
                    target_path_name = self.dynamicUtilities.expand_path(path_fldr=self.output_path_name,
                                                                         path_name=os.path.dirname(entity_path),
                                                                         logger=logger,
                                                                         is_test_mode=is_test_mode)
                    if not os.path.exists(target_path_name):
                        try:
                            self.logger.info('Target folder creation ATTEMPT: "%s"', target_path_name)
                            _dirname, err_str = self.dynamicUtilities.create_path(path_name=target_path_name,
                                                                                  contains_file_name=False,
                                                                                  logger=logger,
                                                                                  is_test_mode=is_test_mode)
                            self.logger.info('Target folder creation SUCCESS: "%s"', target_path_name)
                        except Exception as err:
                            err_str = str(err)
                            self.logger.error('Target folder creation FAILURE: "%s"', target_path_name)
                            self.logger.error(err_str)
            
                    # derive target destination path                
                    target_dest_name = self.dynamicUtilities.expand_path(path_fldr=self.output_path_name,
                                                                         path_name=entity_path,
                                                                         logger=logger,
                                                                         is_test_mode=is_test_mode)
                        
                    # if the remote file exists
                    if self.ftp_conn.exists(entity_path):
                        try:
                            self.logger.info('Remote file accessible YET? "%s"', entity_path)
                            _resp_code = self.ftp_conn.open(remote_file=entity_path,
                                                            mode='a',
                                                            bufsize=-1)
                            self.logger.info('Remote file accessible NOW! "%s"', entity_path)
                            self.bytes_xfered_prcnt = 0
                            try:
                                self.logger.info('Remote file download ATTEMPT: "%s"', entity_path)
                                self.not_yet_reported = True
                                self.first_report = True
                                _resp_code = self.ftp_conn.get(remotepath=entity_path,
                                                               localpath=target_dest_name,
                                                               callback=self.get_pysftp_callback,
                                                               preserve_mtime=True)
                                self.logger.info('Remote file download SUCCESS: "%s"', entity_path)
                            except Exception as err:
                                err_str = str(err)
                                self.logger.error('Remote file download FAILURE! "%s"', entity_path)
                                self.logger.error(err_str)
                            if self.remove_remote_file_on_download:
                                try:
                                    self.logger.info('Remote file removal ATTEMPT: "%s"', entity_path)
                                    self.ftp_conn.remove(entity_path)
                                    self.logger.info('Remote file removal SUCCESS: "%s"', entity_path)
                                except Exception as err:
                                    err_str = str(err)
                                    self.logger.error('Remote file removal FAILURE! "%s"', entity_path)
                                    self.logger.error(err_str)
                        except TypeError as err:
                            self.logger.info(err)
                            self.logger.info('Remote file accessible NOW! "%s"', entity_path)
                        except IOError as err:
                            self.logger.info(err)
                            self.logger.info('Remote file accessible NOT! "%s"', entity_path)
                            self.logger.info('Will attempt accessibility on next iteration..')
                    else:
                        self.logger.error('Remote file NOT found: %s', entity_path)
                elif self.host_type == FtpLibNameEnum.FTPUTIL:
                    # if the source file exists
                    if ftp_conn.path.exists(remote_file_name):
                        # resp_code = ftp_util.open(path=src_file_name, mode='rb')
                        logger.info('Obtaining remote file size for: %s', remote_file_name)
                        self.bytes_xfered_prcnt = 0
                        self.bytes_xfered_sofar = 0
                        self.bytes_xfered_total = ftp_conn.path.getsize(remote_file_name)
                        logger.info('Obtained remote file size %d for: "%s"', self.bytes_xfered_total, remote_file_name)
                        logger.info('Download attempted: "%s"', remote_file_name)
                        self.first_report = True
                        _resp_code = ftp_conn.download(source=remote_file_name,
                                                       target=self.target_full_name,
                                                       callback=self.get_ftputil_callback)
                        logger.info('Download completed: "%s"', self.target_full_name)
                        if remove_remote_file_on_download:
                            logger.info('Remove downloaded file attempted: "%s"', remote_file_name)
                            ftp_conn.remove(remote_file_name)
                            logger.info('Remove downloaded file successful: "%s"', remote_file_name)
                    else:
                        logger.error('FTP file NOT found: "%s"', remote_file_name)
                        logger.error('Will attempt download later...')
            except TypeError as err:
                logger.info(err)
                logger.info('FTP file is now downloadable: "%s"', remote_file_name)
            except ftplib.error_temp as err:
                logger.info(err)
            except ftplib.error_reply as err:
                logger.info(err)
            except ftplib.error_perm as err:
                logger.info(err)
                logger.info('FTP file is NOT yet downloadable: "%s"', remote_file_name)
                logger.info('Will attempt download later...')
            except IOError as err:
                logger.info(err)
                logger.info('FTP file is NOT yet downloadable: "%s"', remote_file_name)
                logger.info('Will attempt download later..')


        if ftp_conn is not None and close_connection:
            # close the FTP connection as appropriate
            try:
                logger.info('Closing FTP connection')
                ftp_conn.close()
                logger.info('Closed FTP connection')
            except Exception as err:
                logger.info(err)

        return self.target_full_name, err_str


    @ensure_annotations
    def get_remote_files(self,
                         remote_path_name: str=None,
                         target_path_name: str=None,
                         ftp_conn=None,
                         close_connection: bool=False,
                         remove_remote_file_on_download: bool=False,
                         logger=None,
                         is_test_mode: bool=False):

        err_str = None
        list_of_files = []

        if logger is None:
            logger = self.logger
            
        if is_test_mode is None:
            is_test_mode = self.is_test_mode
        else:
            self.is_test_mode = is_test_mode
            
        if remote_path_name is None:
            remote_path_name = self.remote_path_name
        else:
            self.remote_path_name = remote_path_name
            
        if target_path_name is None:
            target_path_name = self.target_path_name
        else:
            self.target_path_name = target_path_name
            
        if ftp_conn is None:
            ftp_conn = self.get_connection(logger=logger,
                                           is_test_mode=is_test_mode)
            close_connection = True
        else:
            self.ftp_conn = ftp_conn
            
        if close_connection is None:
            close_connection = self.close_connection

        if remote_path_name is None:
            remote_path_name = self.folder_path_prefix + self.initial_folder_path
            
        if remove_remote_file_on_download is None:
            remove_remote_file_on_download = self.remove_remote_file_on_download
        else:
            self.remove_remote_file_on_download = remove_remote_file_on_download

        if err_str is None:
            logger.info('Navigate to remote folder ATTEMPT: "%s"', remote_path_name)
            if self.remote_path_exists(remote_path_name):
                ftp_conn.chdir(remote_path_name)
                logger.info('Navigate to remote folder SUCCESS: "%s"', remote_path_name)
                try:
                    logger.info('Scanning remote folder: "%s"' % remote_path_name)
                    if self.host_type == FtpLibNameEnum.FTPUTIL:
                        file_names_list = ftp_conn.listdir(remote_path_name)
                        for remote_file_name in file_names_list:
                            if ftp_conn.path.isfile(remote_file_name):
                                logger.info('Fetching remote file: "%s"', remote_file_name)
                                self.get_remote_file(remote_path_name=remote_path_name,
                                                     remove_file_name=remote_file_name,
                                                     target_path_name=target_path_name,
                                                     target_file_name=remote_file_name,
                                                     ftp_conn=ftp_conn,
                                                     close_connection=close_connection,
                                                     remove_remote_file_on_download=remove_remote_file_on_download,
                                                     logger=logger,
                                                     is_test_mode=is_test_mode)
                    else:
                        file_names_list = ftp_conn.listdir()
                        for remote_file_name in file_names_list:
                            if ftp_conn.isfile(remote_file_name):
                                logger.info('Fetching remote file: "%s"', remote_file_name)
                                self.get_remote_file(remote_path_name=remote_path_name,
                                                     remote_file_name=remote_file_name,
                                                     target_path_name=target_path_name,
                                                     target_file_name=remote_file_name,
                                                     ftp_conn=ftp_conn,
                                                     close_connection=close_connection,
                                                     remove_remote_file_on_download=remove_remote_file_on_download,
                                                     logger=logger,
                                                     is_test_mode=is_test_mode)
                except Exception as err:
                    logger.error(err)
            else:
                logger.error('Navigate to remote folder FAILURE: "%s"', remote_path_name)
                logger.error('Remote path does not exist or is inaccessible!')

        # close the FTP connection as appropriate
        if ftp_conn is not None and close_connection:
            logger.info('Closing FTP connection')
            ftp_conn.close()
            logger.info('Closed FTP connection')

        return list_of_files, err_str


    @ensure_annotations
    def get_remote_folders_files(self,
                                 remote_path_name: str,
                                 entity_path_name: str,
                                 target_path_name: str,
                                 recursively: bool=False,
                                 ftp_conn=None,
                                 close_connection: bool=False,
                                 remove_remote_file_on_download: bool=False,
                                 logger=None,
                                 is_test_mode: bool=False):
        
        err_str = None

        if logger is None:
            logger = self.logger
            
        if remote_path_name is None:
            remote_path_name = self.remote_path_name
        else:
            self.remote_path_name = remote_path_name
            
        if target_path_name is None:
            target_path_name = self.target_path_name
        else:
            self.target_path_name = target_path_name
            
        if recursively is None:
            recursively = self.recursively
        else:
            self.recursively = recursively
            
        if ftp_conn is None:
            ftp_conn = self.ftp_conn
        else:
            self.ftp_conn = ftp_conn
            
        if close_connection is None:
            close_connection = self.close_connection
        else:
            self.close_connection = close_connection
            
        if remove_remote_file_on_download is None:
            remove_remote_file_on_download = self.remove_remote_file_on_download
        else:
            self.remove_remote_file_on_download = remove_remote_file_on_download
            
        if is_test_mode is None:
            is_test_mode = self.is_test_mode
        else:
            self.is_test_mode = is_test_mode

        if self.host_type == FtpLibNameEnum.PYSFTP:
            try:
                logger.info('Current working folder: "%s"', ftp_conn.pwd)
                logger.info('Does remote path exist? "%s"', remote_path_name)
                if ftp_conn.exists(remote_path_name):
                    logger.info('Remote path does exist! "%s"', remote_path_name)
                    logger.info('Navigate to remote folder ATTEMPT: "%s"', remote_path_name)
                    ftp_conn.cd(remote_path_name)
                    logger.info('Navigate to remote folder SUCCESS: "%s"', remote_path_name)
                    ftp_conn.walktree(remote_path_name,
                                      self.walktreeFcallback,
                                      self.walktreeDcallback,
                                      self.walktreeUcallback)
                else:
                    logger.error('Remote path does NOT exist! "%s"', remote_path_name)
            except FTPError as err:
                errStr = str(err)
                logger.error('WALK of FTP folder "%s" encountered an FTP error: %s%s', errStr, os.linesep)
                logger.error('Program will retry WALK on the next iteration!%s', os.linesep)
        else:
            for dirpath, dirnames, filenames in ftp_conn.walk(remote_path_name):
                for filename in filenames:
                    target_path_name_temp = self.dynamicUtilities.expand_path(path_fldr=self.output_path_name,
                                                                              path_name=dirpath[1:] if dirpath.startswith('/') else dirpath,
                                                                              logger=logger,
                                                                              is_test_mode=is_test_mode)
                    try:
                        logger.info('Navigate to remote folder ATTEMPT: "%s"', dirpath)
                        ftp_conn.chdir(dirpath)
                        logger.info('Navigate to remote folder SUCCESS: "%s"', dirpath)
                        self.get_remote_file(dirpath, filename, target_path_name_temp, filename, ftp_conn, close_connection, remove_remote_file_on_download, logger=logger)
                    except Exception as err:
                        err_str = str(err)
                        logger.error('Navigate to remote folder FAILURE: "%s"', dirpath)
                        logger.error(err_str)
                if recursively:
                    for dirname in dirnames:
                        temppath = ftp_conn.path.join(dirpath, dirname)
                        target_path_name_temp = self.dynamicUtilities.expand_path(path_fldr=self.output_path_name,
                                                                                  path_name=dirpath[1:] if dirpath.startswith('/') else dirpath,
                                                                                  logger=logger,
                                                                                  is_test_mode=is_test_mode)
                        self.get_remote_folders_files(remote_path_name=temppath,
                                                      entity_path_name=dirpath,
                                                      target_path_name=target_path_name,
                                                      recursively=recursively,
                                                      ftp_conn=ftp_conn,
                                                      close_connection=close_connection,
                                                      remove_remote_file_on_download=remove_remote_file_on_download,
                                                      logger=logger,
                                                      is_test_mode=is_test_mode)

        return err_str
    
    
    @ensure_annotations
    def remote_path_exists(self,
                           path_name: str,
                           logger=None):
        
        exists = False
        err_str = None
        
        if logger is None:
            logger = self.logger
        
        try:
            if self.host_type == FtpLibNameEnum.FTPUTIL:
                logger.info('Remote path exists via FTPUTIL? "%s"', path_name)
                exists = self.ftp_conn.path.exists(path_name)
                logger.info('Remote path exists via FTPUTIL! "%s"', path_name)
            else:
                logger.info('Remote path exists via PYSFTP? "%s"', path_name)
                exists = self.ftp_conn.exists(path_name)
                logger.info('Remote path exists via PYSFTP! "%s"', path_name)
        except Exception as err:
            err_str = str(err)
            logger.error('Remote path name does NOT exist: "%s"', path_name)
            logger.error(err_str)

        return exists, err_str


    @ensure_annotations
    def walktreeFcallback(self,
                          entity_path: str):
        
        # derive target path name for use with folder creation if so needed
        target_path_name = self.dynamicUtilities.expand_path(path_fldr=self.output_path_name,
                                                             path_name=os.path.dirname(entity_path),
                                                             logger=self.logger,
                                                             is_test_mode=self.is_test_mode)
        
        _target_full_name, _err_str = self.get_remote_file(os.path.dirname(entity_path), os.path.basename(entity_path), target_path_name, os.path.basename(entity_path), self.ftp_conn, self.close_connection, self.remove_remote_file_on_download, self.logger)
        
        return


    @ensure_annotations
    def walktreeDcallback(self,
                          entity_path: str):
        
        return


    @ensure_annotations
    def walktreeUcallback(self,
                          entity_path: str):
        
        
        return


if __name__ == '__main__':

    while True:
        
        for current_folder_path in folder_paths_list:
    
            remote_path_name = folder_path_prefix + current_folder_path
        
            ftpScanner = FtpScanner(host_url=host_url,
                                    host_type=host_type,
                                    host_port=host_port,
                                    username=username,
                                    folder_path_prefix=folder_path_prefix,
                                    initial_folder_path=current_folder_path,
                                    scan_interval_seconds=scan_interval_seconds,
                                    is_test_mode=False)

            ftpScanner.output_path_name = output_path_name
            if not os.path.exists(output_path_name):
                _dirname, err_str = ftpScanner.dynamicUtilities.create_path(path_name=output_path_name,
                                                                            contains_file_name=False,
                                                                            logger=ftpScanner.logger,
                                                                            is_test_mode=ftpScanner.is_test_mode)
        
            ftpScanner.ftp_conn, err_str = ftpScanner.get_connection()
            
            if not err_str:
                
                ftpScanner.logger.info('Remote path exists YES? "%s"', remote_path_name)
                path_exists, err_str = ftpScanner.remote_path_exists(remote_path_name)
                if not path_exists:
                    ftpScanner.logger.warning('Remote path exists NOT! "%s"', remote_path_name)
                    if folder_path_prefix == '/':
                        ftpScanner.logger.warning('Remove folder_path_prefix, try existence check again.')
                        remote_path_name = current_folder_path
                    else:
                        ftpScanner.logger.warning('Prepend folder_path_prefix, try existence check again.')
                        remote_path_name = folder_path_prefix + current_folder_path
                    ftpScanner.logger.info('Remote path exists YES? "%s"', remote_path_name)
                    path_exists, err_str = ftpScanner.remote_path_exists(remote_path_name)
                    
                if path_exists:
                    ftpScanner.logger.info('Remote path exists YES!: "%s"', remote_path_name)
                    ftpScanner.logger.info('Attempt download of files from remote path: "%s"', remote_path_name)
                    ftpScanner.get_remote_folders_files(remote_path_name=remote_path_name,
                                                        entity_path_name=remote_path_name,
                                                        target_path_name=output_path_name,
                                                        recursively=recurse_remote_folders,
                                                        ftp_conn=ftpScanner.ftp_conn,
                                                        close_connection=False,
                                                        remove_remote_file_on_download=remove_remote_file_on_download)
                else:
                    ftpScanner.logger.error('Remote path exists NOT! "%s"', remote_path_name)
                
        if ftpScanner.ftp_conn is not None:
            ftpScanner.logger.info('Closing FTP connection')
            ftpScanner.ftp_conn.close()
            ftpScanner.logger.info('Closed FTP connection')

        ftpScanner.logger.info('---------------------------------------')
        ftpScanner.logger.info('Sleeping for %d seconds, please wait...', scan_interval_seconds)
        ftpScanner.logger.info('=======================================')
        time.sleep(scan_interval_seconds)
        # break
