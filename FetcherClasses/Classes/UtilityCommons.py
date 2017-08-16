import keyring
import os

from ensure import ensure_annotations

class DynamicUtilities:


    def create_path(self,
                    path_name: str,
                    path_fldr: str=None,
                    contains_file_name: bool=False,
                    logger=None,
                    is_test_mode: bool=False):

        err_str = None

        if logger is None:
            logger = self.logger

        path_name_expanded = self.expand_path(path_name, path_fldr, logger=logger)

        if contains_file_name:
            dirname = os.path.dirname(path_name_expanded)
        else:
            dirname = path_name_expanded

        # if folder name has a value
        # and the folder name not blank
        # and the folder does NOT yet exist
        if path_name_expanded != None \
        and path_name_expanded != "" \
        and not os.path.exists(dirname):
            # create the specified folder
            try:
                if contains_file_name:
                    if dirname != '':
                        if not os.path.exists(dirname):
                            os.makedirs(dirname)
                            logger.info('Path created: %s', dirname)
                else:
                    os.makedirs(dirname)
                    logger.info('Path created: %s', dirname)
            except Exception as err:
                err_str = str(err)
                logger.error('Path creation error: %s', path_name_expanded)
                logger.error(err_str)

        return dirname, err_str


    def expand_path(self,
                    path_name: str,
                    path_fldr: str=None,
                    path_suffix: str=None,
                    suffix_before_first_period: bool=True,
                    suffix_after_name_piece_nbr: int=None,
                    name_separator: str='_',
                    logger=None,
                    is_test_mode: bool=False):

        if logger is None:
            logger = self.logger

        # default the return value
        path_name_expanded = path_name

        if is_test_mode:
            logger.debug('-' * (len(path_suffix) if path_suffix is not None else 0 + 13))
            logger.debug('path_suffix: %s', path_suffix)
            logger.debug('suffix_before_first_period: %s', suffix_before_first_period)
            logger.debug('suffix_after_name_piece_nbr: %s', suffix_after_name_piece_nbr)
            logger.debug('name_separator: %s', name_separator)
            logger.debug('path_fldr_original: %s', path_fldr)
            logger.debug('path_base_original: %s', path_name)

        if suffix_after_name_piece_nbr is None or suffix_after_name_piece_nbr < 0:
            suffix_after_name_piece_nbr = 0

        if path_name is not None:
            # default the return value
            path_name_expanded = path_name

            # if it even has a value
            if path_name_expanded != None and path_name_expanded != '':

                # split the folder into its drive and tail
                drive, tail = os.path.splitdrive(path_name_expanded)

                # if it's a sub-folder
                if drive == '' and not (tail.startswith('~') or tail.startswith("/") or tail.startswith("\\\\")):
                    if path_fldr != None:
                        path_name_expanded = os.path.join(path_fldr, path_name_expanded)

                # if the home folder is specified
                if path_name_expanded.startswith("~"):
                    # expand the file path with the home folder
                    path_name_expanded = os.path.expanduser(path_name_expanded)

            # obtain the folder's absolute path
            path_name_expanded = os.path.abspath(path_name_expanded)

            if path_suffix != None:

                path_fldr = os.path.dirname(path_name_expanded)
                file_name = os.path.basename(path_name_expanded)

                first_period = file_name.find('.')
                prfx_name = file_name[:first_period]
                sufx_name = file_name[first_period:]
                if suffix_before_first_period:
                    path_name = prfx_name + path_suffix + sufx_name
                elif suffix_after_name_piece_nbr is not None:
                    pieces = prfx_name.split(name_separator)
                    prfx_name = name_separator.join(pieces[0:suffix_after_name_piece_nbr])
                    psfx_name = name_separator.join(pieces[suffix_after_name_piece_nbr:])
                    if prfx_name is '' and path_suffix.startswith(name_separator):
                        path_suffix = path_suffix[len(name_separator):]
                    if psfx_name != '':
                        psfx_name = name_separator + psfx_name
                    path_name = prfx_name + path_suffix + psfx_name + sufx_name
                else:
                    base_name, extension = os.path.splitext(file_name)
                    path_name = base_name + path_suffix + extension

                if path_fldr != None:
                    path_name_expanded = os.path.join(path_fldr, path_name)

            if is_test_mode:
                logger.debug('path_base_expanded: "%s"', os.path.basename(path_name_expanded))
                logger.debug('path_fldr_expanded: "%s"', os.path.dirname(path_name_expanded))
                logger.debug('path_name_returned: "%s"', path_name_expanded)
                logger.debug('-' * (len(path_name_expanded) if path_name_expanded is not None else 0 + 22))

        else:
            logger.error('path_name to be expanded is None')

        # return expanded folder path
        return path_name_expanded


    #===============================
    # Get password in user's keyring
    #===============================
    @ensure_annotations
    def get_pwd_via_keyring(self,
                            key: str,
                            login: str,
                            redact_passwords: bool=True,
                            log_results: bool=False,
                            logger=None,
                            is_test_mode: bool=False):

        err_str = None
        password = None

        if logger is None:
            logger = self.logger

        # get the password for the specified key and login
        try:
            password = keyring.get_password(key, login)
            if password != None:
                if log_results:
                    logger.info("SUCCESS: Retrieval of password for key: %s and login: %s", key, login)
                    if redact_passwords:
                        logger.info("Key: %s, Login: %s, Password: %s", key, login, '[redacted]')
                    else:
                        logger.info("Key: %s, Login: %s, Password: %s", key, login, password)
#                 if isTestMode:
#                     password = '[redacted]'
            else:
                err_str = "ERROR: key: %s for login: %s not in keyring" % (key, login)
                logger.error(err_str)
        except Exception as err:
            err_str = str(err)
            logger.error("FAILURE: Retrieval of password for key: %s and login: %s", key, login)
            logger.error(err_str)

        return password, err_str
