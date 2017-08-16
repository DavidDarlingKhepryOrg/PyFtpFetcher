"""
Created on Nov 3, 2015

@author: David
"""

from enum import IntEnum, unique
from h5py.h5g import UNKNOWN


@unique
class ArchiveTypeEnums(IntEnum):
    """
    Archive Type Enumerations
    """

    ZIP = 0,        # ZIP
    ZIPX = 1,       # ZIPX
    ZIP7 = 2,       # 7-ZIP
    TAR = 3,        # TAR
    UNKNOWN = 999   # Unknown


@unique
class ConfigOptionTypeEnum(IntEnum):
    """
    Configuration Option Type Enumerations
    """

    STRING = 0,
    BOOLEAN = 1,
    FLOAT = 2,
    INT = 3


@unique
class FileStatusEnum(IntEnum):
    """
    File Status Enumerations
    """

    FS_BAD_STATUS = -6
    FS_GOOD_STATUS = 0
    FS_FILE_LOADED = 1
    FS_FILENAME_VALIDATION_ERR = 6001
    FS_HEADER_VALIDATION_ERR = 6002
    FS_TRAILER_VALIDATION_ERR = 6003
    FS_COLUMN_COUNT_VALIDATION_ERR = 6004
    FS_RECORD_COUNT_VALIDATION_ERR = 6005
    FS_REHASHING_ERR = 6006
    FS_DATA_HEADER_ERR = 6007
    FS_DECRYPTION_ERR = 6008
    FS_SIGNATURE_ERR = 6009
    FS_FILE_EXISTS_ERR = 6010
    FS_HASH_COLUMN_ERR = 6011
    FS_ENCRYPTION_ERR = 6012
    FS_ARCHIVE_INVALID_ERR = 6013
    FS_DATA_DETAIL_ERR = 6014
    FS_OVERWRITING_ERR = 6015
    FS_MOVE_COPY_ERR = 6016
    FS_HDR_TLR_MISMATCH_ERR = 6017
    FS_INVALID_ROWTYPE_ERR = 6018
    FS_RDBMS_UPDATE_ERROR = 9010


@unique
class FtpLibNameEnum(IntEnum):
    """
    FtpLib Name Enumerations
    """

    FTPLIB = 0,
    FTPUTIL = 1,
    PARAMIKO = 2,
    PYSFTP = 3


@unique
class MsgqLibNameEnum(IntEnum):
    """
    MsgqLib Name Enumerations
    """

    PIKA = 0


@unique
class RdbmsTypeEnum(IntEnum):
    """
    RDBMS Type Enumerations
    """

    MSSQL = 1,
    MYSQL = 2,
    PGSQL = 3,
    SQLITE = 4


# execution guard on import
if __name__ == "__main__":
    pass
