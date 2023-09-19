import datetime
import os
import random
from abc import ABC, abstractmethod
from typing import Any, List

from AutoBackupPG.ds_database_backup.exceptions import DriveNotExist, MandatoryPropertiesNotPresent, \
    ArchiverNotFound, OneCDbNotFound, PgBaseBackupNotFound, PgDumpNotFound


class AbstractConfig(ABC):
    _label: str = ''

    def __getitem__(self, item):
        return getattr(self, item)

    def __init__(self, params: {str: Any}):
        self._set_params(params)
        mandatory_properties = self._mandatory_properties_for_check()

        self._check_mandatory_properties(mandatory_properties)

    def _set_params(self, params: {str: Any}):
        for key, value in params.items():
            try:
                set_method = self[f'set_{str.lower(key)}']
                set_method(value)
            except AttributeError:
                continue

    @abstractmethod
    def _mandatory_properties_for_check(self) -> List[str]:
        pass

    def _check_mandatory_properties(self, mandatory_properties: List[str]):
        failed_properties = []
        for prop in mandatory_properties:
            try:
                prop_val = self[prop.lower()]
                if prop_val is None or prop_val == '':
                    failed_properties.append(prop)
            except AttributeError:
                failed_properties.append(prop)

        if failed_properties:
            raise MandatoryPropertiesNotPresent(failed_properties)

    @staticmethod
    def _generate_label(use_millisec=False) -> str:
        if use_millisec:
            time_stamp = datetime.datetime.utcnow().strftime('%Y_%m_%d____%H-%M-%S.%f')[:-3]
        else:
            time_stamp = datetime.datetime.now().strftime('%Y_%m_%d____%H-%M-%S')

        return time_stamp + '_' + str(random.randint(1, 100))

    @staticmethod
    def _check_disk_for_parameter(path: str, parameter_name: str):
        root_path = os.path.abspath(path)
        root_drive, _ = os.path.splitdrive(root_path)
        if not os.path.exists(root_drive):
            raise DriveNotExist(parameter_name=parameter_name, path=path)

    def label(self) -> str:
        if self._label == '':
            self._label = self._generate_label()
        return self._label


class ConfigPgBaseBackuper(AbstractConfig):
    _path_to_backups: str = ''
    _custom_dir: str = ''

    _pg_basebackup: str = ''

    _postgresql_instance_path: str = ''
    _postgresql_username: str = ''
    _postgresql_password: str = ''
    _pg_port: str = ''

    _path_to_7zip: str = ''
    _temp_path: str = './temp'

    def __init__(self, params: {str: Any}):
        super(ConfigPgBaseBackuper, self).__init__(params)
        if not os.path.exists(self.pg_basebackup):
            raise PgBaseBackupNotFound(pg_basebackup_path=self.pg_basebackup,
                                       sql_instance_path=self.postgresql_instance_path)

    def _mandatory_properties_for_check(self) -> List[str]:
        return [
            'path_to_backups',
            'custom_dir',
            'postgresql_instance_path',
            'postgresql_username',
            'postgresql_password'
        ]

    @property
    def path_to_backups(self) -> str:
        return self._path_to_backups

    def set_path_to_backups(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_backups')
        self._path_to_backups = value

    @property
    def custom_dir(self) -> str:
        return self._custom_dir

    def set_custom_dir(self, value: str):
        self._custom_dir = value

    @property
    def pg_basebackup(self) -> str:
        if self._pg_basebackup == '':
            self._pg_basebackup = self.postgresql_instance_path + 'bin\\pg_basebackup.exe'
        return self._pg_basebackup

    def set_pg_basebackup(self, value: str):
        super()._check_disk_for_parameter(value, 'pg_basebackup')
        self._pg_basebackup = value

    @property
    def postgresql_instance_path(self) -> str:
        return self._postgresql_instance_path

    def set_postgresql_instance_path(self, value: str):
        super()._check_disk_for_parameter(value, 'postgresql_instance_path')
        self._postgresql_instance_path = value

    @property
    def postgresql_username(self) -> str:
        return self._postgresql_username

    def set_postgresql_username(self, value: str):
        self._postgresql_username = value

    @property
    def postgresql_password(self) -> str:
        return self._postgresql_password

    def set_postgresql_password(self, value: str):
        self._postgresql_password = value

    @property
    def pg_port(self) -> str:
        return self._pg_port

    def set_pg_port(self, value: str):
        self._pg_port = value

    @property
    def path_to_7zip(self) -> str:
        return self._path_to_7zip

    def set_path_to_7zip(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_7zip')
        value += '\\7za.exe'
        if not os.path.exists(value):
            raise ArchiverNotFound(value)
        self._path_to_7zip = value

    @property
    def temp_path(self) -> str:
        return self._temp_path

    def set_temp_path(self, value: str):
        super()._check_disk_for_parameter(value, 'temp_path')
        self._temp_path = value

    # Properties without class fields
    @property
    def backup_type_dir(self):
        return "Full"

    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self.custom_dir}\\{self.backup_type_dir}'

    @property
    def use_external_archiver(self) -> bool:
        return self._path_to_7zip is not ''


class ConfigPgDumpBackuper(AbstractConfig):
    _path_to_backups: str = ''
    _custom_dir: str = ''

    _database_name: str = ''
    _use_temp_dump: bool = False

    _postgresql_instance_path: str = ''
    _postgresql_username: str = ''
    _postgresql_password: str = ''
    _pg_port: str = ''

    _path_to_7zip: str = ''
    _temp_path: str = './temp'

    def __init__(self, params: {str: Any}):
        super(ConfigPgDumpBackuper, self).__init__(params)
        if not os.path.exists(self.pg_dump):
            raise PgDumpNotFound(pg_dump=self.pg_dump,
                                 sql_instance_path=self.postgresql_instance_path)

    def _mandatory_properties_for_check(self) -> List[str]:
        return [
            'path_to_backups',
            'custom_dir',
            'postgresql_instance_path',
            'postgresql_username',
            'postgresql_password'
        ]

    @property
    def path_to_backups(self) -> str:
        return self._path_to_backups

    def set_path_to_backups(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_backups')
        self._path_to_backups = value

    @property
    def custom_dir(self) -> str:
        return self._custom_dir

    def set_custom_dir(self, value: str):
        self._custom_dir = value

    @property
    def database_name(self) -> str:
        return self._database_name

    def set_database_name(self, value: str):
        self._database_name = value

    @property
    def use_temp_dump(self) -> bool:
        return self._use_temp_dump

    def set_use_temp_dump(self, value: bool):
        self._use_temp_dump = value

    @property
    def postgresql_instance_path(self) -> str:
        return self._postgresql_instance_path

    def set_postgresql_instance_path(self, value: str):
        super()._check_disk_for_parameter(value, 'postgresql_instance_path')
        self._postgresql_instance_path = value

    @property
    def postgresql_username(self) -> str:
        return self._postgresql_username

    def set_postgresql_username(self, value: str):
        self._postgresql_username = value

    @property
    def postgresql_password(self) -> str:
        return self._postgresql_password

    def set_postgresql_password(self, value: str):
        self._postgresql_password = value

    @property
    def pg_port(self) -> str:
        return self._pg_port

    def set_pg_port(self, value: str):
        self._pg_port = value

    @property
    def path_to_7zip(self) -> str:
        return self._path_to_7zip

    def set_path_to_7zip(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_7zip')
        value += '\\7za.exe'
        if not os.path.exists(value):
            raise ArchiverNotFound(value)
        self._path_to_7zip = value

    @property
    def temp_path(self) -> str:
        return self._temp_path

    def set_temp_path(self, value: str):
        super()._check_disk_for_parameter(value, 'temp_path')
        self._temp_path = value

    # Properties without class fields
    @property
    def backup_type_dir(self):
        return "Dumps"

    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self.custom_dir}\\{self.backup_type_dir}'

    @property
    def pg_dump(self):
        return self.postgresql_instance_path + 'bin\\pg_dump.exe'

    @property
    def pg_dumpall(self):
        return self.postgresql_instance_path + 'bin\\pg_dumpall.exe'

    @property
    def use_external_archiver(self) -> bool:
        return self._path_to_7zip is not ''


class Config1CFBBackuper(AbstractConfig):
    _path_to_backups: str = ''
    _custom_dir: str = ''

    _path_to_1c_db: str = ''
    _path_to_7zip: str = ''
    _backup_type_dir: str = 'OneC_file_bases'
    _cd_file_name: str = '1Cv8.1CD'

    def _mandatory_properties_for_check(self) -> List[str]:
        return [
            'path_to_backups',
            'custom_dir',
            'path_to_1c_db',
            'path_to_7zip',
        ]

    def set_path_to_backups(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_backups')
        value += self._cd_file_name
        if not os.path.exists(value):
            raise OneCDbNotFound(value)
        self._path_to_backups = value

    def set_custom_dir(self, value: str):
        self._custom_dir = value

    @property
    def path_to_1c_db(self) -> str:
        return self._path_to_1c_db

    def set_path_to_1c_db(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_1c_db')
        self._path_to_1c_db = value

    @property
    def path_to_7zip(self) -> str:
        return self._path_to_7zip

    def set_path_to_7zip(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_7zip')
        value += '\\7za.exe'
        if not os.path.exists(value):
            raise ArchiverNotFound(value)
        self._path_to_7zip = value

    # Properties without class fields
    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self._custom_dir}\\{self._backup_type_dir}'


class ConfigAWSClient(AbstractConfig):
    _path_to_backups: str = ''
    _custom_dir: str = ''

    _aws_access_key_id: str = ''
    _aws_secret_access_key: str = ''
    _aws_endpoint_url: str = 'https://storage.yandexcloud.net'
    _aws_bucket: str = ''
    _aws_chunk_size: int = 8388608
    _with_hash: bool = False

    _bandwidth_limit: int = 9 * 1000 * 1000 / 8  # Speed - 9Mbit/s
    _max_bandwidth_bytes: int = None
    _threshold_bandwidth: int = 500 * 1000 / 8

    @staticmethod
    def aws_correct_folder_name(dir_name: str) -> str:
        valid_characters = '0123456789qwertyuiopasdfghjklzxcvbnmйцукенгшщзхъфывапролджэячсмитьбюё'
        if dir_name[0].lower() not in valid_characters:
            dir_name = 'A' + dir_name
        return dir_name

    def _mandatory_properties_for_check(self) -> List[str]:
        return [
            'path_to_backups',
            'custom_dir',
            'aws_bucket',
            'aws_access_key_id',
            'aws_secret_access_key',
        ]

    @property
    def path_to_backups(self) -> str:
        return self._path_to_backups

    def set_path_to_backups(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_backups')
        self._path_to_backups = value

    @property
    def custom_dir(self) -> str:
        return self.aws_correct_folder_name(self._custom_dir)

    def set_custom_dir(self, value: str):
        self._custom_dir = value

    @property
    def access_key_id(self) -> str:
        return self._aws_access_key_id

    def set_aws_access_key_id(self, value: str):
        self._aws_access_key_id = value

    @property
    def secret_access_key(self) -> str:
        return self._aws_secret_access_key

    def set_aws_secret_access_key(self, value: str):
        self._aws_secret_access_key = value

    @property
    def endpoint_url(self) -> str:
        return self._aws_endpoint_url

    def set_aws_endpoint_url(self, value: str):
        self._aws_endpoint_url = value

    @property
    def bucket(self) -> str:
        return self._aws_bucket

    def set_aws_bucket(self, value: str):
        self._aws_bucket = value

    @property
    def chunk_size(self) -> int:
        return self._aws_chunk_size

    def set_aws_chunk_size(self, value: int):
        self._aws_chunk_size = value

    @property
    def with_hash(self) -> bool:
        return self._with_hash

    def set_with_hash(self, value: bool):
        self._with_hash = value

    @property
    def bandwidth_limit(self) -> int:
        return self._bandwidth_limit

    def set_bandwidth_limit(self, value: int):
        self._bandwidth_limit = value

    @property
    def max_bandwidth_bytes(self) -> int:
        return self._max_bandwidth_bytes

    def set_max_bandwidth_bytes(self, value: int):
        self._max_bandwidth_bytes = value

    @property
    def threshold_bandwidth(self) -> int:
        return self._threshold_bandwidth

    def set_threshold_bandwidth(self, value: int):
        self._threshold_bandwidth = value

    # Properties without class fields
    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self._custom_dir}'

    @property
    def path_to_backups_cloud(self) -> str:
        return f'/{self.custom_dir}'


class ConfigNonPgBaseCleaner(AbstractConfig):
    _path_to_backups: str = ''
    _custom_dir: str = ''

    _backups_leave_amount: int = 0
    _keep_one_backup_per_day: bool = True
    _storage_time: int = 99999999

    def _mandatory_properties_for_check(self) -> List[str]:
        return [
            'path_to_backups',
            'custom_dir',
            'storage_time',
        ]

    @property
    def path_to_backups(self) -> str:
        return self._path_to_backups

    def set_path_to_backups(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_backups')
        self._path_to_backups = value

    @property
    def custom_dir(self) -> str:
        return self._custom_dir

    def set_custom_dir(self, value: str):
        self._custom_dir = value

    @property
    def backups_leave_amount(self) -> int:
        return self._backups_leave_amount

    def set_backups_leave_amount(self, value: int):
        self._backups_leave_amount = value

    @property
    def keep_one_backup_per_day(self) -> bool:
        return self._keep_one_backup_per_day

    def set_keep_one_backup_per_day(self, value: bool):
        self._keep_one_backup_per_day = value

    @property
    def storage_time(self) -> int:
        return self._storage_time

    def set_storage_time(self, value: int):
        self._storage_time = value

    # Properties without class fields
    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self.custom_dir}'


class ConfigPgBaseCleaner(ConfigNonPgBaseCleaner):
    _path_to_wal_files: str = ''
    _use_simple_way_read_bck_date: bool = True

    @property
    def path_to_wal_files(self) -> str:
        return self._path_to_wal_files

    def set_path_to_wal_files(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_wal_files')
        self._path_to_wal_files = value

    @property
    def use_simple_way_read_bck_date(self) -> bool:
        return self._use_simple_way_read_bck_date

    def set_use_simple_way_read_bck_date(self, value: bool):
        self._use_simple_way_read_bck_date = value

    # Properties without class fields
    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self.custom_dir}'

    @property
    def handle_wal_files(self) -> bool:
        return self._path_to_wal_files is not ''
