import datetime
import os
import random
from abc import ABC, abstractmethod
from typing import Any, List, Dict

from .exceptions import DriveNotExist, MandatoryPropertiesNotPresent, PathNotExist, \
    DrivesNotExist, ItsNotFile


class AbstractConfig(ABC):
    _label: str = ''
    backup_type_dirs = {
        'full': 'Full',
        'dumps': 'Dumps',
        'onec': 'OneC_file_bases',
        'mssql': 'MS_SQL'
    }
    backup_naming_separator: str = '_%!s!%_'
    default_temp_dir = 'temp_'

    def __getitem__(self, item):
        return getattr(self, item)

    def __init__(self, params: {str: Any}):
        self._set_params(params)

        mandatory_properties = self._mandatory_properties_for_check()
        self._check_mandatory_properties(mandatory_properties)

        paths_properties = self._paths_properties_for_check()
        self._check_root_drives_from_paths_properties(paths_properties)
        self._check_paths_properties(paths_properties)

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

    @abstractmethod
    def _paths_properties_for_check(self) -> List[Dict[str, bool]]:
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

    def _check_root_drives_from_paths_properties(self, paths_properties: List[Dict[str, bool]]):
        failed_properties = {}
        for pair in paths_properties:
            for prop, isMustBeFile in pair.items():
                try:
                    prop_val = self[prop.lower()]
                    if prop_val is not None and prop_val != '':
                        abc_path = os.path.abspath(prop_val)
                        root_drive, _ = os.path.splitdrive(abc_path)
                        if not os.path.exists(root_drive):
                            failed_properties.update({prop: prop_val})
                except AttributeError:
                    pass

        if failed_properties:
            raise DrivesNotExist(failed_properties)

    def _check_paths_properties(self, paths_properties:  List[Dict[str, bool]]):
        failed_paths = {}
        failed_files = {}

        for pair in paths_properties:
            for prop, isMustBeFile in pair.items():
                try:
                    prop_val = self[prop.lower()]
                    if prop_val is not None and prop_val != '':
                        abc_path = os.path.abspath(prop_val)
                        if not os.path.exists(abc_path):
                            failed_path = prop_val
                            if abc_path != prop_val:
                                failed_path = f'{prop_val}, полный путь - {abc_path}'
                            failed_paths.update({prop: failed_path})
                        elif isMustBeFile and not os.path.isfile(abc_path):
                            failed_path = prop_val
                            if abc_path != prop_val:
                                failed_path = f'{prop_val}, полный путь - {abc_path}'
                            failed_files.update({prop: failed_path})

                except AttributeError:
                    failed_paths.update({prop: ''})

        if failed_paths:
            raise PathNotExist(failed_paths)
        if failed_files:
            raise ItsNotFile(failed_files)

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

    @property
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
    _temp_path: str = ''

    def _mandatory_properties_for_check(self) -> List[str]:
        return [
            'path_to_backups',
            'custom_dir',
            'postgresql_instance_path',
            'postgresql_username',
            'postgresql_password'
        ]

    def _paths_properties_for_check(self) -> List[Dict[str, bool]]:
        return [
            {'postgresql_instance_path': False},
            {'pg_basebackup': True},
            {'path_to_7zip': True}
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
            self._pg_basebackup = self.postgresql_instance_path + '\\bin\\pg_basebackup.exe'
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
        self._postgresql_username = str(value)

    @property
    def postgresql_password(self) -> str:
        return self._postgresql_password

    def set_postgresql_password(self, value: str):
        self._postgresql_password = str(value)

    @property
    def pg_port(self) -> str:
        return self._pg_port

    def set_pg_port(self, value: str):
        self._pg_port = str(value)

    @property
    def path_to_7zip(self) -> str:
        return self._path_to_7zip

    def set_path_to_7zip(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_7zip')
        self._path_to_7zip = value + '\\7za.exe'

    @property
    def temp_path(self) -> str:
        temp_dir = f'temp\\{super(ConfigPgBaseBackuper, self).default_temp_dir}_{self.label}'
        val = f'{self.path_to_backups}\\{temp_dir}' if self._temp_path == '' else f'{self._temp_path}\\{temp_dir}'
        return val

    def set_temp_path(self, value: str):
        super()._check_disk_for_parameter(value, 'temp_path')
        self._temp_path = value

    # Properties without class fields
    @property
    def backup_type_dir(self):
        return super(ConfigPgBaseBackuper, self).backup_type_dirs.get('full')

    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self.custom_dir}\\{self.backup_type_dir}'

    @property
    def use_external_archiver(self) -> bool:
        return self._path_to_7zip != ''


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
    _temp_path: str = ''

    def _mandatory_properties_for_check(self) -> List[str]:
        return [
            'path_to_backups',
            'custom_dir',
            'postgresql_instance_path',
            'postgresql_username',
            'postgresql_password'
        ]

    def _paths_properties_for_check(self) -> List[Dict[str, bool]]:
        return [
            {'postgresql_instance_path': False},
            {'pg_dump': True},
            {'path_to_7zip': True}
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
        self._postgresql_username = str(value)

    @property
    def postgresql_password(self) -> str:
        return self._postgresql_password

    def set_postgresql_password(self, value: str):
        self._postgresql_password = str(value)

    @property
    def pg_port(self) -> str:
        return self._pg_port

    def set_pg_port(self, value: str):
        self._pg_port = str(value)

    @property
    def path_to_7zip(self) -> str:
        return self._path_to_7zip

    def set_path_to_7zip(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_7zip')
        self._path_to_7zip = value + '\\7za.exe'

    @property
    def temp_path(self) -> str:
        temp_dir = f'temp\\{super(ConfigPgDumpBackuper, self).default_temp_dir}_{self.label}'
        val = f'{self.path_to_backups}\\{temp_dir}' if self._temp_path == '' else f'{self._temp_path}\\{temp_dir}'
        return val

    def set_temp_path(self, value: str):
        super()._check_disk_for_parameter(value, 'temp_path')
        self._temp_path = value

    # Properties without class fields
    @property
    def backup_type_dir(self):
        return super(ConfigPgDumpBackuper, self).backup_type_dirs.get('dumps')

    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self.custom_dir}\\{self.backup_type_dir}'

    @property
    def pg_dump(self):
        return self.postgresql_instance_path + '\\bin\\pg_dump.exe'

    @property
    def pgsql(self):
        return self.postgresql_instance_path + '\\bin\\psql'

    @property
    def use_external_archiver(self) -> bool:
        return self._path_to_7zip != ''


class Config1CFBBackuper(AbstractConfig):
    _path_to_backups: str = ''
    _custom_dir: str = ''

    _path_to_1c_db_dir: str = ''
    _path_to_7zip: str = ''
    _cd_file_name: str = '1Cv8.1CD'

    def _mandatory_properties_for_check(self) -> List[str]:
        return [
            'path_to_backups',
            'custom_dir',
            'path_to_1c_db_dir',
            'path_to_7zip',
        ]

    def _paths_properties_for_check(self) -> List[Dict[str, bool]]:
        return [
            {'path_to_1c_db_dir': True},
            {'path_to_7zip': True}
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
    def path_to_1c_db_dir(self) -> str:
        return self._path_to_1c_db_dir

    def set_path_to_1c_db_dir(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_1c_db_dir')
        self._path_to_1c_db_dir = value + '\\' + self.cd_file_name

    @property
    def path_to_7zip(self) -> str:
        return self._path_to_7zip

    def set_path_to_7zip(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_7zip')
        self._path_to_7zip = value + '\\7za.exe'

    # Properties without class fields
    @property
    def cd_file_name(self) -> str:
        return self._cd_file_name

    @property
    def backup_type_dir(self):
        return super(Config1CFBBackuper, self).backup_type_dirs.get('onec')

    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self._custom_dir}\\{self.backup_type_dir}'


class ConfigMsSqlBackuper(AbstractConfig):
    _path_to_backups: str = ''
    _custom_dir: str = ''

    _database_name: str = ''

    _ms_sql_username: str = ''
    _ms_sql_password: str = ''

    _path_to_7zip: str = ''

    def _mandatory_properties_for_check(self) -> List[str]:
        return [
            'path_to_backups',
            'custom_dir',
            'database_name'
        ]

    def _paths_properties_for_check(self) -> List[Dict[str, bool]]:
        return [
            {'path_to_7zip': True}
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
        self._database_name = str(value)

    @property
    def ms_sql_username(self) -> str:
        return self._ms_sql_username

    def set_ms_sql_username(self, value: str):
        self._ms_sql_username = str(value)

    @property
    def ms_sql_password(self) -> str:
        return self._ms_sql_password

    def set_ms_sql_password(self, value: str):
        self._ms_sql_password = value

    @property
    def path_to_7zip(self) -> str:
        return self._path_to_7zip

    def set_path_to_7zip(self, value: str):
        value = os.path.abspath(value)
        super()._check_disk_for_parameter(value, 'path_to_7zip')
        self._path_to_7zip = value + '\\7za.exe'

    # Properties without class fields
    @property
    def backup_type_dir(self):
        return super(ConfigMsSqlBackuper, self).backup_type_dirs.get('mssql')

    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self.custom_dir}\\{self.backup_type_dir}'

    @property
    def use_external_archiver(self) -> bool:
        return self._path_to_7zip != ''


class ConfigAWSClient(AbstractConfig):
    _path_to_backups: str = ''
    _custom_dir: str = ''

    _access_key_id: str = ''
    _secret_access_key: str = ''
    _endpoint_url: str = 'https://storage.yandexcloud.net'
    _bucket: str = ''
    _chunk_size: int = 8388608
    _with_hash: bool = False

    _default_bandwidth_limit_bytes_for_auto_adjust: int = 9 * 125000  # Speed - 9Mbit/s
    _max_bandwidth_bytes: int = None
    _threshold_bandwidth_bytes: int = 500 * 125  # 500 Kbit/s
    _exclude_dirs: [str] = []

    def __init__(self, params: {str: Any}):
        super().__init__(params)
        self._paths_to_backups_for_sync: [str] = []
        self._prepare_paths_to_backups_for_sync()
        self._prepare_bandwidth()

    def _prepare_paths_to_backups_for_sync(self):
        contents = os.listdir(self.general_path_to_backups)
        directories = [d for d in contents if os.path.isdir(os.path.join(self.general_path_to_backups, d))]
        for sub_dir in directories:
            to_exclude = any(d.lower() == sub_dir.lower() for d in self._exclude_dirs)
            if not to_exclude:
                self._paths_to_backups_for_sync.append(f'{self.general_path_to_backups}\\{sub_dir}')

    def _prepare_bandwidth(self):
        if self.max_bandwidth_bytes is not None:
            self.set_default_bandwidth_limit_bytes_for_auto_adjust(self.max_bandwidth_bytes)

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
            'bucket',
            'access_key_id',
            'secret_access_key',
        ]

    def _paths_properties_for_check(self) -> List[Dict[str, bool]]:
        return [
            {'path_to_backups': False},
            {'general_path_to_backups': False}
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
        return self._access_key_id

    def set_access_key_id(self, value: str):
        self._access_key_id = value

    @property
    def secret_access_key(self) -> str:
        return self._secret_access_key

    def set_secret_access_key(self, value: str):
        self._secret_access_key = value

    @property
    def endpoint_url(self) -> str:
        return self._endpoint_url

    def set_endpoint_url(self, value: str):
        self._endpoint_url = value

    @property
    def bucket(self) -> str:
        return self._bucket

    def set_bucket(self, value: str):
        self._bucket = value

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    def set_chunk_size(self, value: int):
        self._chunk_size = value

    @property
    def with_hash(self) -> bool:
        return self._with_hash

    def set_with_hash(self, value: bool):
        self._with_hash = value

    @property
    def default_bandwidth_limit_bytes_for_auto_adjust(self) -> int:
        return self._default_bandwidth_limit_bytes_for_auto_adjust

    def set_default_bandwidth_limit_bytes_for_auto_adjust(self, value: int):
        self._default_bandwidth_limit_bytes_for_auto_adjust = round(value)

    @property
    def max_bandwidth_bytes(self) -> int:
        return self._max_bandwidth_bytes

    def set_max_bandwidth_bytes(self, value: int):
        self._max_bandwidth_bytes = round(value)

    @property
    def threshold_bandwidth_bytes(self) -> int:
        return self._threshold_bandwidth_bytes

    def set_threshold_bandwidth_bytes(self, value: int):
        self._threshold_bandwidth_bytes = round(value)

    def set_exclude_dirs(self, value: [str]):
        self._exclude_dirs = value

    @property
    def paths_to_backups_for_sync(self) -> [str]:
        return self._paths_to_backups_for_sync

    # Properties without class fields
    @property
    def general_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self._custom_dir}'

    @property
    def path_to_backups_cloud(self) -> str:
        return f'{self.custom_dir}'


class ConfigCleaner(AbstractConfig):
    _path_to_backups: str = ''
    _custom_dir: str = ''
    _path_to_wal_files: str = ''

    _backups_leave_amount: int = 0
    _keep_one_backup_per_day: bool = True
    _storage_time: int = 99999999
    _leave_only_last_full_pg_backup: bool = False

    def _mandatory_properties_for_check(self) -> List[str]:
        return [
            'path_to_backups',
            'custom_dir',
            'storage_time',
        ]

    def _paths_properties_for_check(self) -> List[Dict[str, bool]]:
        return [
            {'path_to_backups': False},
            {'full_path_to_backups': False},
            {'path_to_wal_files': False}
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
    def path_to_wal_files(self) -> str:
        return self._path_to_wal_files

    def set_path_to_wal_files(self, value: str):
        super()._check_disk_for_parameter(value, 'path_to_wal_files')
        self._path_to_wal_files = value

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

    # Грубая врезка. Надо подумать как встроить в архитектуру
    @property
    def leave_only_last_full_pg_backup(self) -> bool:
        return self._leave_only_last_full_pg_backup

    def set_leave_only_last_full_pg_backup(self, value: bool):
        self._leave_only_last_full_pg_backup = value

    # Properties without class fields
    @property
    def full_path_to_backups(self) -> str:
        return f'{self._path_to_backups}\\{self.custom_dir}'

    @property
    def handle_wal_files(self) -> bool:
        return self._path_to_wal_files != ''
