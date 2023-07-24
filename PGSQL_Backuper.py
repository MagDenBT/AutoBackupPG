import lzma
import boto3
import os
import shutil
import subprocess
import tarfile
import datetime
import random
import hashlib
import json
from dateutil import parser
import tzlocal
from boto3.s3.transfer import TransferConfig



class Func:

    @staticmethod
    def get_objects_list_on_disk(path, mask=None, or_second_mask=None, only_files=True):
        objects_list = []
        for root, dirs, files in os.walk(path):
            total_files_in_dir = len(files)
            current_amount_in_dir = 0
            temp = []
            for filename in files:
                current_amount_in_dir += 1
                if mask is not None:
                    if mask in filename:
                        objects_list.append(os.path.join(root, filename))
                    elif or_second_mask is not None:
                        if current_amount_in_dir == total_files_in_dir:
                            if len(temp) > 0:
                                if or_second_mask in filename:
                                    objects_list.append(os.path.join(root, filename))
                                for val in temp:
                                    if or_second_mask in val:
                                        objects_list.append(os.path.join(root, val))
                            elif or_second_mask in filename:
                                objects_list.append(os.path.join(root, filename))
                        else:
                            temp.append(filename)
                            continue
                    else:
                        continue
                else:
                    objects_list.append(os.path.join(root, filename))

            if not only_files:
                for _dir in dirs:
                    if mask is not None:
                        if mask not in _dir:
                            continue
                    objects_list.append(os.path.join(root, _dir))

        return objects_list

    @staticmethod
    def get_md5(file, chunk_size=None):
        if chunk_size is not None:
            md5s = []
            with open(file, 'rb') as fp:
                while True:
                    data = fp.read(chunk_size)
                    if not data:
                        break
                    md5s.append(hashlib.md5(data))

            if len(md5s) < 1:
                return '{}'.format(hashlib.md5().hexdigest())

            if len(md5s) == 1:
                return '{}'.format(md5s[0].hexdigest())

            digests = b''.join(m.digest() for m in md5s)
            digests_md5 = hashlib.md5(digests)
            return '{}-{}'.format(digests_md5.hexdigest(), len(md5s))

        hash_md5 = hashlib.md5()
        with open(file, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash_md5.update(chunk)
        _hash = hash_md5.hexdigest()
        return _hash

    @staticmethod
    def get_md5_aws(session_client, bucket_name, resource_name):

        try:
            md5sum = session_client.head_object(
                Bucket=bucket_name,
                Key=resource_name
            )['ETag'][1:-1]
        except:
            md5sum = None

        return md5sum

    @staticmethod
    def bucket_exists_and_accessible(s3_client, bucket_name):
        message = None

        from botocore.exceptions import (
            ConnectionClosedError,
        )

        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ConnectionClosedError as e:
            message = e.fmt
        except Exception as e:
            err_code = e.response.get('Error').get('Code')
            if err_code == '404':
                message = f'Bucket "{bucket_name}" does not exist'
            elif err_code == '403':
                message = f'Access to the bucket "{bucket_name}" is forbidden'
        return message

    @staticmethod
    def get_objects_on_aws(s3_client, bucket, verification_path, only_files=True, with_hash=True):
        result = []

        try:
            for obj in s3_client.list_objects_v2(Bucket=bucket, Prefix=verification_path)['Contents']:
                resource_name = obj['Key']
                if resource_name.endswith('/') and only_files:
                    continue
                if with_hash:
                    md5 = Func.get_md5_aws(s3_client, bucket, resource_name)
                    item = {'Hash': md5, 'Path': resource_name}
                else:
                    item = resource_name
                result.append(item)
        except Exception:
            a = 1
        return result

    @staticmethod
    def optimize_remove_list_dir(empty_dirs):
        empty_dirs = set(empty_dirs)
        result = empty_dirs.copy()
        for i, sought in enumerate(empty_dirs):
            for z, target in enumerate(empty_dirs):
                if z == i:
                    continue
                if target.startswith(sought):
                    try:
                        result.remove(target)
                    except KeyError:
                        continue
        return result

    @staticmethod
    def add_hashs_to_local_files(local_files: [], chunk_size):
        result = {}
        for file in local_files:
            result.update({file: Func.get_md5(file, chunk_size)})
        return result

    @staticmethod
    def contain_files(path):
        files = Func.get_objects_list_on_disk(path, only_files=True)
        return len(files) > 0

    @staticmethod
    def clear_dir(path):
        # clear the directory of any files
        if os.path.exists(path):
            for _obj in os.listdir(path):
                if os.path.exists(f'{path}\\{_obj}'):
                    os.remove(f'{path}\\{_obj}')


class Args(object):

    class DriveNotExistError(Exception):
        def __init__(self):
            super().__init__()


    _path_to_backups: str = None
    _custom_dir: str = None
    _database_name: str = None
    _use_temp_dump: bool = None
    _local_path_to_wal_files: str = None
    _handle_full_bcks: bool = None
    _pg_basebackup: str = None
    _use_simple_way_read_bck_date: bool = None
    _path_to_7zip: str = None

    _dump_leave_amount: int = None
    _full_bck_leave_amount: int = None
    _keep_one_dump_per_day: bool = None
    _keep_one_full_bck_per_day: bool = None

    _postgresql_isntance_path: str = None
    _postgresql_username: str = None
    _postgresql_password: str = None
    _pg_port: str = None

    _temp_path: str = None
    _storage_time: int = None
    _log_path: str = None

    _aws_access_key_id: str = None
    _aws_secret_access_key: str = None
    _aws_endpoint_url: str = None
    _aws_bucket: str = None
    _aws_chunk_size: int = None
    _with_hash: bool = None
    # Internal
    _label: str = None

    def __getitem__(self, item):
        return getattr(self, item)

    def __init__(self, args=None, create_backup=False, clean_backups=False, sync_backups=False):
        self.set_params(args, True)
        req_arg = self._get_args_for_check(create_backup=create_backup,
                                           clean_backups=clean_backups, sync_backups=sync_backups)
        self.check_params(req_arg)

    def set_params(self, args=None, in_lower_case=True):
        for key, value in args.items():
            key_mod = str.lower(key) if in_lower_case else key
            try:
                set_method = self[f'set_{key_mod}']
                set_method(value)
            except self.DriveNotExistError as e:
                raise Exception(f"Параметр {key} - Диск в '{value}' не существует")
            except:
                continue

    def check_params(self, req_arg):
        args = {}
        for arg in req_arg:
            arg_mod = str.lower(arg)
            get_method = self[arg_mod]
            args.update({arg: get_method})
        _error = False
        message = 'The following arguments are not filled in -'
        for arg, get_method in args.items():
            val = get_method()
            if val is None or val == '':
                _error = True
                message += arg + ', '

        if _error:
            raise Exception(message)

    def _get_args_for_check(self, create_backup=False,
                            clean_backups=False, sync_backups=False):
        req_args = [
            'path_to_backups',
            'custom_dir',
        ]
        if create_backup:
            req_args.extend([
                'postgresql_isntance_path',
                'postgresql_username',
                'postgresql_password'
            ])
        if clean_backups:
            req_args.extend([
                'storage_time',
            ])
        if sync_backups:
            req_args.extend([
                'aws_bucket',
                'aws_access_key_id',
                'aws_secret_access_key',
            ])
        return req_args

    @staticmethod
    def _generate_label(use_millisec=False):
        if use_millisec:
            time_stamp = datetime.datetime.utcnow().strftime('%Y_%m_%d____%H-%M-%S.%f')[:-3]
        else:
            time_stamp = datetime.datetime.now().strftime('%Y_%m_%d____%H-%M-%S')

        return time_stamp + '_' + str(random.randint(1, 100))

    def _disk_exists(self, path):
        root_path = os.path.abspath(path)
        root_drive, _ = os.path.splitdrive(root_path)
        return os.path.exists(root_drive)

    def with_hash(self):
        return self._with_hash

    def set_with_hash(self, val: bool):
        self._with_hash = val


    # Getters

    def aws_chunk_size(self):
        if self._aws_chunk_size is not int or self._aws_chunk_size < 1:
            return 8388608
        return self._aws_chunk_size

    def aws_bucket(self):
        return self._aws_bucket

    def aws_access_key_id(self):
        return self._aws_access_key_id

    def aws_secret_access_key(self):
        return self._aws_secret_access_key

    def aws_endpoint_url(self):
        if self._aws_endpoint_url is None or self._aws_endpoint_url == '':
            var = 'https://storage.yandexcloud.net'
            self._aws_endpoint_url = var
        return self._aws_endpoint_url

    def path_to_backups(self):
        if not self._path_to_backups.endswith('\\'):
            self._path_to_backups = self._path_to_backups + '\\'
        return self._path_to_backups

    def custom_dir(self, for_aws=False):
        if for_aws:
            return self.aws_correct_folder_name(self._custom_dir)
        else:
            return self._custom_dir

    def full_bp_dir(self, for_aws=False):
        ldir = "Full"
        if for_aws:
            return self.aws_correct_folder_name(ldir)
        else:
            return ldir

    def dump_dir(self, for_aws=False):
        ldir = "Dumps"
        if for_aws:
            return self.aws_correct_folder_name(ldir)
        else:
            return ldir

    def local_path_to_wal_files(self):
        return self._local_path_to_wal_files

    def postgresql_isntance_path(self):
        if not self._postgresql_isntance_path.endswith('\\'):
            self._postgresql_isntance_path = self._postgresql_isntance_path + '\\'
        return self._postgresql_isntance_path

    def postgresql_username(self):
        return self._postgresql_username

    def postgresql_password(self):
        return self._postgresql_password

    def database_name(self):
        return self._database_name

    def pg_basebackup(self):
        if self._pg_basebackup is None or self._pg_basebackup == '':
            self._pg_basebackup = self.postgresql_isntance_path() + 'bin\\pg_basebackup.exe'
        return self._pg_basebackup

    def pg_dump(self):
        return self.postgresql_isntance_path() + 'bin\\pg_dump.exe'

    def pg_dumpall(self):
        return self.postgresql_isntance_path() + 'bin\\pg_dumpall.exe'

    def temp_path(self):
        if self._temp_path is None or self._temp_path == '':
            self._temp_path = './temp'  # The path to the temporary directory for full backup
        return self._temp_path

    def log_path(self):
        if self._log_path is None or self._log_path == '':
            self._log_path = './PostgreSQLBackuperLogs'  # The path to the script logs
        return self._log_path

    def storage_time(self):
        return self._storage_time

    def path_to_dump_local(self):
        # The path to the permanent  directory for full backup
        return f'{self.path_to_backups()}{self.custom_dir()}\\{self.dump_dir()}'

    def full_path_to_full_backup_local(self):
        return f'{self.path_to_backups()}{self.custom_dir()}\\{self.full_bp_dir()}'

    def path_to_dump_cloud(self, for_aws=False):
        path = f'{self.custom_dir(for_aws)}/{self.dump_dir(for_aws)}'
        if not for_aws:
            path = f'/{path}'
        return path

    def path_to_full_backup_cloud(self, for_aws=False):
        path = f'{self.custom_dir(for_aws)}/{self.full_bp_dir(for_aws)}'
        if not for_aws:
            path = f'/{path}'
        return path

    def path_to_incr_backup_cloud(self, for_aws=False):
        incr_bp_dir = os.path.basename(self.local_path_to_wal_files())
        if for_aws:
            incr_bp_dir = self.aws_correct_folder_name(incr_bp_dir)
        path = f'{self.custom_dir(for_aws)}/{incr_bp_dir}'
        if not for_aws:
            path = f'/{path}'
        return path

    def label(self):
        if self._label is None or self._label == '':
            self._label = self._generate_label()
        return self._label

    def path_to_cloud_custom_dir(self, for_aws=False):
        path = self.custom_dir(for_aws)
        if not for_aws:
            path = f'/{path}'
        return path

    def full_bck_use_ext_archiver(self):
        return self._path_to_7zip is not None and self._path_to_7zip != ''

    def archive_dump(self):
        return self._path_to_7zip is not None and self._path_to_7zip != ''

    def path_to_7zip(self):
        return self._path_to_7zip

    def handle_full_bcks(self):
        return self._handle_full_bcks

    def handle_wal_files(self):
        return self.handle_full_bcks() and self._local_path_to_wal_files is not None and self._local_path_to_wal_files != ''

    def pg_port(self):
        return self._pg_port

    def use_simple_way_read_bck_date(self):
        return self._use_simple_way_read_bck_date

    def use_temp_dump(self):
        return self._use_temp_dump

    def dump_leave_amount(self):
        return self._dump_leave_amount

    def full_bck_leave_amount(self):
        return self._full_bck_leave_amount

    def keep_one_dump_per_day(self):
        return self._keep_one_dump_per_day

    def keep_one_full_bck_per_day(self):
        return self._keep_one_full_bck_per_day

    # Setters
    def set_aws_access_key_id(self, val: str):
        self._aws_access_key_id = str(val)

    def set_aws_secret_access_key(self, val: str):
        self._aws_secret_access_key = str(val)

    def set_aws_endpoint_url(self, val: str):
        self._aws_endpoint_url = str(val)

    def set_path_to_backups(self, val: str):
        if not self._disk_exists(val):
            raise self.DriveNotExistError()

        self._path_to_backups = str(val)

    def set_custom_dir(self, val: str):
        self._custom_dir = str(val)

    def set_local_path_to_wal_files(self, val: str):
        if not self._disk_exists(val):
            raise self.DriveNotExistError()

        self._local_path_to_wal_files = str(val)

    def set_postgresql_isntance_path(self, val: str):
        if not self._disk_exists(val):
            raise self.DriveNotExistError()

        self._postgresql_isntance_path = str(val)

    def set_postgresql_username(self, val: str):
        self._postgresql_username = str(val)

    def set_postgresql_password(self, val: str):
        self._postgresql_password = str(val)

    def set_database_name(self, val: str):
        self._postgresql_password = str(val)
        self._database_name = val

    def set_temp_path(self, val: str):
        if not self._disk_exists(val):
            raise self.DriveNotExistError()

        self._temp_path = str(val)

    def set_log_path(self, val: str):
        if not self._disk_exists(val):
            raise self.DriveNotExistError()

        self._log_path = str(val)

    def set_storage_time(self, val: int):
        self._storage_time = int(val)

    def set_aws_bucket(self, val: str):
        self._aws_bucket = str(val)

    def set_aws_chunk_size(self, val: int):
        self._aws_chunk_size = int(val)

    def set_path_to_7zip(self, val: str):
        if not self._disk_exists(val):
            raise self.DriveNotExistError()

        self._path_to_7zip = val

    def set_handle_full_bcks(self, val: bool):
        self._handle_full_bcks = val

    def set_pg_basebackup(self, val: str):
        if not self._disk_exists(val):
            raise self.DriveNotExistError()

        self._pg_basebackup = val

    def set_pg_port(self, val: str):
        self._pg_port = str(val)

    def set_use_simple_way_read_bck_date(self, val: bool):
        self._use_simple_way_read_bck_date = val

    def set_use_temp_dump(self, val: bool):
        self._use_temp_dump = val

    def set_dump_leave_amount(self, val: int):
        self._dump_leave_amount = val

    def set_full_bck_leave_amount(self, val: int):
        self._full_bck_leave_amount = val

    def aws_correct_folder_name(self, _dir: str):
        valid_characters = '0123456789qwertyuiopasdfghjklzxcvbnmйцукенгшщзхъфывапролджэячсмитьбюё'
        if _dir[0].lower() not in valid_characters:
            _dir = 'A' + _dir
        return _dir

    def set_keep_one_dump_per_day(self, val: bool):
        self._keep_one_dump_per_day = val

    def set_keep_one_full_bck_per_day(self, val: bool):
            self._keep_one_full_bck_per_day = val


class BaseBackuper:

    args: Args = None

    def __init__(self, args):
        self.args = args

    def create_backup(self):
        Func.clear_dir(self.args.temp_path())

        if not os.path.exists(self.args.pg_basebackup()):
            raise Exception(
                f'pg_basebackup по адресу {self.args.pg_basebackup()} не найден. Проверьте правльность пути до каталога сервера PosgtrSQL или pg_basebackup(если он задан отдельно). Текущий путь до сервера в скрипте - {self.args.postgresql_isntance_path()}')

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self.args.postgresql_password()
        use_ext_archiver = self.args.full_bck_use_ext_archiver()
        comm_args = [self.args.pg_basebackup(),
             '-D', self.args.temp_path(),
             '-X', 'fetch',
             '-F', 'tar',
             '--label', self.args.label(),
             '--no-password',
             '--username', self.args.postgresql_username(),
             ]

        if use_ext_archiver:
            arch = f'{self.args.path_to_7zip()}\\7za.exe'
            if not os.path.exists(arch):
                raise Exception(f'{arch} - архиватор не найден')
        else:
            comm_args.append('--gzip')
        if self.args.pg_port() is not None and self.args.pg_port() != '':
            comm_args.extend(['-p', self.args.pg_port()])

        process = subprocess.run(
            comm_args,
            stderr=subprocess.PIPE,
            env=my_env,
        )

        text_error = process.stderr.decode(errors='replace')
        if text_error == "":
            if use_ext_archiver:
                self._archive_with_external_tool()
            else:
                self._move_to_permanent_dir()
                Func.clear_dir(self.args.temp_path())
        else:
            raise Exception(text_error)

    def _archive_with_external_tool(self):
        label = self.args.label()
        comm_args = f'"{self.args.path_to_7zip()}\\7za.exe" a -ttar -so -sdel -an "{self.args.temp_path()}\\"* | "{self.args.path_to_7zip()}\\7za.exe" a -si "{self.args.full_path_to_full_backup_local()}\\{label}__base.txz"'

        try:
            subprocess.check_output(comm_args, stderr=subprocess.PIPE, shell=True)
        except subprocess.CalledProcessError as e:
            raise Exception(e.stderr.decode(errors='replace'))
        except Exception as e:
            raise e

    def _move_to_permanent_dir(self, create_subdir=True):
        label = self.args.label()
        files = Func.get_objects_list_on_disk(self.args.temp_path())
        target_dir = f'{self.args.full_path_to_full_backup_local()}\\{label}' if create_subdir else self.args.full_path_to_full_backup_local()

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        # move & rename
        for file in files:
            shutil.move(file,
                        f'{target_dir}\\{label}__{os.path.basename(file)}')


class DumpBackuper:

    args = None

    def __init__(self, args: Args):
        self.args = args

    def create_backup(self):

        if not os.path.exists(self.args.pg_dump()):
            raise Exception(
                f'pg_dump по адресу {self.args.pg_dump()} не найден. Проверьте правильность пути до каталога сервера PosgtrSQL или pg_dump(если он задан отдельно). Текущий путь до сервера в скрипте - {self.args.postgresql_isntance_path()}')

        all_bases = self.args.database_name() is None or self.args.database_name() == ""

        base_name = "all_bases" if all_bases else self.args.database_name()


        dump_name = f'{base_name}_{self.args.label()}.dump'
        dump_full_path = f'{self.args.path_to_dump_local()}\\{dump_name}'
        if not os.path.exists(self.args.path_to_dump_local()):
            os.makedirs(self.args.path_to_dump_local())

        if self.args.use_temp_dump():
            self._create_through_ROM(dump_full_path, all_bases)
        else:
            self._create_through_stdout(dump_full_path, all_bases)

    def _create_through_stdout(self, dump_full_path, all_bases: bool):
        archiver = None
        if self.args.archive_dump():
            archiver = f'{self.args.path_to_7zip()}\\7za.exe'
            if not os.path.exists(archiver):
                raise Exception(f'{archiver} - архиватор не найден')

        comm_args = self._all_bases_command_through_stdout(archiver, dump_full_path) if all_bases else self._specific_base_command_through_stdout(archiver, dump_full_path)
        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self.args.postgresql_password()
        try:
            output = subprocess.check_output(comm_args, stderr=subprocess.STDOUT, env=my_env, shell=True)
        except subprocess.CalledProcessError as e:
            raise Exception(e.output.decode(errors='replace'))
        except Exception as e:
            raise e
        output = output.decode(errors='replace')
        pg_error = output.splitlines()[0] if len(output.splitlines()) > 0 else ''
        if pg_error != '':
            raise Exception(pg_error)

    def _all_bases_command_through_stdout(self, archiver, dump_full_path):
        port_arg = ''
        if self.args.pg_port() is not None and self.args.pg_port() != '':
            port_arg = f' -p {self.args.pg_port()}'
        comm_args = f'"{self.args.pg_dumpall()}"{port_arg} -U {self.args.postgresql_username()}'
        if archiver is not None:
            comm_args = comm_args + f' | "{archiver}" a -si "{dump_full_path}.xz"'
        else:
            comm_args += f' > "{dump_full_path}"'
        return comm_args

    def _specific_base_command_through_stdout(self, archiver, dump_full_path):
        port_arg = ''
        if self.args.pg_port() is not None and self.args.pg_port() != '':
            port_arg = f' -p {self.args.pg_port()}'
        comm_args = f'"{self.args.pg_dump()}"{port_arg} -U {self.args.postgresql_username()} -Fc {self.args.database_name()}'
        if archiver is not None:
            comm_args = comm_args + f' | "{archiver}" a -si "{dump_full_path}.xz"'
        else:
            comm_args += f' > "{dump_full_path}"'
        return comm_args

    def _create_through_ROM(self, finish_dump_path, all_bases: bool):
        arch = f'{self.args.path_to_7zip()}\\7za.exe'
        if self.args.archive_dump():
            if not os.path.exists(arch):
                raise Exception(f'{arch} - архиватор не найден')
            if not os.path.exists(self.args.temp_path()):
                os.makedirs(self.args.temp_path())
            dump_full_path = f'{self.args.temp_path()}\\{os.path.basename(finish_dump_path)}'
        else:
            dump_full_path = finish_dump_path

        comm_args = self._all_bases_command_through_ROM(dump_full_path) if all_bases else self._specific_base_command_through_ROM(dump_full_path)

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self.args.postgresql_password()
        process = subprocess.run(
            comm_args,
            stderr=subprocess.PIPE,
            env=my_env,
        )

        text_error = process.stderr.decode(errors='replace')
        if text_error == "":
            if self.args.archive_dump():
                self._archive_with_external_tool(arch, dump_full_path, finish_dump_path)
        else:
            raise Exception(text_error)

    def _all_bases_command_through_ROM(self, dump_full_path):
        comm_args = [self.args.pg_dumpall(),
                     '-U', self.args.postgresql_username(),
                     '-f', dump_full_path
                     ]
        if self.args.pg_port() is not None and self.args.pg_port() != '':
            comm_args.insert(1, '-p')
            comm_args.insert(2, self.args.pg_port())
        return comm_args

    # noinspection PyPep8Naming
    def _specific_base_command_through_ROM(self, dump_full_path):
        comm_args = [self.args.pg_dump(),
                     '-U', self.args.postgresql_username(),
                     '-Fc',
                     '-f', dump_full_path,
                     self.args.database_name()
                     ]
        if self.args.pg_port() is not None and self.args.pg_port() != '':
            comm_args.insert(1, '-p')
            comm_args.insert(2, self.args.pg_port())
        return comm_args

    def _archive_with_external_tool(self, arch_path, source, target):
        comm_args = f'"{arch_path}" a -sdel "{target}.xz" "{source}"'
        try:
            subprocess.check_output(comm_args, stderr=subprocess.PIPE, shell=True)
        except subprocess.CalledProcessError as e:
            os.remove(source)
            raise Exception(e.stderr.decode(errors='replace'))
        except Exception as e:
            os.remove(source)
            raise e


class AWS_Connector:

    _aws_client = None
    _args: Args = None
    _cloud_backups = []


    def __init__(self, args: Args, max_bandwidth_bytes = None, threshold_bandwidth = 500*1000/8):
        self._args = args
        session = boto3.session.Session()
        self._aws_client = session.client(
            service_name='s3',
            endpoint_url=self._args.aws_endpoint_url(),
            aws_access_key_id=self._args.aws_access_key_id(),
            aws_secret_access_key=self._args.aws_secret_access_key()
        )
        self._max_bandwidth_bytes = max_bandwidth_bytes
        self._threshold_bandwidth = threshold_bandwidth

    def sync_with_cloud(self):
        bucket = self._args.aws_bucket()
        message = Func.bucket_exists_and_accessible(self._aws_client, bucket)
        if message is not None:
            raise Exception(message)

        local_cloud_paths = {}
        if Func.contain_files(self._args.path_to_dump_local()):
            local_cloud_paths.update({self._args.path_to_dump_local(): self._args.path_to_dump_cloud(for_aws=True)})
        if self._args.handle_full_bcks():
            if Func.contain_files(self._args.full_path_to_full_backup_local()):
                local_cloud_paths.update(
                    {self._args.full_path_to_full_backup_local(): self._args.path_to_full_backup_cloud(for_aws=True)})
        if self._args.handle_wal_files():
            if Func.contain_files(self._args.local_path_to_wal_files()):
                local_cloud_paths.update(
                    {self._args.local_path_to_wal_files(): self._args.path_to_incr_backup_cloud(for_aws=True)})

        with_hash = self._args.with_hash()
        all_cloud_backups = Func.get_objects_on_aws(self._aws_client, bucket, self._args.path_to_cloud_custom_dir(True), with_hash=with_hash)
        for local_path, cloud_path in local_cloud_paths.items():
            for cloud_backup in all_cloud_backups:
                if cloud_backup.startswith(cloud_path):
                    self._cloud_backups.append(cloud_backup)

        corrupt_files = self._get_corrupt_files(local_cloud_paths)
        if len(corrupt_files) == 0:
            self._clean_cloud(local_cloud_paths, with_hash)
        self._upload_to_cloud(local_cloud_paths, with_hash)
        if len(corrupt_files) > 0:
            raise Exception(
                f'Traces of a ransomware VIRUS may have been found. The following files have an unknown extension -{corrupt_files}')

    def _get_corrupt_files(self, local_cloud_paths:{}):
        loca_files = []
        for local_path, cloud_path in local_cloud_paths.items():
            loca_files.extend(Func.get_objects_list_on_disk(local_path, only_files=True))
        corrupt_files = []
        for file in loca_files:
            if not self._check_extension(file):
                corrupt_files.append(file)
        return corrupt_files

    def _check_extension(self, path: str):
        arr = os.path.splitext(path)
        exten = arr[len(arr) - 1]
        if exten != '' and exten not in self._get_valid_extensions():
            return False
        elif exten == '' and not path.endswith('backup_manifest'):
            try:
                numb = int('0x' + os.path.basename(path), base=16)
            except ValueError:
                return False
        return True

    def _get_valid_extensions(self):
        return ['.gz', '.xz', '.txz', '.backup', '.dump']

    def _upload_to_cloud(self, local_cloud_paths: {}, with_hash):
        to_upload = {}
        for local_path, cloud_path in local_cloud_paths.items():
            local_backups = Func.get_objects_list_on_disk(local_path)
            result = self._compute_files_to_upload(local_backups, local_path, cloud_path, with_hash)
            to_upload.update(result)

        if len(to_upload) == 0:
            return 'Нет новых файлов для выгрузки'

        for backup_local, savefile in to_upload.items():
                self._upload_file(backup_local, savefile)

        return ''

    def _upload_file(self, local_file, target_file, adjust_bandwidth=True):

        upload_config = TransferConfig(multipart_chunksize=self._args.aws_chunk_size(), max_bandwidth=self._max_bandwidth_bytes)

        from botocore.exceptions import (
            ConnectionClosedError,
            ConnectTimeoutError,
            ReadTimeoutError
        )

        try:
            self._aws_client.upload_file(local_file, self._args.aws_bucket(), target_file, Config=upload_config)
        except (
                ConnectionClosedError,
                ReadTimeoutError,
               ConnectTimeoutError,
        ) as e:
            new_max_bandwidth_bytes = self._get_bandwidth_limit() if self._max_bandwidth_bytes is None else self._max_bandwidth_bytes / 100 * 70

            if adjust_bandwidth:
                if self._threshold_bandwidth < new_max_bandwidth_bytes:
                    self._max_bandwidth_bytes = new_max_bandwidth_bytes
                    self._upload_file(local_file, target_file, True)
                else:
                    raise Exception(f"Нестабильный интернет. Автопонижение скорости выгрузки до {self._max_bandwidth_bytes / 125} Кбит/с не решило проблему")

    def _compute_files_to_upload(self, local_backups: [], root_local_path, path_cloud, with_hash=False):
        if with_hash:
            return self._compute_files_to_upload_with_hash(local_backups, root_local_path, path_cloud)
        else:
            return self._compute_files_to_upload_no_hash(local_backups, root_local_path, path_cloud)

    def _get_bandwidth_limit(self):
        return 9 * 1000 * 1000 / 8 #Speed - 9Mbit/s

    def _compute_files_to_upload_no_hash(self, local_backups: [], root_local_path, path_cloud):
        result = {}
        for l_backup in local_backups:
            _dir = os.path.dirname(l_backup)
            dir_name = os.path.basename(_dir)
            file_name_for_cloud = file_name = os.path.basename(l_backup)
            if not root_local_path.endswith(_dir):
                file_name_for_cloud = f'{self._args.aws_correct_folder_name(dir_name)}/{file_name}'
            l_add = True
            for cloud_backup in self._cloud_backups:
                if cloud_backup.endswith(file_name):
                    l_add = False
                    break
            if l_add:
                full_path_cloud = f'{path_cloud}/{file_name_for_cloud}'
                result.update({l_backup: full_path_cloud})
        return result

    def _compute_files_to_upload_with_hash(self, local_backups: [], root_local_path, path_cloud):
        result = {}
        for l_backup in local_backups:
            path_to_dir = os.path.dirname(l_backup)
            dir_name = os.path.basename(path_to_dir)
            file_name_for_cloud = file_name = os.path.basename(l_backup)
            if not root_local_path.endswith(path_to_dir):
                file_name_for_cloud = f'{self._args.aws_correct_folder_name(dir_name)}/{file_name}'

            md5_local = Func.get_md5(l_backup, self._args.aws_chunk_size())
            l_add = True
            for cloud_backup in self._cloud_backups:
                if cloud_backup['Path'].endswith(file_name) and md5_local == cloud_backup['Hash']:
                    l_add = False
                    break
            if l_add:
                full_path_cloud = f'{path_cloud}/{file_name_for_cloud}'
                result.update({l_backup: full_path_cloud})
        return result

    def _clean_cloud(self, local_cloud_paths, with_hash):
        extra_bck = self._get_extra_bck_on_cloud(local_cloud_paths, with_hash)
        if len(extra_bck) > 0:
            objects = []
            for bck in extra_bck:
                objects.append({'Key': bck})
            self._aws_client.delete_objects(Bucket=self._args.aws_bucket(), Delete={'Objects': objects})

    def _get_extra_bck_on_cloud(self, local_cloud_paths: {}, with_hash=False):
        if with_hash:
            return self._get_extra_bck_on_cloud_with_hash(local_cloud_paths)
        else:
            return self._get_extra_bck_on_cloud_no_hash(local_cloud_paths)

    def _get_extra_bck_on_cloud_no_hash(self, local_cloud_paths: {}):
        result = []
        loca_files = []
        for local_path, cloud_path in local_cloud_paths.items():
            loca_files.extend(Func.get_objects_list_on_disk(local_path, only_files=True))
        for cloud_file in self._cloud_backups:
            cloud_file_name = os.path.basename(cloud_file)
            to_delete = True
            for local_file in loca_files:
                if local_file.endswith(cloud_file_name):
                    to_delete = False
                    break
            if to_delete:
                result.append(cloud_file)

        return result

    def _get_extra_bck_on_cloud_with_hash(self, local_cloud_paths: {}):
        result = []
        loca_files = {}
        for local_path, cloud_path in local_cloud_paths.items():
            loca_files_pre = Func.get_objects_list_on_disk(local_path, only_files=True)
            loca_files.update(Func.add_hashs_to_local_files(loca_files_pre, self._args.aws_chunk_size()))
            for cloud_file in self._cloud_backups:
                cloud_file_name = os.path.basename(cloud_file['Path'])
                to_delete = True
                for local_file, local_file_hash in loca_files.items():
                    if local_file.endswith(cloud_file_name) and local_file_hash == cloud_file['Hash']:
                        to_delete = False
                        break
                if to_delete:
                    result.append(cloud_file['Path'])
        return result

    def _delete_empty_dirs_on_aws(self):
        verification_path = self._args.path_to_cloud_custom_dir(for_aws=True)
        empty_dirs = self._empty_aws_cloud_dirs(verification_path)

        try:
            empty_dirs.remove(verification_path + '/')
        except KeyError:
            a = 1

        empty_dirs = Func.optimize_remove_list_dir(empty_dirs)

        for_deletion = []
        for path in empty_dirs:
            for_deletion.append({'Key': path})

        self._aws_client.delete_objects(Bucket=self._args.aws_bucket(), Delete={'Objects': for_deletion})

    def _empty_aws_cloud_dirs(self, verification_path):
        obj_on_aws = Func.get_objects_on_aws(self._aws_client, self._args.aws_bucket(),
                                             verification_path, only_files=False, with_hash=False)
        obj_on_aws = set(obj_on_aws)
        result = obj_on_aws.copy()

        for i, path in enumerate(obj_on_aws):
            if not path.endswith('/'):  # is file
                result.remove(path)

                dirs = path.split('/')
                firm_path = ''
                for z, val in enumerate(dirs):
                    if z == len(dirs) - 1:
                        break
                    firm_path += f'{val}/'
                    try:
                        result.remove(firm_path)
                        continue
                    except KeyError:
                        continue
        return result


class LocalCleaner:
    whois_timezone_info = {}
    args: Args = None

    def __init__(self, args):
        self.args = args
        self.set_whois_timezone_info()

    def set_whois_timezone_info(self):
        self.whois_timezone_info = {
            "A": 1 * 3600,
            "ACDT": 10.5 * 3600,
            "ACST": 9.5 * 3600,
            "ACT": -5 * 3600,
            "ACWST": 8.75 * 3600,
            "ADT": 4 * 3600,
            "AEDT": 11 * 3600,
            "AEST": 10 * 3600,
            "AET": 10 * 3600,
            "AFT": 4.5 * 3600,
            "AKDT": -8 * 3600,
            "AKST": -9 * 3600,
            "ALMT": 6 * 3600,
            "AMST": -3 * 3600,
            "AMT": -4 * 3600,
            "ANAST": 12 * 3600,
            "ANAT": 12 * 3600,
            "AQTT": 5 * 3600,
            "ART": -3 * 3600,
            "AST": 3 * 3600,
            "AT": -4 * 3600,
            "AWDT": 9 * 3600,
            "AWST": 8 * 3600,
            "AZOST": 0 * 3600,
            "AZOT": -1 * 3600,
            "AZST": 5 * 3600,
            "AZT": 4 * 3600,
            "AoE": -12 * 3600,
            "B": 2 * 3600,
            "BNT": 8 * 3600,
            "BOT": -4 * 3600,
            "BRST": -2 * 3600,
            "BRT": -3 * 3600,
            "BST": 6 * 3600,
            "BTT": 6 * 3600,
            "C": 3 * 3600,
            "CAST": 8 * 3600,
            "CAT": 2 * 3600,
            "CCT": 6.5 * 3600,
            "CDT": -5 * 3600,
            "CEST": 2 * 3600,
            "CET": 1 * 3600,
            "CHADT": 13.75 * 3600,
            "CHAST": 12.75 * 3600,
            "CHOST": 9 * 3600,
            "CHOT": 8 * 3600,
            "CHUT": 10 * 3600,
            "CIDST": -4 * 3600,
            "CIST": -5 * 3600,
            "CKT": -10 * 3600,
            "CLST": -3 * 3600,
            "CLT": -4 * 3600,
            "COT": -5 * 3600,
            "CST": -6 * 3600,
            "CT": -6 * 3600,
            "CVT": -1 * 3600,
            "CXT": 7 * 3600,
            "ChST": 10 * 3600,
            "D": 4 * 3600,
            "DAVT": 7 * 3600,
            "DDUT": 10 * 3600,
            "E": 5 * 3600,
            "EASST": -5 * 3600,
            "EAST": -6 * 3600,
            "EAT": 3 * 3600,
            "ECT": -5 * 3600,
            "EDT": -4 * 3600,
            "EEST": 3 * 3600,
            "EET": 2 * 3600,
            "EGST": 0 * 3600,
            "EGT": -1 * 3600,
            "EST": -5 * 3600,
            "ET": -5 * 3600,
            "F": 6 * 3600,
            "FET": 3 * 3600,
            "FJST": 13 * 3600,
            "FJT": 12 * 3600,
            "FKST": -3 * 3600,
            "FKT": -4 * 3600,
            "FNT": -2 * 3600,
            "G": 7 * 3600,
            "GALT": -6 * 3600,
            "GAMT": -9 * 3600,
            "GET": 4 * 3600,
            "GFT": -3 * 3600,
            "GILT": 12 * 3600,
            "GMT": 0 * 3600,
            "GST": 4 * 3600,
            "GYT": -4 * 3600,
            "H": 8 * 3600,
            "HDT": -9 * 3600,
            "HKT": 8 * 3600,
            "HOVST": 8 * 3600,
            "HOVT": 7 * 3600,
            "HST": -10 * 3600,
            "I": 9 * 3600,
            "ICT": 7 * 3600,
            "IDT": 3 * 3600,
            "IOT": 6 * 3600,
            "IRDT": 4.5 * 3600,
            "IRKST": 9 * 3600,
            "IRKT": 8 * 3600,
            "IRST": 3.5 * 3600,
            "IST": 5.5 * 3600,
            "JST": 9 * 3600,
            "K": 10 * 3600,
            "KGT": 6 * 3600,
            "KOST": 11 * 3600,
            "KRAST": 8 * 3600,
            "KRAT": 7 * 3600,
            "KST": 9 * 3600,
            "KUYT": 4 * 3600,
            "L": 11 * 3600,
            "LHDT": 11 * 3600,
            "LHST": 10.5 * 3600,
            "LINT": 14 * 3600,
            "M": 12 * 3600,
            "MAGST": 12 * 3600,
            "MAGT": 11 * 3600,
            "MART": 9.5 * 3600,
            "MAWT": 5 * 3600,
            "MDT": -6 * 3600,
            "MHT": 12 * 3600,
            "MMT": 6.5 * 3600,
            "MSD": 4 * 3600,
            "MSK": 3 * 3600,
            "MST": -7 * 3600,
            "MT": -7 * 3600,
            "MUT": 4 * 3600,
            "MVT": 5 * 3600,
            "MYT": 8 * 3600,
            "N": -1 * 3600,
            "NCT": 11 * 3600,
            "NDT": 2.5 * 3600,
            "NFT": 11 * 3600,
            "NOVST": 7 * 3600,
            "NOVT": 7 * 3600,
            "NPT": 5.5 * 3600,
            "NRT": 12 * 3600,
            "NST": 3.5 * 3600,
            "NUT": -11 * 3600,
            "NZDT": 13 * 3600,
            "NZST": 12 * 3600,
            "O": -2 * 3600,
            "OMSST": 7 * 3600,
            "OMST": 6 * 3600,
            "ORAT": 5 * 3600,
            "P": -3 * 3600,
            "PDT": -7 * 3600,
            "PET": -5 * 3600,
            "PETST": 12 * 3600,
            "PETT": 12 * 3600,
            "PGT": 10 * 3600,
            "PHOT": 13 * 3600,
            "PHT": 8 * 3600,
            "PKT": 5 * 3600,
            "PMDT": -2 * 3600,
            "PMST": -3 * 3600,
            "PONT": 11 * 3600,
            "PST": -8 * 3600,
            "PT": -8 * 3600,
            "PWT": 9 * 3600,
            "PYST": -3 * 3600,
            "PYT": -4 * 3600,
            "Q": -4 * 3600,
            "QYZT": 6 * 3600,
            "R": -5 * 3600,
            "RET": 4 * 3600,
            "ROTT": -3 * 3600,
            "S": -6 * 3600,
            "SAKT": 11 * 3600,
            "SAMT": 4 * 3600,
            "SAST": 2 * 3600,
            "SBT": 11 * 3600,
            "SCT": 4 * 3600,
            "SGT": 8 * 3600,
            "SRET": 11 * 3600,
            "SRT": -3 * 3600,
            "SST": -11 * 3600,
            "SYOT": 3 * 3600,
            "T": -7 * 3600,
            "TAHT": -10 * 3600,
            "TFT": 5 * 3600,
            "TJT": 5 * 3600,
            "TKT": 13 * 3600,
            "TLT": 9 * 3600,
            "TMT": 5 * 3600,
            "TOST": 14 * 3600,
            "TOT": 13 * 3600,
            "TRT": 3 * 3600,
            "TVT": 12 * 3600,
            "U": -8 * 3600,
            "ULAST": 9 * 3600,
            "ULAT": 8 * 3600,
            "UTC": 0 * 3600,
            "UYST": -2 * 3600,
            "UYT": -3 * 3600,
            "UZT": 5 * 3600,
            "V": -9 * 3600,
            "VET": -4 * 3600,
            "VLAST": 11 * 3600,
            "VLAT": 10 * 3600,
            "VOST": 6 * 3600,
            "VUT": 11 * 3600,
            "W": -10 * 3600,
            "WAKT": 12 * 3600,
            "WARST": -3 * 3600,
            "WAST": 2 * 3600,
            "WAT": 1 * 3600,
            "WEST": 1 * 3600,
            "WET": 0 * 3600,
            "WFT": 12 * 3600,
            "WGST": -2 * 3600,
            "WGT": -3 * 3600,
            "WIB": 7 * 3600,
            "WIT": 9 * 3600,
            "WITA": 8 * 3600,
            "WST": 14 * 3600,
            "WT": 0 * 3600,
            "X": -11 * 3600,
            "Y": -12 * 3600,
            "YAKST": 10 * 3600,
            "YAKT": 9 * 3600,
            "YAPT": 10 * 3600,
            "YEKST": 6 * 3600,
            "YEKT": 5 * 3600,
            "Z": 0 * 3600,
        }

    class DefaultTimezone(datetime.tzinfo):
        def utcoffset(self, dt):
            return datetime.timedelta(hours=3)

        def dst(self, dt):
            return datetime.timedelta(0)

        def tzname(self, dt):
            return "Default Timezone"

    def __get_local_zone(self):
        try:
            localzone = tzlocal.get_localzone()
        except ValueError:
            localzone = self.DefaultTimezone()
        return localzone

    def _clean_local(self):
        storage_time = self.args.storage_time()
        expire_date = datetime.datetime.now(self.__get_local_zone()) - datetime.timedelta(seconds=storage_time)
        self.__clean_dumps(expire_date)
        if self.args.handle_full_bcks():
            self.__clean_full_bcks(expire_date)
        if self.args.handle_wal_files():
            self.__clean_WALs()

        self.__delete_local_empty_bck_dirs()

    def __clean_dumps(self, expire_date):
        dumps = Func.get_objects_list_on_disk(self.args.path_to_dump_local(), only_files=True)
        dic_backups = {}

        for backup in dumps:
            backup_date = datetime.datetime.fromtimestamp(os.path.getmtime(backup), self.__get_local_zone())
            dic_backups.update({backup: backup_date})

        dic_backups = dict(sorted(dic_backups.items(), key=lambda x: x[1]))

        items = list(dic_backups.items())
        i = 0
        suitable_backups = {}
        while i < len(items):
            backup, backup_date = items[i]

            if backup_date < expire_date:
                os.remove(backup)
                dic_backups.pop(backup)
            elif self.args.keep_one_dump_per_day():
                older_backup_per_day = next((key for key, value in suitable_backups.items() if value == backup_date.date()), None)

                if older_backup_per_day is not None:
                    os.remove(older_backup_per_day)
                    dic_backups.pop(older_backup_per_day)
                    suitable_backups.pop(older_backup_per_day)
                
                suitable_backups.update({backup: backup_date.date()})

            i += 1

        leave_amount = self.args.dump_leave_amount()
        if(isinstance(leave_amount, int) and leave_amount > -1):
            self.__dumps_leave_N_plus_1(dic_backups,leave_amount)

    def __dumps_leave_N_plus_1(self, sorted_backups: {}, leave_amount: int):
        i = 0
        items = list(sorted_backups.items())
        while i < len(items):
            backup, backup_date = items[i]
            if 0 < i < len(items) - leave_amount:
                os.remove(backup)
                sorted_backups.pop(backup)
            i += 1

    def __clean_full_bcks(self, expire_date):
        expired_backups = self.__get_expired_full_backups(expire_date)

        for backup in expired_backups:
            os.remove(backup)

        if self.args.keep_one_full_bck_per_day():
            self.__revome_extra_per_day_full_bcks()

        leave_amount = self.args.full_bck_leave_amount()
        if(isinstance(leave_amount, int) and leave_amount > -1):
            self.__full_bcks_leave_N_plus_1(leave_amount)

    def __revome_extra_per_day_full_bcks(self):
        backups = self.__get_full_backups_with_dates()
        backups_sorted_by_date = dict(sorted(backups.items(), key=lambda x: x[1]))

        items = list(backups_sorted_by_date.items())

        i = 0
        suitable_backups = {}

        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' not in backup:
                older_backup_per_day = next(
                    (key for key, value in suitable_backups.items() if value == backup_date.date()), None)

                if older_backup_per_day is not None:
                    os.remove(older_backup_per_day)
                    backups_sorted_by_date.pop(older_backup_per_day)
                    suitable_backups.pop(older_backup_per_day)

                suitable_backups.update({backup: backup_date.date()})
            i += 1

        self.delete_manifest_files_without_backup(backups_sorted_by_date)


    def __full_bcks_leave_N_plus_1(self, leave_amount: int):
        backups = self.__get_full_backups_with_dates()
        backups_sorted_by_date = dict(sorted(backups.items(), key=lambda x: x[1]))

        items = list(backups_sorted_by_date.items())
        i = 0
        total_backups = 0
        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' not in backup:
                total_backups += 1
            i += 1

        amount_to_delete = total_backups - leave_amount - 1 if total_backups >= leave_amount else 0
        items = list(backups_sorted_by_date.items())
        is_first = True
        i = 0
        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' not in backup:
                if is_first:
                    is_first = False
                elif amount_to_delete > 0:
                    os.remove(backup)
                    backups_sorted_by_date.pop(backup)
                    amount_to_delete -= 1
            i += 1

        self.delete_manifest_files_without_backup(backups_sorted_by_date)

    def __get_full_backups_with_dates(self):
        mask = '_backup_manifest'
        second_mask = '_base.'
        backups = Func.get_objects_list_on_disk(self.args.full_path_to_full_backup_local(), mask=mask,
                                                  or_second_mask=second_mask, only_files=True)
        result = {}
        for file in backups:
            file_name = os.path.basename(file)
            bck_date = self.__read_full_bck_create_date(file)
            if bck_date is not None:
                current_mask = mask if mask in file_name else second_mask
                portion = Func.get_objects_list_on_disk(os.path.dirname(file),
                                                        mask=file_name.split(current_mask)[0], only_files=True)
                for bck_file in portion:
                    result.update({bck_file: bck_date})
        return result

    def delete_manifest_files_without_backup(self, backups_sorted_by_date):
        items = list(backups_sorted_by_date.items())
        i = 0
        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' in backup:
                dir_path = os.path.dirname(backup)
                base_bcks = Func.get_objects_list_on_disk(dir_path, mask='_base', only_files=True)
                if len(base_bcks) == 0:
                    os.remove(backup)
                    backups_sorted_by_date.pop(backup)
            i += 1

    def __get_expired_full_backups(self, expire_date: datetime):
        mask = '_backup_manifest'
        second_mask = '_base.'
        full_bck = Func.get_objects_list_on_disk(self.args.full_path_to_full_backup_local(), mask=mask,
                                                 or_second_mask=second_mask, only_files=True)
        result = []

        for file in full_bck:
            file_name = os.path.basename(file)
            bck_date = self.__read_full_bck_create_date(file)

            if bck_date is not None and bck_date < expire_date:
                current_mask = mask if mask in file_name else second_mask
                portion_if_has_subdir = Func.get_objects_list_on_disk(os.path.dirname(file),
                                                        mask=file_name.split(current_mask)[0], only_files=True)
                result.extend(portion_if_has_subdir)

        return list(set(result))

    def __read_full_bck_create_date(self, backup: str):
        if(self.args.use_simple_way_read_bck_date()):
            return datetime.datetime.fromtimestamp(os.path.getmtime(backup), self.__get_local_zone())

        date_str = None
        try:

            if not tarfile.is_tarfile(backup):
                if backup.endswith('backup_manifest'):
                    with open(backup) as json_file:
                        data = json.load(json_file)
                        for p in data['Files']:
                            if p['Path'] == 'backup_label':
                                date_str = p['Last-Modified']
                                break
                    json_file.close()
            else:
                tar = tarfile.open(backup, "r")
                members = tar.getmembers()
                if len(members) == 1:
                    if members[0].isfile():
                        tar = tar.extractfile(members[0])
                    ex_file = tar.extractfile('backup_label')
                    date_str = self.read_time_from_backup_label(ex_file)
                elif len(members) == 2:
                    for member in members:
                        if member.name.endswith('backup_manifest'):
                            ex_file = tar.extractfile(member.name)
                            data = json.load(ex_file)
                            for p in data['Files']:
                                if p['Path'] == 'backup_label':
                                    date_str = p['Last-Modified']
                                    break
                            ex_file.close()
                            break
                else:
                    ex_file = tar.extractfile('backup_label')
                    date_str = self.read_time_from_backup_label(ex_file)
        except:
            date_str = None

        if date_str is None:
            return None

        tz = date_str.split(' ')[2]
        tzinfos = ''
        for item in self.whois_timezone_info.items():
            if item[0] == tz:
                tzinfos = {item[0]: item[1]}
                break
        result = parser.parse(date_str, tzinfos=tzinfos)
        return result

    def read_time_from_backup_label(self, file_obj):
        date_str = None
        while True:
            # считываем строку
            line = file_obj.readline()
            # прерываем цикл, если строка пустая
            if not line:
                break
            # выводим строку
            text = str(line.strip())
            if 'START TIME:' in text:
                date_str = text.split('START TIME:')[1]
                date_str = date_str.replace('\'', '')
                date_str = date_str.replace(' ', '', 1)
                break

            # закрываем файл
        file_obj.close()
        return date_str

    def __clean_WALs(self):
        wals = Func.get_objects_list_on_disk(self.args.local_path_to_wal_files(), only_files=True)
        if len(wals) == 0:
            return

        mask = '_backup_manifest'
        second_mask = '_base.'
        full_bck = Func.get_objects_list_on_disk(self.args.full_path_to_full_backup_local(), mask=mask,
                                                 or_second_mask=second_mask, only_files=True)
        oldest_date = datetime.datetime.now(self.__get_local_zone())
        oldest_label = None
        for file in full_bck:
            file_name = os.path.basename(file)
            bck_date = self.__read_full_bck_create_date(file)
            if bck_date is not None and oldest_date >= bck_date:
                oldest_date = bck_date
                if mask in file_name:
                    current_mask = mask
                else:
                    current_mask = second_mask

                oldest_label = file_name.split(current_mask)[0]
        if oldest_label is not None:
            wals = self.__WALs_to_remove(self.args.local_path_to_wal_files(), oldest_label)
            for wal in wals:
                os.remove(wal)

    def __WALs_to_remove(self, path, oldest_label, delete_unsuitable=False):
        oldest_number = self.__WAL_decimal_number(path, oldest_label)
        to_remove = []
        if oldest_number is None:
            return to_remove

        for root, dirs, files in os.walk(path):
            for file in files:
                filename = file
                if '.' in filename:
                    filename = filename.split('.')[0]
                filename = '0x' + filename
                try:
                    cur_number = int(filename, base=16)
                    if cur_number < oldest_number:
                        to_remove.append(os.path.join(root, file))
                except Exception:
                    if delete_unsuitable:
                        to_remove.append(os.path.join(root, file))

        return to_remove

    def __WAL_decimal_number(self, path, label):
        result = None
        for root, dirs, files in os.walk(path):
            for file in files:
                if result is not None:
                    break
                if '.backup' not in file:
                    continue
                try:
                    if file.endswith('.backup'):
                        content = open(os.path.join(root, file))
                    else:
                        content = lzma.open(os.path.join(root, file), "rt")

                    temp_res = ''
                    for line in content:
                        if 'START WAL LOCATION' in line:
                            temp_res = line.split('file ')[1]
                        if label in line:
                            if ')' in temp_res:
                                temp_res = temp_res.split(')')[0]
                            result = '0x' + temp_res
                            break
                    content.close()

                except Exception as e:
                    print("TROUBLE - " + str(e))
                    continue

        if result is not None:
            result = int(result, base=16)
        return result

    def __delete_local_empty_bck_dirs(self):
        arr = [f'{self.args.path_to_backups()}{self.args.custom_dir()}']
        if self.args.handle_wal_files():
            arr.append(self.args.local_path_to_wal_files())

        for path in arr:
            if os.path.exists(path):
                for root, dirs, files in os.walk(path):
                    for _dir in dirs:
                        dir_path = os.path.join(root, _dir)
                        self.__safely_delete_dir(dir_path)

    def __safely_delete_dir(self, dir_path):
        delete_it = True
        for root, dirs, files in os.walk(dir_path):
            for _dir in dirs:
                delete_it = self.__safely_delete_dir(os.path.join(root, _dir))
            for filename in files:
                delete_it = False
                break
        if delete_it:
            os.rmdir(dir_path)
        return delete_it


class Manager:

    _connector = None
    _backupers = {}
    _cleaner = None
    _args = None
    _aws_client = None

    def __init__(self, new_args=None, create_backup=False,
                 clean_backups=False, sync_backups=False):
        try:
            self._args = Args(args=new_args, create_backup=create_backup, clean_backups=clean_backups,
                              sync_backups=sync_backups)
        except Exception as e:
            self.write_log('PGSQL_BackuperInit-', False, str(e))
            raise e

        if create_backup:
            self._backupers.update({'pg_dump': DumpBackuper(self._args)})
            if self._args.handle_full_bcks():
                self._backupers.update({'pg_basebackup': BaseBackuper(self._args)})

        if sync_backups:
                self._connector = AWS_Connector(self._args)
        if clean_backups:
            self._cleaner = LocalCleaner(self._args)

    def clean_backups(self, write_to_log_file=True, raise_exception=False):
        try:
            self._cleaner._clean_local()
            if write_to_log_file:
                self.write_log('cleaning-', True, '')
        except Exception as e:
            if write_to_log_file:
                self.write_log('cleaning-', False, str(e))
            if raise_exception:
                raise e

    def create_backup(self, write_to_log_file=True, raise_exception=False):
        exceptions = []

        for name, backuper in self._backupers.items():
            try:
                backuper.create_backup()
                if write_to_log_file:
                    self.write_log(f'{name}-', True, '')
            except Exception as e:
                exceptions.append(str(e))
                if write_to_log_file:
                    self.write_log(f'{name}-', False, str(e))

        if raise_exception and len(exceptions) > 0:
            raise Exception('\n'.join(exceptions))

    def sync_with_cloud(self, write_to_log_file=True, raise_exception=False):
        try:
            message = self._connector.sync_with_cloud()
            if write_to_log_file:
                self.write_log('syncr-', True, str(message))
        except Exception as e:
            if write_to_log_file:
                self.write_log('syncr-', False, str(e))
            if raise_exception:
                raise e

    def main(self):
        self.create_backup()
        self.sync_with_cloud()
        self.clean_backups()

    def write_log(self, file_pref, success, text=''):

        default_path = './PostgreSQLBackuperLogs'
        try:
            path = self._args.log_path()
            if path == '' or path is None:
                path = default_path
        except:
            path = default_path

        if not os.path.exists(path):
            os.makedirs(path)

        if self._args is None:
            label = Args._generate_label(use_millisec=True)
        else:
            label = self._args.label()
        result = "Success_" if success else 'FAIL_'
        path = f'{path}\\{file_pref}{result}{label}.txt'
        file = open(path, "w", encoding="utf-8")
        file.write(text)
        file.close()

    def backupers(self):
        return self._backupers

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    m = Manager()
    m.main()
