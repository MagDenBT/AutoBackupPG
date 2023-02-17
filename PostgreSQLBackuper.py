# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import boto3

import os
import pathlib
import shutil
import subprocess
import tarfile
import traceback
import datetime
import random
import requests as requests
import hashlib
import json
from dateutil import parser
import tzlocal


class Func:

    @staticmethod
    def get_objects_list_on_disk(path, mask=None, second_mask=None, only_files=True):
        objects_list = []
        for root, dirs, files in os.walk(path):
            total_files = len(files)
            i = 0
            temp = []
            for filename in files:
                i += 1
                if mask is not None:
                    if mask in filename:
                        objects_list.append(os.path.join(root, filename))
                        i = total_files + 1
                    else:
                        if second_mask is not None:
                            if i == total_files:
                                if len(temp) > 0:
                                    for val in temp:
                                        if second_mask in val:
                                            objects_list.append(os.path.join(root, val))
                                elif second_mask in filename:
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
        try:
            s3_client.head_bucket(Bucket=bucket_name)
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
            for obj in s3_client.list_objects(Bucket=bucket)['Contents']:
                resource_name = obj['Key']
                if resource_name.endswith('/') and only_files:
                    continue
                if resource_name.startswith(verification_path):
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


class Args(object):
    # Paths settings
    """
            The directory structure is as follows:
            On the local machine - disk:\rootDir\customDir\fullBpDir and for WAL-files - the one you specify
            In the cloud - /rootDir/customDir/fullBpDir and
                    for WAL-files - /rootDir/customDir/"LastDirectoryIn(localPathToWALFiles)"
            """
    __cloud_token: str = None
    __disk: str = None
    __root_dir: str = None
    __custom_dir: str = None
    __full_bp_dir: str = None
    __local_path_to_wal_files: str = None
    __postgresql_isntance_path: str = None
    __postgresql_username: str = None
    __postgresql_password: str = None
    __backuper: str = None
    __temp_path: str = None
    __storage_time: int = None
    __log_path: str = None
    __label: str = None
    __url: str = None
    __aws_access_key_id: str = None
    __aws_secret_access_key: str = None
    __aws_endpoint_url: str = None
    __aws_bucket: str = None
    __aws_chunk_size: int = None

    def __getitem__(self, item):
        return getattr(self, item)

    def __init__(self, use_backuper, use_cleaner, use_yandex, args=None, in_lower_case=True):
        self.set_params(args, in_lower_case)
        self.check_params(use_backuper, use_cleaner, use_yandex,)

    def set_params(self, args=None, in_lower_case=True):
        if args is not None:
            for key, value in args.items():
                key_mod = str.lower(key) if in_lower_case else key
                try:
                    set_method = self[f'set_{key_mod}']
                    set_method(value)
                except:
                    continue
        else:
            self.__set_default_params()

    def check_params(self, use_backuper, use_cleaner, use_yandex):
        args = {}
        req_arg = self.__get_args_for_check(use_backuper, use_cleaner, use_yandex)
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

    def __get_args_for_check(self, use_backuper, use_cleaner, use_yandex):
        req_args = [
            'disk',
            'root_dir',
            'custom_dir',
            'full_bp_dir',
            'local_path_to_wal_files'
        ]
        if use_backuper:
            req_args.extend([
                'postgresql_isntance_path',
                'postgresql_username',
                'postgresql_password'
            ])
        if use_cleaner:
            req_args.extend([
                'storage_time'
            ])
        if use_yandex:
            req_args.extend([
                'cloud_token',
                'url'
            ])
        else:
            req_args.extend([
                'aws_bucket',
                'aws_access_key_id',
                'aws_secret_access_key',
            ])
        return req_args

    def __set_default_params(self):
        # Paths settings
        """
                The directory structure is as follows:
                On the local machine - disk:\rootDir\customDir\fullBpDir and for WAL-files - the one you specify
                In the cloud - /rootDir/customDir/fullBpDir and
                    for WAL-files - /rootDir/customDir/"LastDirectoryIn(localPathToWALFiles)"
        """
        self.__disk = 'C'
        self.__root_dir = 'Postgresql backups'
        self.__custom_dir = 'Accounting department'
        self.__full_bp_dir = 'Full'
        self.__local_path_to_wal_files = 'C:\\pg_log_archive'  # The path of the WAL files.(aka Incremental backups)
        self.__postgresql_isntance_path = 'C:\\Program Files\\PostgreSQL\\14.4-1.1C\\'  # The path to PostgreSQL
        self.__postgresql_username = 'postgres'
        self.__postgresql_password = '1122'

    @staticmethod
    def _generate_label(use_millisec=False):
        if use_millisec:
            time_stamp = datetime.datetime.utcnow().strftime('%Y_%m_%d____%H-%M-%S.%f')[:-3]
        else:
            time_stamp = datetime.datetime.now().strftime('%Y_%m_%d____%H-%M-%S')

        return time_stamp + '_' + str(random.randint(1, 100))

    # Getters

    def aws_chunk_size(self):
        if self.__aws_chunk_size is not int or self.__aws_chunk_size < 1:
            return 8388608
        return self.__aws_chunk_size

    def aws_bucket(self):
        return self.__aws_bucket

    def aws_access_key_id(self):
        return self.__aws_access_key_id

    def aws_secret_access_key(self):
        return self.__aws_secret_access_key

    def aws_endpoint_url(self):
        if self.__aws_endpoint_url is None or self.__aws_endpoint_url == '':
            var = 'https://storage.yandexcloud.net'
            self.__aws_endpoint_url = var
        return self.__aws_endpoint_url

    def cloud_token(self):
        return self.__cloud_token

    def disk(self):
        return self.__disk

    def root_dir(self, for_aws=False):
        if for_aws:
            return self.aws_correct_folder_name(self.__root_dir)
        else:
            return self.__root_dir

    def custom_dir(self, for_aws=False):
        if for_aws:
            return self.aws_correct_folder_name(self.__custom_dir)
        else:
            return self.__custom_dir

    def full_bp_dir(self, for_aws=False):
        if for_aws:
            return self.aws_correct_folder_name(self.__full_bp_dir)
        else:
            return self.__full_bp_dir

    def local_path_to_wal_files(self):
        return self.__local_path_to_wal_files

    def postgresql_isntance_path(self):
        return self.__postgresql_isntance_path

    def postgresql_username(self):
        return self.__postgresql_username

    def postgresql_password(self):
        return self.__postgresql_password

    def backuper(self):
        if self.__backuper is None or self.__backuper == '':
            path = self.postgresql_isntance_path()
            if not path.endswith('\\'):
                path = path + '\\'
            self.__backuper = path + 'bin\\pg_basebackup.exe'
        return self.__backuper

    def temp_path(self):
        if self.__temp_path is None or self.__temp_path == '':
            self.__temp_path = './temp'  # The path to the temporary directory for full backup
        return self.__temp_path

    def log_path(self):
        if self.__log_path is None or self.__log_path == '':
            self.__log_path = './PostgreSQLBackuperLogs'  # The path to the script logs
        return self.__log_path

    def storage_time(self):
        return self.__storage_time

    def headers(self):
        return {'Content-Type': 'application/json', 'Accept': 'application/json',
                'Authorization': f'OAuth {self.cloud_token()}'}

    def path_to_full_backup_local(self):
        # The path to the permanent  directory for full backup
        return f'{self.disk()}:\\{self.root_dir()}\\{self.custom_dir()}\\{self.full_bp_dir()}'

    def path_to_full_backup_cloud(self, for_aws=False):
        path = f'{self.root_dir(for_aws)}/{self.custom_dir(for_aws)}/{self.full_bp_dir(for_aws)}'
        if not for_aws:
            path = f'/{path}'
        return path

    def path_to_incr_backup_cloud(self, for_aws=False):
        incr_bp_dir = os.path.basename(self.local_path_to_wal_files())
        if for_aws:
            incr_bp_dir = self.aws_correct_folder_name(incr_bp_dir)
        path = f'{self.root_dir(for_aws)}/{self.custom_dir(for_aws)}/{incr_bp_dir}'
        if not for_aws:
            path = f'/{path}'
        return path

    def label(self):
        if self.__label is None or self.__label == '':
            self.__label = self._generate_label()
        return self.__label

    def url(self):
        if self.__url is None or self.__url == '':
            var = 'https://cloud-api.yandex.net/v1/disk/resources'
            self.__url = var
        return self.__url

    def path_to_cloud_custom_dir(self, for_aws=False):
        path = f'{self.root_dir(for_aws)}/{self.custom_dir(for_aws)}'
        if not for_aws:
            path = f'/{path}'
        return path

    # Setters
    def set_aws_access_key_id(self, val: str):
        self.__aws_access_key_id = str(val)

    def set_aws_secret_access_key(self, val: str):
        self.__aws_secret_access_key = str(val)

    def set_aws_endpoint_url(self, val: str):
        self.__aws_endpoint_url = str(val)

    def set_disk(self, val: str):
        self.__disk = str(val)

    def set_root_dir(self, val: str):
        self.__root_dir = str(val)

    def set_custom_dir(self, val: str):
        self.__custom_dir = str(val)

    def set_full_bp_dir(self, val: str):
        self.__full_bp_dir = str(val)

    def set_local_path_to_wal_files(self, val: str):
        self.__local_path_to_wal_files = str(val)

    def set_postgresql_isntance_path(self, val: str):
        self.__postgresql_isntance_path = str(val)

    def set_postgresql_username(self, val: str):
        self.__postgresql_username = str(val)

    def set_postgresql_password(self, val: str):
        self.__postgresql_password = str(val)

    def set_cloud_token(self, val: str):
        self.__cloud_token = str(val)

    def set_temp_path(self, val: str):
        self.__temp_path = str(val)

    def set_log_path(self, val: str):
        self.__log_path = str(val)

    def set_backuper(self, val: str):
        self.__backuper = str(val)

    def set_storage_time(self, val: int):
        self.__storage_time = int(val)

    def set_url(self, val: str):
        self.__url = str(val)

    def set_aws_bucket(self, val: str):
        self.__aws_bucket = str(val)

    def set_aws_chunk_size(self, val: int):
        self.__aws_chunk_size = int(val)

    def aws_correct_folder_name(self, _dir: str):
        valid_characters = '0123456789qwertyuiopasdfghjklzxcvbnmйцукенгшщзхъфывапролджэячсмитьбюё'
        if _dir[0].lower() not in valid_characters:
            _dir = 'A' + _dir
        return _dir


class BaseBackuper:

    args = None

    def __init__(self, args):
        self.args = args

    def __move_to_permanent_dir(self):
        label = self.args.label()
        files = Func.get_objects_list_on_disk(self.args.temp_path())
        if not os.path.exists(f'{self.args.path_to_full_backup_local()}\\{label}'):
            os.makedirs(f'{self.args.path_to_full_backup_local()}\\{label}')

        # move & rename
        for file in files:
            shutil.move(file,
                        f'{self.args.path_to_full_backup_local()}\\{label}\\{label}__{os.path.basename(file)}')

    def __clear_temp_dir(self):
        # clear the directory of any files
        if os.path.exists(self.args.temp_path()):
            for path in os.listdir(self.args.temp_path()):
                if os.path.exists(f'{self.args.temp_path()}\\{path}'):
                    os.remove(f'{self.args.temp_path()}\\{path}')

    def _create_backup(self):

        label = self.args.label()

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self.args.postgresql_password()

        self.__clear_temp_dir()

        if not os.path.exists(self.args.backuper()):
            raise Exception(
                f'Файл {self.args.backuper()} не найден. Проверьте правльность пути до кластера(инстанса) сервера PosgtrSQL или бэкапера(если он задан). Текущий путь до кластера в скрипте - {self.args.postgresql_isntance_path()}')

        process = subprocess.run(
            [self.args.backuper(),
             '-D', self.args.temp_path(),
             '-X', 'fetch',
             '-F', 'tar',
             '--label', label,
             '--gzip',
             '--no-password',
             '--username', self.args.postgresql_username(),
             ],
            stderr=subprocess.PIPE,
            env=my_env,
        )
        text_error = process.stderr.decode()
        if text_error == "":
            self.__move_to_permanent_dir()
            self.__clear_temp_dir()
        else:
            raise Exception(text_error)




class YandexConnector:

    args = None

    def __init__(self, args):

        self.args = args

    def __compute_files_to_upload(self, backups, path_cloud, with_last_dir=True):
        to_upload = []
        for backup in backups:
            _dir = os.path.dirname(backup)
            dir_name = os.path.basename(_dir)
            file_name = os.path.basename(backup)
            if with_last_dir:
                file_name = f'{dir_name}/{file_name}'

            try:
                res = requests.get(f'{self.args.url()}?path={path_cloud}/{file_name}',
                                   headers=self.args.headers())
                if not res.status_code == 200:
                    if res.status_code == 404:
                        to_upload.append(backup)
                    else:
                        raise Exception(
                            f'При синхронизации с облаком не удалось определить файлы для выгрузки. {res.text}')
            except Exception as e:
                raise Exception(f'{traceback.format_exc()}\n{e}')

        return to_upload

    def __get_available_memory(self):
        try:
            res = requests.get(f'https://cloud-api.yandex.net/v1/disk?fields=used_space%2Ctotal_space',
                               headers=self.args.headers())
            if not res.status_code == 200:
                raise Exception(f'Не удалось получить инфо о свободном месте в облаке. {res.text}')
            data = res.json()
            return data['total_space'] - data['used_space']
        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')

    def __prepare_dir_on_cloud(self, backups, path_cloud, with_last_dir=True):
        requerd_path = pathlib.Path(path_cloud).parts
        i = 1
        step = ''
        while i < len(requerd_path):
            step += '/' + requerd_path[i]
            self.__create_dir_on_cloud(step)
            i = i + 1

        if with_last_dir:
            paths = []
            for backup in backups:
                _dir = os.path.dirname(backup)
                dir_name = os.path.basename(_dir)
                paths.append(dir_name)

            paths = set(paths)
            for dir_name in paths:
                self.__create_dir_on_cloud(f'{path_cloud}/{dir_name}')

    def __create_dir_on_cloud(self, path):
        try:
            res = requests.put(f'{self.args.url()}?path={path}',
                               headers=self.args.headers())

            if not res.status_code == 201 and not res.status_code == 409:
                raise Exception(f'Не удалось создать каталог {path} в облаке. {res.text}')

        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')

    def _upload_on_cloud(self):
        full_backups = Func.get_objects_list_on_disk(self.args.path_to_full_backup_local())
        incr_backups = Func.get_objects_list_on_disk(self.args.local_path_to_wal_files())

        path_to_full_backup_cloud = self.args.path_to_full_backup_cloud()
        path_to_incr_backup_cloud = self.args.path_to_incr_backup_cloud()

        full_backups_to_upload = self.__compute_files_to_upload(full_backups, path_to_full_backup_cloud)
        incr_backups_to_upload = self.__compute_files_to_upload(incr_backups, path_to_incr_backup_cloud, False)

        upload_size = 0
        for file_path in full_backups_to_upload:
            upload_size += os.stat(file_path).st_size

        for file_path in incr_backups_to_upload:
            upload_size += os.stat(file_path).st_size

        if upload_size == 0:
            return 'Нет новых файлов для выгрузки'

        av_memory = self.__get_available_memory()
        to_clear = upload_size - av_memory

        if to_clear > 0:
            raise Exception(f'Недостаточно места в облаке. Требуется еще {to_clear / 1024 / 1024} мб')

        self.__prepare_dir_on_cloud(full_backups_to_upload, path_to_full_backup_cloud)
        self.__prepare_dir_on_cloud(incr_backups_to_upload, path_to_incr_backup_cloud, False)

        for backup in full_backups_to_upload:
            _dir = os.path.dirname(backup)
            dir_name = os.path.basename(_dir)
            file_name = os.path.basename(backup)
            self.__upload_file(backup, f'{path_to_full_backup_cloud}/{dir_name}/{file_name}')

        for backup in incr_backups_to_upload:
            file_name = os.path.basename(backup)
            self.__upload_file(backup, f'{path_to_incr_backup_cloud}/{file_name}')

    def __create_folder(self, path):
        """Создание папки. \n path: Путь к создаваемой папке."""
        requests.put(f'{self.args.url()}?path={path}', headers=self.args.headers())

    def __upload_file(self, loadfile, savefile, replace=False):
        """Загрузка файла.
        savefile: Путь к файлу на Диске
        loadfile: Путь к загружаемому файлу
        replace: true or false Замена файла на Диске"""
        res = requests.get(f'{self.args.url()}/upload?path={savefile}&overwrite={replace}',
                           headers=self.args.headers()).json()
        with open(loadfile, 'rb') as f:
            try:
                res = requests.put(res['href'], files={'file': f})
                if not res.status_code == 201:
                    raise Exception(f'Не удалось выгрузить файл {loadfile} в облако. {res.text}')
            except Exception as e:
                raise Exception(f'{traceback.format_exc()}\n{e}')

    def __delete_obj_on_yandex_cloud(self, path):
        result = False
        try:
            res = requests.delete(f'{self.args.url()}?path={path}&permanently=true',
                                  headers=self.args.headers())
            if not res.status_code == 202 or not res.status_code == 204:
                result = True
        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')

        return result

    def __delete_empty_dirs_on_yandex(self, keep_roots=True):
        root_dirs = self.__get_root_dirs()
        empty_dirs = []
        for path in root_dirs:
            self.__empty_yandex_cloud_dirs(path, empty_dirs)

        if keep_roots:
            for _dir in root_dirs:
                path = '/' + _dir
                try:
                    empty_dirs.remove(path)
                except ValueError:
                    continue

        empty_dirs = Func.optimize_remove_list_dir(empty_dirs)
        for path in empty_dirs:
            self.__delete_obj_on_yandex_cloud(path)

    def __empty_yandex_cloud_dirs(self, path, empty_dirs: []):
        try:
            limit = 1000000000
            res = requests.get(f'{self.args.url()}?path={path}&limit={limit}',
                               headers=self.args.headers())
            if not res.status_code == 200:
                raise Exception(
                    f'Удаление пустых папок в облаке. Не удалось получить список папок в облаке. {res.text}')
            data = res.json()
        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')

        empty_dirs.append(path)

        for item in data['_embedded']['items']:
            if item['type'] == 'file':
                firm_path = ''
                # let's remove each parent directory from the list, going up the level step by step
                curr_path = item['path'].replace('disk:', '').split('/')
                for i, _dir in enumerate(curr_path):
                    if i == 0:
                        continue
                    firm_path += f'/{_dir}'
                    try:
                        empty_dirs.remove(firm_path)
                    except ValueError:
                        continue
            if item['type'] == 'dir':
                sub_path = item['path'].replace('disk:', '')
                self.__empty_yandex_cloud_dirs(sub_path, empty_dirs)

    def _clean_cloud(self):
        extra_bck = self.__get_extra_bck_on_cloud()
        if len(extra_bck) > 0:
            for bck in extra_bck:
                self.__delete_obj_on_yandex_cloud(bck)
            self.__delete_empty_dirs_on_yandex()

    def __get_extra_bck_on_cloud(self):
        local_paths = [self.args.path_to_full_backup_local(),
                       self.args.local_path_to_wal_files()]
        existing_files_md5 = []
        for path in local_paths:
            files = Func.get_objects_list_on_disk(path, only_files=True)
            for file in files:
                existing_files_md5.append(Func.get_md5(file))

        extra_bck = self._calculate_extra_on_yandex_cloud(existing_files_md5)
        return extra_bck

    def _calculate_extra_on_yandex_cloud(self, existing_files_md5):
        extra_bck = []
        try:
            limit = 1000000000
            res = requests.get(f'{self.args.url()}/files?preview_crop=true&sort=path&limit={limit}',
                               headers=self.args.headers())
            if not res.status_code == 200:
                raise Exception(f'Очитка облака. Не удалось получить список файлов в облаке. {res.text}')
            data = res.json()
        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')
        check_dirs = self.__get_root_dirs()
        for item in data['items']:
            for check_dir in check_dirs:
                curr_cl_dir = item['path'].replace('disk:', '')
                if curr_cl_dir.startswith(check_dir):
                    try:
                        existing_files_md5.index(item['md5'])
                        continue
                    except ValueError:
                        extra_bck.append(curr_cl_dir)
        return extra_bck

    def __get_root_dirs(self):
        root_dirs = [self.args.path_to_full_backup_cloud(),
                     self.args.path_to_incr_backup_cloud()]
        for i, val in enumerate(root_dirs):
            temp = val.split("/")
            root_dirs[i] = f'/{temp[1]}/{temp[2]}'

        return set(root_dirs)


class AWS_Connector:

    aws_client = None
    args = None

    def __init__(self, args):
        self.args = args
        session = boto3.session.Session()
        self.aws_client = session.client(
            service_name='s3',
            endpoint_url=self.args.aws_endpoint_url(),
            aws_access_key_id=self.args.aws_access_key_id(),
            aws_secret_access_key=self.args.aws_secret_access_key(),
        )

    def _upload_on_cloud(self):
        bucket = self.args.aws_bucket()
        message = Func.bucket_exists_and_accessible(self.aws_client, bucket)
        if message is not None:
            raise Exception(message)
        local_path_wal = self.args.local_path_to_wal_files()
        local_path_full = self.args.path_to_full_backup_local()

        local_backups = Func.get_objects_list_on_disk(local_path_full)
        local_backups.extend(Func.get_objects_list_on_disk(local_path_wal))

        cloud_backups = Func.get_objects_on_aws(self.aws_client, bucket, self.args.path_to_cloud_custom_dir(True))
        backups_to_upload = self.__compute_files_to_upload(local_backups, cloud_backups, self.args.aws_chunk_size())

        if len(backups_to_upload) == 0:
            return 'Нет новых файлов для выгрузки'

        cloud_path_full = self.args.path_to_full_backup_cloud(for_aws=True)
        cloud_path_wal = self.args.path_to_incr_backup_cloud(for_aws=True)

        upload_config = boto3.s3.transfer.TransferConfig(multipart_chunksize=self.args.aws_chunk_size())
        for backup_local in backups_to_upload:
            if local_path_wal in backup_local:
                cloud_path = cloud_path_wal
            else:
                parent_path = os.path.dirname(backup_local)
                parent_dir = os.path.basename(parent_path)
                cloud_path = cloud_path_full + '/' + self.args.aws_correct_folder_name(parent_dir)

            file_name = os.path.basename(backup_local)
            savefile = f'{cloud_path}/{file_name}'
            self.aws_client.upload_file(backup_local, bucket, savefile, Config=upload_config)

    def __compute_files_to_upload(self, local_backups: [], cloud_backups: [], chunk_size):
        to_upload = local_backups.copy()

        for backup_local in local_backups:
            file_name = os.path.basename(backup_local)
            parent_path = os.path.dirname(backup_local)
            parent_dir = os.path.basename(parent_path)
            parent_dir = self.args.aws_correct_folder_name(parent_dir)
            search_string = f'{parent_dir}/{file_name}'

            md5_local = Func.get_md5(backup_local, chunk_size)
            for cloud_backup in cloud_backups:
                if search_string in cloud_backup['Path'] and md5_local == cloud_backup['Hash']:
                    try:
                        to_upload.remove(backup_local)
                        break
                    except ValueError:
                        break

        return to_upload

    def _clean_cloud(self):
        extra_bck = self.__get_extra_bck_on_cloud()
        if len(extra_bck) > 0:
            objects = []
            for bck in extra_bck:
                objects.append({'Key': bck})
            self.aws_client.delete_objects(Bucket=self.args.aws_bucket(), Delete={'Objects': objects})
            # self.__delete_empty_dirs_on_aws()

    def __get_extra_bck_on_cloud(self):

        local_paths = [self.args.path_to_full_backup_local(),
                       self.args.local_path_to_wal_files()]
        existing_files_md5 = []
        chunk_size = self.args.aws_chunk_size()
        for path in local_paths:
            files = Func.get_objects_list_on_disk(path, only_files=True)
            for file in files:
                existing_files_md5.append(Func.get_md5(file, chunk_size))
        extra_bck = self._calculate_extra_on_aws(existing_files_md5)
        return extra_bck

    def _calculate_extra_on_aws(self, existing_files_md5):
        extra_bck = []
        cloud_backups = Func.get_objects_on_aws(self.aws_client, self.args.aws_bucket(),
                                                self.args.path_to_cloud_custom_dir(for_aws=True))
        for cloud_backup in cloud_backups:
            try:
                existing_files_md5.index(cloud_backup['Hash'])
                continue
            except ValueError:
                extra_bck.append(cloud_backup['Path'])
                continue

        return extra_bck

    def __delete_empty_dirs_on_aws(self):
        verification_path = self.args.path_to_cloud_custom_dir(for_aws=True)
        empty_dirs = self.__empty_aws_cloud_dirs(verification_path)

        try:
            empty_dirs.remove(verification_path + '/')
        except KeyError:
            a = 1

        empty_dirs = Func.optimize_remove_list_dir(empty_dirs)

        for_deletion = []
        for path in empty_dirs:
            for_deletion.append({'Key': path})

        self.aws_client.delete_objects(Bucket=self.args.aws_bucket(), Delete={'Objects': for_deletion})

    def __empty_aws_cloud_dirs(self, verification_path):
        obj_on_aws = Func.get_objects_on_aws(self.aws_client, self.args.aws_bucket(),
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


class Cleaner:

    whois_timezone_info = {}
    args = None

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

    def _clean_local(self, expire_date):
        full_bck = self.__full_bck_to_remove(expire_date)
        for obj in full_bck:
            os.remove(obj)

        mask = '__backup_manifest'
        second_mask = '__base.'
        full_bck = Func.get_objects_list_on_disk(self.args.path_to_full_backup_local(), mask=mask, second_mask=second_mask, only_files=True)

        oldest_date = datetime.datetime.now(tzlocal.get_localzone())
        oldest_label = None
        for file in full_bck:
            file_name = os.path.basename(file)
            bck_date = self.__read_create_date(file)
            if bck_date is not None and oldest_date >= bck_date:
                oldest_date = bck_date
                if mask in file_name:
                    current_mask = mask
                else:
                    current_mask = second_mask

                oldest_label = file_name.split(current_mask)[0]

        if oldest_label is not None:
            inc_bck = self.__inc_bck_to_remove(self.args.local_path_to_wal_files(), oldest_label)
            for obj in inc_bck:
                os.remove(obj)

        self.__delete_local_empty_bck_dirs()

    def __number_inc_bck(self, path, label):
        result = None

        for root, dirs, files in os.walk(path):
            for file in files:
                if result is not None:
                    break
                if '.backup' not in file:
                    continue
                try:
                    with open(os.path.join(root, file)) as content:
                        temp_res = ''
                        for line in content:
                            if 'START WAL LOCATION' in line:
                                temp_res = line.split('file ')[1]
                            if label in line:
                                if ')' in temp_res:
                                    temp_res = temp_res.split(')')[0]
                                result = '0x' + temp_res
                                break

                except Exception as e:
                    print("TROUBLE - " + str(e))
                    continue

        if result is not None:
            result = int(result, base=16)
        return result

    def __inc_bck_to_remove(self, path, oldest_label, delete_unsuitable=False):
        oldest_number = self.__number_inc_bck(path, oldest_label)
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

    def __full_bck_to_remove(self, exprire_date: datetime):
        mask = '__backup_manifest'
        second_mask = '__base.'
        full_bck = Func.get_objects_list_on_disk(self.args.path_to_full_backup_local(), mask=mask, second_mask=second_mask, only_files=True)
        result = []

        for file in full_bck:
            file_name = os.path.basename(file)
            bck_date = self.__read_create_date(file)
            if bck_date is not None and bck_date <= exprire_date:
                if mask in file_name:
                    current_mask = mask
                else:
                    current_mask = second_mask

                portion = Func.get_objects_list_on_disk(os.path.dirname(file),
                                                        mask=file_name.split(current_mask)[0], only_files=True)
                result.extend(portion)
        return result

    def __read_create_date(self, backup: str):

        try:
            if '__backup_manifest' in backup:
                with open(backup) as json_file:
                    data = json.load(json_file)
                    for p in data['Files']:
                        if p['Path'] == 'backup_label':
                            date_str = p['Last-Modified']
                            break
                json_file.close()
            else:
                if backup.endswith("tar.gz"):
                    tar = tarfile.open(backup, "r:gz")
                elif backup.endswith("tar"):
                    tar = tarfile.open(backup, "r:")

                ex_file = tar.extractfile('backup_label')

                while True:
                    # считываем строку
                    line = ex_file.readline()
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
                ex_file.close()
        except:
            date_str = None

        if date_str is None:
            return None
        tz = date_str.split(' ')[2]
        tzinfos = ''
        for item in self.whois_timezone_info.items():
            if item[0] == tz:
                tzinfos = {item[0]:item[1]}
                break

        result = parser.parse(date_str, tzinfos=tzinfos)

        return result

    def __delete_local_empty_bck_dirs(self):
        arr = [self.args.path_to_full_backup_local(), self.args.local_path_to_wal_files()]
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
        if delete_it:
            os.rmdir(dir_path)
        return delete_it


class Manager:
    __connector = None
    __backuper = None
    __cleaner = None
    __args = None
    __aws_client = None

    def __init__(self, new_args=None, args_in_lower_case=False, use_backuper=True,
                 use_cleaner=True, use_yandex=False):


        try:
            self.__args = Args(use_backuper, use_cleaner, use_yandex, new_args, args_in_lower_case)
        except Exception as e:
            self.write_log('backup-', False, str(e))
            raise e

        if use_backuper:
            self.__backuper = BaseBackuper(self.__args)

        if use_yandex:
            self.__connector = YandexConnector(self.__args)
        else:
            self.__connector = AWS_Connector(self.__args)

        if use_cleaner:
            self.__cleaner = Cleaner(self.__args)

    def _test(self):

        self.__args.set_disk('C')
        self.__args.set_root_dir('Postgresql backups')
        self.__args.set_custom_dir('Отдел продаж')
        self.__args.set_full_bp_dir('Full')
        self.__args.set_local_path_to_wal_files('C:\\Postgresql backups\\pg_log_archive')
        file = open('./aws_access_key_id')
        self.__args.set_aws_access_key_id(file.read())
        file = open('./aws_secret_access_key')
        self.__args.set_aws_secret_access_key(file.read())

        now = parser.parse("26.11.2022 00:00:00 +3")
        # Test.Delete before release

        storage_time = 1565100
        expire_date = now - datetime.timedelta(seconds=storage_time)
        self.__cleaner._clean_local(expire_date)
        self.__connector._clean_cloud()

    def clean_backups(self, write_to_log_file=True, raise_exception=False):
        storage_time = self.__args.storage_time()
        if storage_time is None or storage_time < 1:
            message = 'Failed to delete outdated backups - the storage_time parameter is not set or it is less than 1'

            if write_to_log_file:
                self.write_log('clearing-', False, message)
            if raise_exception:
                raise Exception(message)

        try:
            now = datetime.datetime.now(tzlocal.get_localzone())
            expire_date = now - datetime.timedelta(seconds=storage_time)
            self.__cleaner._clean_local(expire_date)
            self.__connector._clean_cloud()
            if write_to_log_file:
                self.write_log('clearing-', True, '')
        except Exception as e:
            if write_to_log_file:
                self.write_log('clearing-', False, str(e))
            if raise_exception:
                raise e

    def create_backup(self, write_to_log_file=True, raise_exception=False):
        try:
            self.__backuper._create_backup()
            if write_to_log_file:
                self.write_log('backup-', True, '')
        except Exception as e:
            if write_to_log_file:
                self.write_log('backup-', False, str(e))
            if raise_exception:
                raise e

    def upload_on_cloud(self, write_to_log_file=True, raise_exception=False):
        try:
            message = self.__connector._upload_on_cloud()
            if write_to_log_file:
                self.write_log('upload-', True, str(message))
        except Exception as e:
            if write_to_log_file:
                self.write_log('upload-', False, str(e))
            if raise_exception:
                raise e

    def main(self):
        self.create_backup()
        self.upload_on_cloud()
        self.clean_backups()

    def write_log(self, file_pref, success, text=''):

        default_path = './PostgreSQLBackuperLogs'
        try:
            path = self.__args.log_path()
            if path == '' or path is None:
                path = default_path
        except:
            path = default_path

        if not os.path.exists(path):
            os.makedirs(path)

        if self.__args is None:
            label = Args._generate_label(use_millisec=True)
        else:
            label = self.__args.label()
        result = "Success_" if success else 'FAIL_'
        path = f'{path}\\{file_pref}{result}{label}.txt'
        file = open(path, "w")
        file.write(text)
        file.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    m = Manager()
    m.main()
