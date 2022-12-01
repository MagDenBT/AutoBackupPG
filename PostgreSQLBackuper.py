# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import boto3

import os
import pathlib
import shutil
import subprocess
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
    def get_objects_list_on_disk(path, mask=None, only_files=True):
        objects_list = []
        for root, dirs, files in os.walk(path):
            for filename in files:
                if mask is not None:
                    if mask not in filename:
                        continue
                objects_list.append(os.path.join(root, filename))

            if not only_files:
                for _dir in dirs:
                    if mask is not None:
                        if mask not in _dir:
                            continue
                    objects_list.append(os.path.join(root, _dir))

        return objects_list

    @staticmethod
    def get_md5(files):
        hashes = []
        for path in files:
            hash_md5 = hashlib.md5()
            with open(path, "rb") as file:
                for chunk in iter(lambda: file.read(4096), b""):
                    hash_md5.update(chunk)
            hashes.append(hash_md5.hexdigest())
        return hashes

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

    def __getitem__(self, item):
        return getattr(self, item)

    def __init__(self, args=None, in_lower_case=True):
        self.set_params(args, in_lower_case)
        if not self._check_params():
            raise Exception('Required parameter is missing')

    def set_params(self, args=None, in_lower_case=True):
        if args is not None:
            for key, value in args.items():
                key_mod = str.lower(key) if in_lower_case else key
                set_method = self[f'set_{key_mod}']
                set_method(value)
        else:
            self.__set_default_params()

    def _check_params(self):
        result = True
        req_arg = [self.__disk,
                   self.__root_dir,
                   self.__custom_dir,
                   self.__full_bp_dir,
                   self.__local_path_to_wal_files,
                   self.__postgresql_isntance_path,
                   self.__postgresql_username,
                   self.__postgresql_password,
                   self.__cloud_token]
        for arg in req_arg:
            if arg is None or arg == '':
                result = False
                break

        return result

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
        self.__cloud_token = 'jjj'

    def __generate_label(self, millisec=False):
        if millisec:
            time_stamp = datetime.datetime.utcnow().strftime('%Y_%m_%d____%H-%M-%S.%f')[:-3]
        else:
            time_stamp = datetime.datetime.now().strftime('%Y_%m_%d____%H-%M-%S')

        return time_stamp + '_' + str(random.randint(1, 100))

    # Getters

    def aws_bucket(self):
        valid_characters = '0123456789qwertyuiopasdfghjklzxcvbnm-.'
        valid_first_last = '0123456789qwertyuiopasdfghjklzxcvbnm'
        bucket = self.root_dir().lower()

        if bucket[0] not in valid_first_last:
            bucket = 'a' + bucket
        temp_str = ''

        for s in bucket:
            if s in valid_characters:
                temp_str += s
            else:
                temp_str += '-'
        bucket = temp_str

        if bucket[len(bucket) - 1] not in valid_first_last:
            bucket = bucket + "z"

        return bucket

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

    def root_dir(self):
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
            self.__backuper = self.postgresql_isntance_path() + 'bin\\pg_basebackup.exe'
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
        if for_aws:
            # The path to the permanent  directory for full backup, without the bucket
            return f'{self.custom_dir(True)}/{self.full_bp_dir(True)}'
        else:
            # The path to the permanent  directory for full backup
            return f'/{self.root_dir()}/{self.custom_dir()}/{self.full_bp_dir()}'

    def path_to_incr_backup_cloud(self, for_aws=False):
        incr_bp_dir = os.path.basename(self.local_path_to_wal_files())
        if for_aws:
            incr_bp_dir = self.aws_correct_folder_name(incr_bp_dir)
            return f'{self.custom_dir(True)}/{incr_bp_dir}'
        else:
            return f'/{self.root_dir()}/{self.custom_dir()}/{incr_bp_dir}'

    def label(self):
        if self.__label is None or self.__label == '':
            self.__label = self.__generate_label()
        return self.__label

    def url(self):
        if self.__url is None or self.__url == '':
            var = 'https://cloud-api.yandex.net/v1/disk/resources'
            self.__url = var
        return self.__url

    # Setters
    def set_aws_access_key_id(self, val: str):
        self.__aws_access_key_id = val

    def set_aws_secret_access_key(self, val: str):
        self.__aws_secret_access_key = val

    def set_aws_endpoint_url(self, val: str):
        self.__aws_endpoint_url = val

    def set_disk(self, val: str):
        self.__disk = val

    def set_root_dir(self, val: str):
        self.__root_dir = val

    def set_custom_dir(self, val: str):
        self.__custom_dir = val

    def set_full_bp_dir(self, val: str):
        self.__full_bp_dir = val

    def set_local_path_to_wal_files(self, val: str):
        self.__local_path_to_wal_files = val

    def set_postgresql_isntance_path(self, val: str):
        self.__postgresql_isntance_path = val

    def set_postgresql_username(self, val: str):
        self.__postgresql_username = val

    def set_postgresql_password(self, val: str):
        self.__postgresql_password = val

    def set_cloud_token(self, val: str):
        self.__cloud_token = val

    def set_temp_path(self, val: str):
        self.__temp_path = val

    def set_log_path(self, val: str):
        self.__log_path = val

    def set_backuper(self, val: str):
        self.__backuper = val

    def set_storage_time(self, val: int):
        self.__storage_time = val

    def set_url(self, val: str):
        self.__url = val

    def aws_correct_folder_name(self, _dir: str):
        valid_characters = '0123456789qwertyuiopasdfghjklzxcvbnmйцукенгшщзхъфывапролджэячсмитьбюё'
        if _dir[0].lower() not in valid_characters:
            _dir = 'A' + _dir
        return _dir


class Backuper:
    manager = None
    args = None

    def __init__(self, manager, args):
        self.manager = manager
        self.args = args

    def __file_operations_full_backup(self):
        label = self.args.label()
        files = Func.get_objects_list_on_disk(self.args.temp_path())
        if not os.path.exists(f'{self.args.path_to_full_backup_local()}\\{label}'):
            os.makedirs(f'{self.args.path_to_full_backup_local}\\{label}')

        # move & rename
        for file in files:
            shutil.move(file,
                        f'{self.args.path_to_full_backup_local}\\{label}\\{label}__{os.path.basename(file)}')

    def __clear_temp_dir_full_backup(self):
        # clear the directory of any files
        if os.path.exists(self.args.temp_path()):
            for path in os.listdir(self.args.temp_path()):
                if os.path.exists(f'{self.args.temp_path()}\\{path}'):
                    os.remove(f'{self.args.temp_path()}\\{path}')

    def create_full_backup(self):

        label = self.args.label()

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self.args.set_postgresql_password()

        self.__clear_temp_dir_full_backup()

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
            self.__file_operations_full_backup()
            self.__clear_temp_dir_full_backup()
        else:
            raise Exception(text_error)


class UploaderYandex:
    manager = None
    args = None

    def __init__(self, manager, args):
        self.manager = manager
        self.args = args

    def __get_files_to_upload(self, backups, pathCloud, withLastDir=True):
        to_upload = []
        for backup in backups:
            _dir = os.path.dirname(backup)
            dir_name = os.path.basename(_dir)
            file_name = os.path.basename(backup)
            if withLastDir:
                file_name = f'{dir_name}/{file_name}'

            try:
                res = requests.get(f'{self.args.url()}?path={pathCloud}/{file_name}',
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

    def __prepare_dir_on_cloud(self, backups, pathCloud, with_last_dir=True):
        requerd_path = pathlib.Path(pathCloud).parts
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
                self.__create_dir_on_cloud(f'{pathCloud}/{dir_name}')

    def __create_dir_on_cloud(self, path):
        try:
            res = requests.put(f'{self.args.url()}?path={path}',
                               headers=self.args.headers())

            if not res.status_code == 201 and not res.status_code == 409:
                raise Exception(f'Не удалось создать каталог {path} в облаке. {res.text}')

        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')

    def upload_on_yandex_cloud(self):
        full_backups = Func.get_objects_list_on_disk(self.args.path_to_full_backup_local())
        incr_backups = Func.get_objects_list_on_disk(self.args.local_path_to_wal_files())

        path_to_full_backup_cloud = self.args.path_to_full_backup_cloud()
        path_to_incr_backup_cloud = self.args.path_to_incr_backup_cloud()

        full_backups_to_upload = self.__get_files_to_upload(full_backups, path_to_full_backup_cloud)
        incr_backups_to_upload = self.__get_files_to_upload(incr_backups, path_to_incr_backup_cloud, False)

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


class UploaderAWSS3:

    aws_client = None
    manager = None
    args = None

    def __init__(self, manager, args, aws_client):
        self.manager = manager
        self.args = args
        self.aws_client = aws_client

    @property
    def _upload_on_aws_cloud(self):

        local_path_wal = self.args.local_path_to_wal_files()
        local_path_full = self.args.path_to_full_backup_local()
        bucket = self.args.aws_bucket()

        local_backups = Func.get_objects_list_on_disk(local_path_full)
        local_backups.extend(Func.get_objects_list_on_disk(local_path_wal))

        cloud_backups = self._get_files_list_on_cloud(bucket, self.args.custom_dir(True))
        backups_to_upload = self.__get_files_to_upload(local_backups, cloud_backups)

        if len(backups_to_upload) == 0:
            return 'Нет новых файлов для выгрузки'

        cloud_path_full = self.args.path_to_full_backup_cloud(for_aws=True)
        cloud_path_wal = self.args.path_to_incr_backup_cloud(for_aws=True)

        for backup_local in backups_to_upload:
            if local_path_wal in backup_local:
                cloud_path = cloud_path_wal
            else:
                parent_path = os.path.dirname(backup_local)
                parent_dir = os.path.basename(parent_path)
                cloud_path = cloud_path_full + '/' + self.args.aws_correct_folder_name(parent_dir)

            file_name = os.path.basename(backup_local)
            savefile = f'{cloud_path}/{file_name}'
            self.aws_client.upload_file(backup_local, bucket, savefile)

    def __get_files_to_upload(self, local_backups: [], cloud_backups: []):
        to_upload = local_backups.copy()

        for backup_local in local_backups:
            file_name = os.path.basename(backup_local)
            parent_path = os.path.dirname(backup_local)
            parent_dir = os.path.basename(parent_path)
            parent_dir = self.args.aws_correct_folder_name(parent_dir)
            search_string = f'{parent_dir}/{file_name}'

            for cloud_backup in cloud_backups:
                if search_string in cloud_backup['Path']:
                    md5_local = Func.get_md5(cloud_backup['Path'])
                    if md5_local == cloud_backup['Hash']:
                        try:
                            to_upload.remove(backup_local)
                            break
                        except KeyError or ValueError:
                            break

        return to_upload

    def _get_files_list_on_cloud(self, bucket, custom_dir):
        result = []
        for obj in self.aws_client.list_objects(Bucket=bucket)['Contents']:
            if obj['Size'] == 0:
                continue
            resource_name = obj['Key']
            if custom_dir + '/' in resource_name:
                md5 = Func.get_md5_aws(self.aws_client, bucket, resource_name)
                result.append({'Hash': md5, 'Path': resource_name})
        return result


class Cleaner:

    manager = None
    args = None
    aws_client = None

    def __init__(self, manager, args, aws_client):
        self.manager = manager
        self.args = args
        self.aws_client = aws_client

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

    def __delete_empty_dirs_on_aws(self):
        custom_dir = self.args.custom_dir(for_aws=True)
        empty_dirs = self.__empty_aws_cloud_dirs(custom_dir)

        try:
            empty_dirs.remove(custom_dir + '/')
        except ValueError:
            a = 1

        empty_dirs = self.__optimize_remove_list(empty_dirs)

        for_deletion = []
        for path in empty_dirs:
            for_deletion.append({'Key': path})

        self.aws_client.delete_objects(Bucket=self.args.aws_bucket(), Delete={'Objects': for_deletion})

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

        empty_dirs = self.__optimize_remove_list(empty_dirs)
        for path in empty_dirs:
            self.__delete_obj_on_yandex_cloud(path)

    def __optimize_remove_list(self, empty_dirs):
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

    def __empty_aws_cloud_dirs(self, custom_dir):
        obj_on_aws = self._get_objects_on_aws(self.args.aws_bucket(), custom_dir, only_files=False, with_hash=False)
        obj_on_aws = set(obj_on_aws)
        result = obj_on_aws.copy()

        for i, path in enumerate(obj_on_aws):
            if not path.endswith('/'):  # is file
                dirs = path.split('/')
                firm_path = ''
                for z in dirs:
                    if z == len(dirs) - 1:
                        break
                    firm_path += f'{dirs[z]}/'
                    try:
                        result.remove(firm_path)
                        continue
                    except KeyError:
                        continue
        return result

    def clean_cloud(self):
        extra_bck = self.__get_extra_bck_on_cloud()
        if len(extra_bck) > 0:
            if self.aws_client is not None:
                objects = []
                for bck in extra_bck:
                    objects.append({'Key': bck})
                self.aws_client.delete_objects(Bucket='backuptest', Delete={'Objects': objects})
            else:
                for bck in extra_bck:
                    self.__delete_obj_on_yandex_cloud(bck)

            self.__delete_empty_dirs_on_yandex()

    def __get_extra_bck_on_cloud(self):

        local_paths = [self.args.path_to_full_backup_local(),
                       self.args.local_path_to_wal_files()]
        existing_files_md5 = []

        for path in local_paths:
            files = Func.get_objects_list_on_disk(path, only_files=True)
            hashes = Func.get_md5(files)
            existing_files_md5.extend(hashes)

        if self.aws_client is not None:
            extra_bck = self._calculate_extra_on_aws(existing_files_md5)
        else:
            extra_bck = self._calculate_extra_on_yandex_cloud(existing_files_md5)

        return extra_bck

    def _calculate_extra_on_aws(self, existingFilesMD5):
        extra_bck = []
        cloud_backups = self._get_objects_on_aws(self.args.aws_bucket(), self.args.custom_dir(for_aws=True))
        for cloud_backup in cloud_backups:
            try:
                existingFilesMD5.index(cloud_backup['Hash'])
                continue
            except ValueError or KeyError:
                extra_bck.append(cloud_backup['Path'])

        return extra_bck

    def _get_objects_on_aws(self, bucket, custom_dir, only_files=True, with_hash=True):
        result = []
        for obj in self.aws_client.list_objects(Bucket=bucket)['Contents']:
            if obj['Size'] == 0 and only_files:
                continue
            resource_name = obj['Key']
            if custom_dir == resource_name.split('/')[0]:
                if with_hash:
                    item = resource_name
                else:
                    md5 = Func.get_md5_aws(self.aws_client, bucket, resource_name)
                    item = {'Hash': md5, 'Path': resource_name}
                result.append(item)
        return result

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
        root_dirs = self.__get_root_dirs()
        for item in data['items']:
            is_bck_dir = False
            for _dir in root_dirs:
                if _dir == item['path'].split('/')[1]:
                    is_bck_dir = True
            if is_bck_dir:
                try:
                    existing_files_md5.index(item['md5'])
                    continue
                except ValueError:
                    extra_bck.append(item['path'].replace('disk:', ''))
        return extra_bck

    def __get_root_dirs(self):
        root_dirs = [self.args.path_to_full_backup_cloud(),
                     self.args.path_to_incr_backup_cloud()]
        for i, val in enumerate(root_dirs):
            temp = val.split("/")
            root_dirs[i] = f'/{temp[1]}/{temp[2]}'

        return set(root_dirs)

    def clean_local(self, expire_date):
        full_bck = self.__full_bck_to_remove(expire_date)
        for obj in full_bck:
            os.remove(obj)

        mask = '__backup_manifest'
        full_bck = Func.get_objects_list_on_disk(self.args.path_to_full_backup_local(), mask, only_files=False)

        oldest_date = datetime.datetime.now(tzlocal.get_localzone())
        oldest_label = None
        for file in full_bck:
            fileName = os.path.basename(file)
            if mask in fileName:
                date_str = self.__read_create_date(file)
                bck_date = parser.parse(date_str)
                if oldest_date >= bck_date:
                    oldest_date = bck_date
                    oldest_label = fileName.replace(mask, '')

        if oldest_label is not None:
            inc_bck = self.__inc_bck_to_remove(self.args.local_path_to_wal_files(), oldest_label)
            for obj in inc_bck:
                os.remove(obj)

        self.__delete_local_empty_bck_dirs()

    def __number_inc_bck(self, path, label):
        result = None

        for root, dirs, files in os.walk(path):
            for file in files:
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
        full_bck = Func.get_objects_list_on_disk(self.args.path_to_full_backup_local(), only_files=False)
        result = []
        mask = '__backup_manifest'
        for file in full_bck:
            file_name = os.path.basename(file)
            if mask in file_name:
                date_str = self.__read_create_date(file)
                bck_date = parser.parse(date_str)
                if bck_date <= exprire_date:
                    portion = Func.get_objects_list_on_disk(os.path.dirname(file),
                                                            file_name.split(mask)[0], only_files=False)
                    result.extend(portion)
        return result

    def __read_create_date(self, backupManifest: str):
        with open(backupManifest) as json_file:
            data = json.load(json_file)
            for p in data['Files']:
                if p['Path'] == 'backup_label':
                    return p['Last-Modified']

    def __delete_local_empty_bck_dirs(self):
        arr = [self.args.path_to_full_backup_local(), self.args.local_path_to_wal_files()]
        for path in arr:
            if os.path.exists(path):
                for root, dirs, files in os.walk(path):
                    for _dir in dirs:
                        dir_path = os.path.join(root, _dir)
                        content = Func.get_objects_list_on_disk(dir_path, only_files=True)
                        if len(content) == 0:
                            os.rmdir(dir_path)


class Manager:
    uploader = None
    backaper = None
    cleaner = None
    args = None
    s3 = None

    def __init__(self, new_args=None, args_in_lower_case=False):
        try:
            self.args = Args(new_args, args_in_lower_case)
        except Exception as e:
            self.write_log('backup-', False, str(e))
        self.backaper = Backuper(self, self.args)
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url=self.args.aws_endpoint_url(),
            aws_access_key_id=self.args.aws_access_key_id(),
            aws_secret_access_key=self.args.aws_secret_access_key(),
        )
        self.s3 = s3
        self.cleaner = Cleaner(self, self.args, s3)
        self.uploader = UploaderYandex(self, self.args, s3)

    def _test(self):
        file = open('./token')
        self.args.set_cloud_token(file.read())
        self.args.set_disk('C')
        self.args.set_root_dir('Postgresql backups')
        self.args.set_custom_dir('Отдел продаж')
        self.args.set_full_bp_dir('Full')
        self.args.set_local_path_to_wal_files('C:\\Postgresql backups\\pg_log_archive')

        now = parser.parse("26.11.2022 00:00:00 +3")
        # Test.Delete before release

        storage_time = 1565100
        expire_date = now - datetime.timedelta(seconds=storage_time)
        self.cleaner.clean_local(expire_date)
        self.cleaner.clean_cloud()

    def clean_backups(self):
        storage_time = self.args.storage_time()
        if storage_time is not None and storage_time > 0:
            now = datetime.datetime.now(tzlocal.get_localzone())
            expire_date = now - datetime.timedelta(seconds=storage_time)
            self.cleaner.clean_local(expire_date)

        self.cleaner.clean_cloud()

    def create_full_backup(self, write_to_log_file=True, raise_exception=False):
        try:
            self.backaper.create_full_backup()
            if write_to_log_file:
                self.write_log('backup-', True, '')
        except Exception as e:
            if write_to_log_file:
                self.write_log('backup-', False, str(e))
            if raise_exception:
                raise e

    def upload_on_yandex_cloud(self, write_to_log_file=True, raise_exception=False):
        try:
            self.uploader.upload_on_yandex_cloud()
            if write_to_log_file:
                self.write_log('upload-', True, '')
        except Exception as e:
            if write_to_log_file:
                self.write_log('upload-', False, str(e))
            if raise_exception:
                raise e

    def main(self):
        self.create_full_backup()
        self.upload_on_yandex_cloud()
        self.clean_backups()

    def write_log(self, file_pref, success, text=''):

        default_path = './PostgreSQLBackuperLogs'
        try:
            path = self.args.log_path()
            if path == '' or path is None:
                path = default_path
        except:
            path = default_path

        if not os.path.exists(path):
            os.makedirs(path)

        label = self.args.label()
        result = "Success_" if success else 'FAIL_'
        path = f'{path}\\{file_pref}{result}{label}.txt'
        file = open(path, "w")
        file.write(text)
        file.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    m = Manager()
    m._test()
