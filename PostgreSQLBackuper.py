# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
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


class Args(object):

    # Paths settings
    """
            The directory structure is as follows:
            On the local machine - disk:\rootDir\customDir\fullBpDir and for WAL-files - the one you specify
            In the cloud - /rootDir/customDir/fullBpDir and for WAL-files - /rootDir/customDir/"LastDirectoryIn(localPathToWALFiles)"
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

    def __getitem__(self, item):
        return getattr(self, item)

    def __init__(self, args=None, in_lower_case=True):
        self.set_params(args)
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
                In the cloud - /rootDir/customDir/fullBpDir and for WAL-files - /rootDir/customDir/"LastDirectoryIn(localPathToWALFiles)"
                """
        self.__disk = 'C'
        self.__root_dir = 'Postgresql backups'
        self.__custom_dir = 'Accounting department'
        self.__full_bp_dir = 'Full'
        self.__local_path_to_wal_files = 'C:\\pg_log_archive'  # The path of the WAL files.(aka Incremental backups)
        self.__postgresql_isntance_path = "C:\\Program Files\\PostgreSQL\\14.4-1.1C\\"  # The path to PostgreSQL
        self.__postgresql_username = "postgres"
        self.__postgresql_password = '1122'
        self.__cloud_token = 'jjj'

    def __generate_label(self, millisec=False):
        if millisec:
            timeStamp = datetime.datetime.utcnow().strftime('%Y_%m_%d____%H-%M-%S.%f')[:-3]
        else:
            timeStamp = datetime.datetime.now().strftime('%Y_%m_%d____%H-%M-%S')

        return timeStamp + '_' + str(random.randint(1, 100))

    # Getters
    def cloud_token(self):
        return self.__cloud_token

    def disk(self):
        return self.__disk

    def root_dir(self):
        return self.__root_dir

    def custom_dir(self):
        return self.__custom_dir

    def full_bp_dir(self):
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
        return f'{self.disk()}:\\{self.root_dir()}\\{self.custom_dir()}\\{self.full_bp_dir()}'  # The path to the permanent  directory for full backup

    def path_to_full_backup_cloud(self):
        return f'/{self.root_dir()}/{self.custom_dir()}/{self.full_bp_dir()}'  # The path to the permanent  directory for full backup

    def path_to_incr_backup_cloud(self):
        incrBpDir = os.path.basename(self.local_path_to_wal_files())
        return f'/{self.root_dir()}/{self.custom_dir()}/{incrBpDir}'

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


class Backuper:
   
    manager = None
    args = None
    def __init__(self, manager,args):
        self.manager = manager
        self.args = args
        
    def __file_operations_full_backup(self):
        label = self.args.label()
        files = self.__get_files_list_on_disk(self.args.temp_path())
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

    def _create_full_backup(self):

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
        textError = process.stderr.decode()
        if textError == "":
            self.__file_operations_full_backup()
            self.__clear_temp_dir_full_backup()
        else:
            raise Exception(textError)

    def __get_files_list_on_disk(self, path):
        filesList = []

        for root, dirs, files in os.walk(path):
            for filename in files:
                filesList.append(os.path.join(root, filename))

        return filesList

    def __get_files_to_upload(self, backups, pathCloud, withLastDir=True):
        toUpload = []
        for backup in backups:
            dir = os.path.dirname(backup)
            dirName = os.path.basename(dir)
            fileName = os.path.basename(backup)
            if withLastDir:
                fileName = f'{dirName}/{fileName}'

            try:
                res = requests.get(f'{self.args.url()}?path={pathCloud}/{fileName}',
                                   headers=self.args.headers())
                if not res.status_code == 200:
                    if res.status_code == 404:
                        toUpload.append(backup)
                    else:
                        raise Exception(
                            f'При синхронизации с облаком не удалось определить файлы для выгрузки. {res.text}')
            except Exception as e:
                raise Exception(f'{traceback.format_exc()}\n{e}')

        return toUpload

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

    def __prepare_dir_on_cloud(self, backups, pathCloud, withLastDir=True):
        requerdPath = pathlib.Path(pathCloud).parts
        i = 1
        step = ''
        while i < len(requerdPath):
            step += '/' + requerdPath[i]
            self.__create_dir_on_cloud(step)
            i = i + 1

        if withLastDir:
            paths = []
            for backup in backups:
                dir = os.path.dirname(backup)
                dirName = os.path.basename(dir)
                paths.append(dirName)

            paths = set(paths)
            for dirName in paths:
                self.__create_dir_on_cloud(f'{pathCloud}/{dirName}')

    def __create_dir_on_cloud(self, path):
        try:
            res = requests.put(f'{self.args.url()}?path={path}',
                               headers=self.args.headers())

            if not res.status_code == 201 and not res.status_code == 409:
                raise Exception(f'Не удалось создать каталог {path} в облаке. {res.text}')

        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')

    def _upload_on_yandex_cloud(self):
        fullBackups = self.__get_files_list_on_disk(self.args.path_to_full_backup_local())
        incrBackups = self.__get_files_list_on_disk(self.args.local_path_to_wal_files())

        pathToFullBackupCloud = self.args.path_to_full_backup_cloud()
        pathToIncrBackupCloud = self.args.path_to_incr_backup_cloud()

        fullBackupsToUpload = self.__get_files_to_upload(fullBackups, pathToFullBackupCloud)
        incrBackupsToUpload = self.__get_files_to_upload(incrBackups, pathToIncrBackupCloud, False)

        uploadSize = 0
        for filePath in fullBackupsToUpload:
            uploadSize += os.stat(filePath).st_size

        for filePath in incrBackupsToUpload:
            uploadSize += os.stat(filePath).st_size

        if uploadSize == 0:
            return 'Нет новых файлов для выгрузки'

        avMemory = self.__get_available_memory()
        toClear = uploadSize - avMemory

        if toClear > 0:
            raise Exception(f'Недостаточно места в облаке. Требуется еще {toClear / 1024 / 1024} мб')

        self.__prepare_dir_on_cloud(fullBackupsToUpload, pathToFullBackupCloud)
        self.__prepare_dir_on_cloud(incrBackupsToUpload, pathToIncrBackupCloud, False)

        for backup in fullBackupsToUpload:
            dir = os.path.dirname(backup)
            dirName = os.path.basename(dir)
            filename = os.path.basename(backup)
            self.__upload_file(backup, f'{pathToFullBackupCloud}/{dirName}/{filename}')

        for backup in incrBackupsToUpload:
            filename = os.path.basename(backup)
            self.__upload_file(backup, f'{pathToIncrBackupCloud}/{filename}')

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

class Cleaner:
    
    manager = None
    args = None

    def __init__(self, manager, args):
        self.manager = manager
        self.args = args

    def __delete_obj_on_cloud(self, path, permanently=True):
        result = False
        try:
            res = requests.delete(f'{self.args.url()}?path={path}&permanently={permanently}',
                                  headers=self.args.headers())
            if not res.status_code == 202 or not res.status_code == 204:
                result = True
        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')
        return result

    def __delete_cloud_empty_bck_dirs(self, keepRoots=True):
        rootDirs = self.__get_root_dirs()
        emptyDirs = []
        for path in rootDirs:
            self.__empty_cloud_dirs(path, emptyDirs)

        if keepRoots:
            for dir in rootDirs:
                path = '/' + dir
                try:
                    emptyDirs.remove(path)
                except ValueError:
                    continue
        failedToDelete = []
        emptyDirs = self.__optimize_remove_list(emptyDirs)
        for path in emptyDirs:
            if not self.__delete_obj_on_cloud(path, False):
                failedToDelete.append(path)
        return failedToDelete

    def __optimize_remove_list(self, emptyDirs):
        emptyDirs = set(emptyDirs)
        tempColl = emptyDirs.copy()
        for i, sought in enumerate(emptyDirs):
            for z, target in enumerate(emptyDirs):
                if z == i:
                    continue
                if target.startswith(sought):
                    try:
                        tempColl.remove(target)
                    except KeyError:
                        continue
        return tempColl

    def __empty_cloud_dirs(self, path, emptyDirs: []):
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

        emptyDirs.append(path)

        for item in data['_embedded']['items']:
            if item['type'] == 'file':
                firmPath = ''
                # let's remove each parent directory from the list, going up the level step by step
                currPath = item['path'].replace('disk:', '').split('/')
                for i, dir in enumerate(currPath):
                    if i == 0:
                        continue
                    firmPath += f'/{dir}'
                    try:
                        emptyDirs.remove(firmPath)
                    except ValueError:
                        continue
            if item['type'] == 'dir':
                subPath = item['path'].replace('disk:', '')
                self.__empty_cloud_dirs(subPath, emptyDirs)

    def _clean_cloud(self):
        extraBck = self.__get_extra_bck_on_cloud()
        failedToDelete = []
        permanently = False
        for bck in extraBck:
            if not self.__delete_obj_on_cloud(bck, permanently):
                failedToDelete.append(bck)

        info = self.__delete_cloud_empty_bck_dirs()
        failedToDelete.extend(info)
        return failedToDelete

    def __get_extra_bck_on_cloud(self):
        localPaths = [self.args.path_to_full_backup_local(),
                      self.args.local_path_to_wal_files()]
        existingFilesMD5 = []
        extraBck = []

        for path in localPaths:
            files = self.__get_objects_list_on_disk(path, onlyFiles=True)
            hashes = self.__get_md5(files)
            existingFilesMD5.extend(hashes)

        try:
            limit = 1000000000
            res = requests.get(f'{self.args.url()}/files?preview_crop=true&sort=path&limit={limit}',
                               headers=self.args.headers())
            if not res.status_code == 200:
                raise Exception(f'Очитка облака. Не удалось получить список файлов в облаке. {res.text}')
            data = res.json()
        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')

        rootDirs = self.__get_root_dirs()

        for item in data['items']:
            isBckDir = False
            for dir in rootDirs:
                if dir == item['path'].split('/')[1]:
                    isBckDir = True
            if isBckDir:
                try:
                    existingFilesMD5.index(item['md5'])
                    continue
                except ValueError:
                    extraBck.append(item['path'].replace('disk:', ''))

        return extraBck

    def __get_root_dirs(self, level=2):
        rootDirs = [self.args.path_to_full_backup_cloud(),
                    self.args.path_to_incr_backup_cloud()]
        for i, val in enumerate(rootDirs):
            temp = val.split("/")
            rootDirs[i] = f'/{temp[1]}/{temp[2]}'

        return set(rootDirs)

    def __get_md5(self, files):
        hashes = []
        for path in files:
            hash_md5 = hashlib.md5()
            with open(path, "rb") as file:
                for chunk in iter(lambda: file.read(4096), b""):
                    hash_md5.update(chunk)
            hashes.append(hash_md5.hexdigest())
        return hashes

    def _clean_local(self, expire_date):
        full_bck = self.__full_bck_to_remove(expire_date)
        for obj in full_bck:
            os.remove(obj)

        mask = '__backup_manifest'
        full_bck = self.__get_objects_list_on_disk(self.args.path_to_full_backup_local(), mask)

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
                if not '.backup' in file:
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
                    curNumber = int(filename, base=16)
                    if curNumber < oldest_number:
                        to_remove.append(os.path.join(root, file))
                except Exception:
                    if delete_unsuitable:
                        to_remove.append(os.path.join(root, file))

        return to_remove

    def __full_bck_to_remove(self, exprireDate: datetime):
        fullBck = self.__get_objects_list_on_disk(self.args.path_to_full_backup_local())
        result = []
        mask = '__backup_manifest'
        for file in fullBck:
            fileName = os.path.basename(file)
            if mask in fileName:
                dateStr = self.__read_create_date(file)
                bckDate = parser.parse(dateStr)
                if bckDate <= exprireDate:
                    portion = self.__get_objects_list_on_disk(os.path.dirname(file), fileName.split(mask)[0])
                    result.extend(portion)
        return result

    def __read_create_date(self, backupManifest: str):
        with open(backupManifest) as json_file:
            data = json.load(json_file)
            for p in data['Files']:
                if p['Path'] == 'backup_label':
                    return p['Last-Modified']

    def __get_objects_list_on_disk(self, path, mask=None, onlyFiles=False):
        objects_list = []
        for root, dirs, files in os.walk(path):
            for filename in files:
                if mask is not None:
                    if mask not in filename:
                        continue
                objects_list.append(os.path.join(root, filename))

            if not onlyFiles:
                for dir in dirs:
                    if mask is not None:
                        if mask not in dir:
                            continue
                    objects_list.append(os.path.join(root, dir))

        return objects_list

    def __delete_local_empty_bck_dirs(self):
        arr = [self.args.path_to_full_backup_local(), self.args.local_path_to_wal_files()]
        for path in arr:
            if os.path.exists(path):
                for root, dirs, files in os.walk(path):
                    for dir in dirs:
                        dirPath = os.path.join(root, dir)
                        content = self.__get_objects_list_on_disk(dirPath, onlyFiles=True)
                        if len(content) == 0:
                            os.rmdir(dirPath)

class Manager:

    backaper = None
    cleaner = None
    args = None

    def __init__(self, new_args=None, args_in_lower_case=False):
        try:  
            self.args = Args(new_args, args_in_lower_case)
        except Exception as e:
            self.write_log('backup-', False, str(e))
        self.backaper = Backuper(self, self.args)
        self.cleaner = Cleaner(self, self.args)

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
        self.cleaner._clean_local(expire_date)
        failedToDelete = self.cleaner._clean_cloud()
        for v in failedToDelete:
            print(v)
        print(failedToDelete)

    def clean_backups(self, write_to_log_file=True, raise_exception=False):
        storage_time = self.args.storage_time()
        if storage_time is not None and storage_time > 0:
            now = datetime.datetime.now(tzlocal.get_localzone())
            expire_date = now - datetime.timedelta(seconds=storage_time)
            self.cleaner._clean_local(expire_date)

        failedToDelete = self.cleaner._clean_cloud()
        # for v in failedToDelete:
        #     print(v)
        # print(failedToDelete)

    def create_full_backup(self, write_to_log_file=True, raise_exception=False):
        try:
            self.backaper._create_full_backup()
            if write_to_log_file:
                self.write_log('backup-', True, '')
        except Exception as e:
            if write_to_log_file:
                self.write_log('backup-', False, str(e))
            if raise_exception:
                raise e

    def upload_on_yandex_cloud(self, write_to_log_file=True, raise_exception=False):
        try:
            self.backaper._create_full_backup()
            if write_to_log_file:
                self.write_log('backup-', True, '')
        except Exception as e:
            if write_to_log_file:
                self.write_log('backup-', False, str(e))
            if raise_exception:
                raise e

    def main(self):
        self.create_full_backup()
        self.upload_on_yandex_cloud()
        self.clean_backups()

    def write_log(self, file_pref, success, text=''):

        defaultPath = './PostgreSQLBackuperLogs'
        try:
            path = self.args.log_path()
            if path == '' or path is None:
                path = defaultPath
        except:
            path = defaultPath

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
