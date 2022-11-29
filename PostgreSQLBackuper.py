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


class Manager:
    backaper = None
    cleaner = None
    args = {}

    def __init__(self, newArgs=None, ArgsInLowerCase=False):
        self.backaper = self.Backuper(self)
        self.cleaner = self.Cleaner(self)
        self.setParam(newArgs, ArgsInLowerCase)
        message = ''
        if not self._checkParams(message):
            self.writeLog('backup-', False, message)

    def _test(self):
        file = open('./token')
        self.args.update({'CLOUDTOKEN': file.read()})

        self.args.update({'disk': 'C'})
        self.args.update({'rootDir': 'Postgresql backups'})
        self.args.update({'customDir': 'Отдел продаж'})
        self.args.update({'fullBpDir': 'Full'})
        self.args.update({'localPathToWALFiles': 'C:\\Postgresql backups\\pg_log_archive'})
        self.__add_internal_params()

        now = parser.parse("26.11.2022 00:00:00 +3")
        # Test.Delete before release

        storage_time = 1565100
        expire_date = now - datetime.timedelta(seconds=storage_time)
        self.cleaner._cleanLocal(expire_date)
        failedToDelete = self.cleaner._clean_cloud()
        for v in failedToDelete:
            print(v)
        print(failedToDelete)

    def clean_backups(self, writetologfile=True, raiseException=False):
        storage_time = self.get_param("storageTime")
        if storage_time is not None and storage_time > 0:
            now = datetime.datetime.now(tzlocal.get_localzone())
            expire_date = now - datetime.timedelta(seconds=storage_time)
            self.cleaner._cleanLocal(expire_date)

        failedToDelete = self.cleaner._clean_cloud()
        # for v in failedToDelete:
        #     print(v)
        # print(failedToDelete)

    def create_full_backup(self, writetologfile=True, raiseException=False):
        try:
            self.backaper._create_full_backup()
            if writetologfile:
                self.writeLog('backup-', True, '')
        except Exception as e:
            if writetologfile:
                self.writeLog('backup-', False, str(e))
            if raiseException:
                raise e

    def upload_on_yandex_cloud(self, writetologfile=True, raiseException=False):
        try:
            self.backaper._create_full_backup()
            if writetologfile:
                self.writeLog('backup-', True, '')
        except Exception as e:
            if writetologfile:
                self.writeLog('backup-', False, str(e))
            if raiseException:
                raise e

    def main(self):
        self.create_full_backup()
        self.upload_on_yandex_cloud()
        self.clean_backups()

    def _checkParams(self, message):
        result = True
        for item in self.args.items():
            if item[1] == '' or item[1] == None:
                message += f'Parameter "{item[0]}" is not set + \n'
                result = False

        return result

    def setParam(self, newArgs=None, inLowerCase=False):

        self.__generate_params()

        if not newArgs == None:
            for key in self.args.keys():
                key2 = str.lower(key) if inLowerCase else key
                value = str(newArgs.get(key2))
                self.args.update({key: value})

        self.__add_internal_params()

    def __generate_params(self):
        # Generating Required Parameters
        # CLoud settings
        self.args.update({'CLOUDTOKEN': '1111'})
        # Paths settings
        """
                The directory structure is as follows:
                On the local machine - disk:\rootDir\customDir\fullBpDir and for WAL-files - the one you specify
                In the cloud - /rootDir/customDir/fullBpDir and for WAL-files - /rootDir/customDir/"LastDirectoryIn(localPathToWALFiles)"
                """
        self.args.update({'disk': 'C'})
        self.args.update({'rootDir': 'Postgresql backups'})
        self.args.update({'customDir': 'Accounting department'})
        self.args.update({'fullBpDir': 'Full'})
        self.args.update(
            {'localPathToWALFiles': 'C:\\pg_log_archive'})  # The path of the WAL files.(aka Incremental backups)
        # PostgreSQL settings
        self.args.update(
            {'postgresqlIsntancePath': "C:\\Program Files\\PostgreSQL\\14.4-1.1C\\"})  # The path to PostgreSQL
        self.args.update({'postgresqlUsername': "postgres"})
        self.args.update({'postgresqlPassword': '1122'})
        self.args.update({'backuper': None})
        self.args.update({'tempPath': None})
        self.args.update({'storageTime': 0})
        self.args.update({'logPath': None})
        # args.update({'URL': None})

    def __add_internal_params(self):
        self.args.update({'headers': {'Content-Type': 'application/json', 'Accept': 'application/json',
                                      'Authorization': f'OAuth {self.get_param("CLOUDTOKEN")}'}})
        self.args.update({
            'pathToFullBackupLocal': f'{self.get_param("disk")}:\\{self.get_param("rootDir")}\\{self.get_param("customDir")}\\{self.get_param("fullBpDir")}'})  # The path to the permanent  directory for full backup

        # args.update({'incrBpDir': incrBpDir})

        self.args.update({'pathToFullBackupCloud':
                              f'/{self.get_param("rootDir")}/{self.get_param("customDir")}/{self.get_param("fullBpDir")}'})

        incrBpDir = os.path.basename(self.get_param("localPathToWALFiles"))
        self.args.update({'pathToIncrBackupCloud':
                              f'/{self.get_param("rootDir")}/{self.get_param("customDir")}/{incrBpDir}'})

        self.args.update({'label': self.__generate_label()})

        if self.args.get('backuper') == None or self.args.get('backuper') == '':
            self.args.update({'backuper': self.get_param("postgresqlIsntancePath") + 'bin\\pg_basebackup.exe'})

        if self.args.get('tempPath') == None or self.args.get('tempPath') == '':
            self.args.update({'tempPath': './temp'})  # The path to the temporary directory for full backup

        if self.args.get('logPath') == None or self.args.get('logPath') == '':
            self.args.update({'logPath': './PostgreSQLBackuperLogs'})  # The path to the script logs

        if self.args.get('URL') == None or self.args.get('URL') == '':
            var = 'https://cloud-api.yandex.net/v1/disk/resources'
            self.args.update({'URL': var})

    def writeLog(self, filePref, success, text=''):

        defaultPath = './PostgreSQLBackuperLogs'
        try:
            path = self.get_param('logPath')
            if path == '' or path is None:
                path = defaultPath
        except:
            path = defaultPath

        if not os.path.exists(path):
            os.makedirs(path)

        try:
            label = self.get_param('label')
            if label == '' or label is None:
                label = self.__generate_label()
        except:
            label = self.__generate_label()

        result = "Success_" if success else 'FAIL_'
        path = f'{path}\\{filePref}{result}{label}.txt'
        file = open(path, "w")
        file.write(text)
        file.close()

    def __generate_label(self, millisec=False):
        if millisec:
            timeStamp = datetime.datetime.utcnow().strftime('%Y_%m_%d____%H-%M-%S.%f')[:-3]
        else:
            timeStamp = datetime.datetime.now().strftime('%Y_%m_%d____%H-%M-%S')

        return timeStamp + '_' + str(random.randint(1, 100))

    def get_param(self, key: str):
        return self.args.get(key)

    class Backuper:
        manager = None

        def __init__(self, manager):
            self.manager = manager

        def __file_operations_full_backup(self):
            files = self.__get_files_list_on_disk(self.manager.get_param("tempPath"))
            if not os.path.exists(f'{self.manager.get_param("pathToFullBackupLocal")}\\{label}'):
                os.makedirs(f'{self.manager.get_param("pathToFullBackupLocal")}\\{label}')

            # move & rename
            for file in files:
                shutil.move(file,
                            f'{self.manager.get_param("pathToFullBackupLocal")}\\{label}\\{label}__{os.path.basename(file)}')

        def __clear_temp_dir_full_backup(self):
            # clear the directory of any files
            if os.path.exists(self.manager.get_param("tempPath")):
                for path in os.listdir(self.manager.get_param("tempPath")):
                    if os.path.exists(f'{self.manager.get_param("tempPath")}\\{path}'):
                        os.remove(f'{self.manager.get_param("tempPath")}\\{path}')

        def _create_full_backup(self):
            global label
            label = Manager.__generate_label()

            my_env = os.environ.copy()
            my_env["PGPASSWORD"] = self.manager.get_param("postgresqlPassword")

            self.__clear_temp_dir_full_backup()

            process = subprocess.run(
                [self.manager.get_param("backuper"),
                 '-D', self.manager.get_param("tempPath"),
                 '-X', 'fetch',
                 '-F', 'tar',
                 '--label', label,
                 '--gzip',
                 '--no-password',
                 '--username', self.manager.get_param("postgresqlUsername"),
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
                    res = requests.get(f'{self.manager.get_param("URL")}?path={pathCloud}/{fileName}',
                                       headers=self.manager.get_param("headers"))
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
                                   headers=self.manager.get_param("headers"))
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
                res = requests.put(f'{self.manager.get_param("URL")}?path={path}',
                                   headers=self.manager.get_param("headers"))

                if not res.status_code == 201 and not res.status_code == 409:
                    raise Exception(f'Не удалось создать каталог {path} в облаке. {res.text}')

            except Exception as e:
                raise Exception(f'{traceback.format_exc()}\n{e}')

        def _upload_on_yandex_cloud(self):
            fullBackups = self.__get_files_list_on_disk(self.manager.get_param("pathToFullBackupLocal"))
            incrBackups = self.__get_files_list_on_disk(self.manager.get_param("localPathToWALFiles"))

            pathToFullBackupCloud = self.manager.get_param("pathToFullBackupCloud")
            pathToIncrBackupCloud = self.manager.get_param("pathToIncrBackupCloud")

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
            requests.put(f'{self.manager.get_param("URL")}?path={path}', headers=self.manager.get_param("headers"))

        def __upload_file(self, loadfile, savefile, replace=False):
            """Загрузка файла.
            savefile: Путь к файлу на Диске
            loadfile: Путь к загружаемому файлу
            replace: true or false Замена файла на Диске"""
            res = requests.get(f'{self.manager.get_param("URL")}/upload?path={savefile}&overwrite={replace}',
                               headers=self.manager.get_param("headers")).json()
            with open(loadfile, 'rb') as f:
                try:
                    res = requests.put(res['href'], files={'file': f})
                    if not res.status_code == 201:
                        raise Exception(f'Не удалось выгрузить файл {loadfile} в облако. {res.text}')
                except Exception as e:
                    raise Exception(f'{traceback.format_exc()}\n{e}')

    class Cleaner:
        manager = None

        def __init__(self, manager):
            self.manager = manager

        def __delete_obj_on_cloud(self, path, permanently=True):
            result = False
            try:
                res = requests.delete(f'{self.manager.get_param("URL")}?path={path}&permanently={permanently}',
                                      headers=self.manager.get_param("headers"))
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
                res = requests.get(f'{self.manager.get_param("URL")}?path={path}&limit={limit}',
                                   headers=self.manager.get_param("headers"))
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
            localPaths = [self.manager.get_param("pathToFullBackupLocal"),
                          self.manager.get_param("localPathToWALFiles")]
            existingFilesMD5 = []
            extraBck = []

            for path in localPaths:
                files = self.__get_objects_list_on_disk(path, onlyFiles=True)
                hashes = self.__get_md5(files)
                existingFilesMD5.extend(hashes)

            try:
                limit = 1000000000
                res = requests.get(f'{self.manager.get_param("URL")}/files?preview_crop=true&sort=path&limit={limit}',
                                   headers=self.manager.get_param("headers"))
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
            rootDirs = [self.manager.get_param("pathToFullBackupCloud"),
                        self.manager.get_param("pathToIncrBackupCloud")]
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

        def _cleanLocal(self, expire_date):
            full_bck = self.__full_bck_to_remove(expire_date)
            for obj in full_bck:
                os.remove(obj)

            mask = '__backup_manifest'
            full_bck = self.__get_objects_list_on_disk(self.manager.get_param('pathToFullBackupLocal'), mask)

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
                inc_bck = self.__inc_bck_to_remove(self.manager.get_param("localPathToWALFiles"), oldest_label)
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
            fullBck = self.__get_objects_list_on_disk(self.manager.get_param('pathToFullBackupLocal'))
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
            arr = [self.manager.get_param('pathToFullBackupLocal'), self.manager.get_param('localPathToWALFiles')]
            for path in arr:
                if os.path.exists(path):
                    for root, dirs, files in os.walk(path):
                        for dir in dirs:
                            dirPath = os.path.join(root, dir)
                            content = self.__get_objects_list_on_disk(dirPath, onlyFiles=True)
                            if len(content) == 0:
                                os.rmdir(dirPath)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    m = Manager()
    m._test()
