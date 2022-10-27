# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# Этот модуль требуется импортировать в начале каждого скрипта.
from lib.common import bootstrap
# Экспорт базового класса для сценария. Он отвечает за настройку логгирования,
# базовую валидацию параметров, перехват исключений и корректное завершение
# скрипта и т.д.
from lib.common.base_scenario import BaseScenario
# Экспорт класса ошибок.
from lib.common.errors import SACError

import os
import subprocess
from datetime import datetime
import random
import requests as requests

URL = 'https://cloud-api.yandex.net/v1/disk/resources'
TOKEN = '1111'
headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'OAuth {TOKEN}'}
rootCloudPath = 'Postgresql backups'
fullBpCloudPath = 'Full'
incrBpCloudPath = 'Incremental'

postgresqlIsntancePath = "C:\\Program Files\\PostgreSQL\\14.4-1.1C\\"
backuper = postgresqlIsntancePath + "bin\\pg_basebackup.exe"
tempPathToFullBackup = 'F:\\pgBackIncr'
pathToFullBackup = 'F:\\Postgresql full backups'
pathToIncrementalBackup = 'F:\\pg_log_archive'
postgresqlUsername = "postgres"
postgresqlPassword = '1122'
logPath = 'F:\\pgBackupLog'

def generateLabel(millisec = False):

    if millisec:
        timeStamp = datetime.utcnow().strftime('%Y_%m_%d____%H-%M-%S.%f')[:-3]
    else:
        timeStamp = datetime.now().strftime('%Y_%m_%d____%H-%M-%S')

    # label = now.strftime(f"%Y_%m_%d____%H-%M-%S{ssss}") + '_' + str(random.randint(1, 100))
    return timeStamp + '_' + str(random.randint(1, 100))


def fileOperationsFullBackup():
    files = ['backup_manifest', "base.tar" if os.path.exists(tempPathToFullBackup + '\\base.tar') else "base.tar.gz"]
    if not os.path.exists(f'{pathToFullBackup}\\{label}'):
        os.makedirs(f'{pathToFullBackup}\\{label}')

    # move & rename
    for file in files:
        os.replace(f'{tempPathToFullBackup}\\{file}', f'{pathToFullBackup}\\{label}\\{label}__{file}')


def clearTempDirFullBackup():
    # clear the directory of any files
    if os.path.exists(tempPathToFullBackup):
        for path in os.listdir(tempPathToFullBackup):
            if os.path.exists(f'{tempPathToFullBackup}\\{path}'):
                os.remove(f'{tempPathToFullBackup}\\{path}')


def createFullBackup():
    global label
    label = generateLabel()

    my_env = os.environ.copy()
    my_env["PGPASSWORD"] = postgresqlPassword

    clearTempDirFullBackup()

    process = subprocess.run(
        [backuper,
         '-D', tempPathToFullBackup,
         '-X', 'fetch',
         '-F', 'tar',
         '--label', label,
         '--gzip',
         '--no-password',
         '--username', postgresqlUsername,
         ],
        stderr=subprocess.PIPE,
        env=my_env,
    )
    textError = process.stderr.decode()
    if textError == "":
        fileOperationsFullBackup()
        clearTempDirFullBackup()
        writeLog('backup-', True)
    else:
        writeLog('backup-', False,textError)

def writeLog(filePref,success,text = ''):
    result = "Success_" if success else 'FAIL_'
    if not os.path.exists(logPath):
        os.makedirs(logPath)
    path = f'{logPath}\\{filePref}{result}{generateLabel(True)}.txt'
    file = open(path, "w")
    file.write(text)
    file.close()


def getFilesListOnDisk(*args):
    filesList = []

    for path in args:

        for root, dirs, files in os.walk(path):
            for filename in files:
                filesList.append(root + '\\' + filename)

            for dir in dirs:
                filesList.extend(getFilesListOnDisk(root + '\\' + dir))

    return filesList


def getFilesToUpload(backups,rootDir):
    toUpload = []
    for backup in backups:
        dir = os.path.dirname(backup)
        dirName = os.path.basename(dir)
        fileName = os.path.basename(backup)


        try:
            res = requests.get(f'{URL}?path=/{rootCloudPath}/{rootDir}/{dirName}/{fileName}', headers=headers)
            if not res.status_code == 200:
                if res.status_code == 404:
                    toUpload.append(backup)
                else:
                    raise Exception(f'При синхронизации с облаком не удалось определить файлы для выгрузки. {res.text}')
        except Exception as e:
            raise Exception(e)

    return toUpload


def getAvailableMemory():
    try:
        res = requests.get(f'https://cloud-api.yandex.net/v1/disk?fields=used_space%2Ctotal_space', headers=headers)
        if not res.status_code == 200:
            raise Exception(f'Не удалось получить инфо о свободном месте в облаке. {res.text}')
        data = res.json()
        return data['total_space'] - data['used_space']
    except Exception as e:
            raise Exception(e)


def prepareDirOnCloud(backups,rootDir):
    paths = []
    for backup in backups:
        dir = os.path.dirname(backup)
        dirName = os.path.basename(dir)
        paths.append(dirName)

    paths = set(paths)
    for dirName in paths:
        step = '/'+ rootCloudPath
        createDirOnCloud(step)
        step += "/"+ rootDir
        createDirOnCloud(step)
        step += "/"+ dirName
        createDirOnCloud(step)


def createDirOnCloud(path):
    try:
        res = requests.put(f'{URL}?path={path}', headers=headers)

        if not res.status_code == 201 and not res.status_code == 409:
            raise Exception(f'Не удалось создать каталог {path} в облаке. {res.text}')

    except Exception as e:
            raise Exception(e)


def uploadOnYandexCloud():


    fullBackups = getFilesListOnDisk(pathToFullBackup)
    incrBackups = getFilesListOnDisk(pathToIncrementalBackup)

    fullBackupsToUpload = getFilesToUpload(fullBackups,fullBpCloudPath)
    incrBackupsToUpload = getFilesToUpload(incrBackups,incrBpCloudPath)

    uploadSize = 0
    for filePath in fullBackupsToUpload:
        uploadSize += os.stat(filePath).st_size

    for filePath in incrBackupsToUpload:
        uploadSize += os.stat(filePath).st_size

    if uploadSize == 0:
        writeLog('upload-',True,'Нет новых файлов для выгрузки')
        return

    avMemory = getAvailableMemory()
    toClear = uploadSize - avMemory

    if toClear > 0:
        raise Exception(f'Недостаточно места в облаке. Требуется еще {toClear/1024/1024} мб')


    prepareDirOnCloud(fullBackupsToUpload,fullBpCloudPath)
    prepareDirOnCloud(incrBackupsToUpload,incrBpCloudPath)

    for backup in fullBackupsToUpload:
        dir = os.path.dirname(backup)
        dirName = os.path.basename(dir)
        filename = os.path.basename(backup)
        upload_file(backup,f'/{rootCloudPath}/{fullBpCloudPath}/{dirName}/{filename}')

    for backup in incrBackupsToUpload:
        dir = os.path.dirname(backup)
        dirName = os.path.basename(dir)
        filename = os.path.basename(backup)
        upload_file(backup,f'/{rootCloudPath}/{incrBpCloudPath}/{dirName}/{filename}')



def create_folder(path):
    """Создание папки. \n path: Путь к создаваемой папке."""
    requests.put(f'{URL}?path={path}', headers=headers)


def upload_file(loadfile, savefile, replace=False):
    """Загрузка файла.
    savefile: Путь к файлу на Диске
    loadfile: Путь к загружаемому файлу
    replace: true or false Замена файла на Диске"""
    res = requests.get(f'{URL}/upload?path={savefile}&overwrite={replace}', headers=headers).json()
    with open(loadfile, 'rb') as f:
        try:
           res = requests.put(res['href'], files={'file': f})
           if not res.status_code == 201:
               raise Exception(f'Не удалось выгрузить файл {loadfile} в облако. {res.text}')
        except Exception as e:
            raise Exception(e)

    ## Метод, реализующий основную логику. Он запускается в случае, если
    # test-mode == False.
    def _real(self):
        createFullBackup()
        try:
            uploadOnYandexCloud()
            writeLog('upload-', True, '')
        except Exception as e:
            writeLog('upload-', False, str(e))


i# Позволяет запускать сценарий, если данный файл был запущен напрямую.
if __name__ == "__main__":
    PGBackupScenario.main()

