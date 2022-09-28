# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import os
import subprocess
from datetime import datetime
import random
import requests as requests

URL = 'https://cloud-api.yandex.net/v1/disk/resources'
TOKEN = 'AQAAAAAz55vbAAc-fohhPDQSvU5kroy21-HguNA'
headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'OAuth {TOKEN}'}
rootCloudPath = '/Postgresql backups'
fullBpCloudPath = '/Full'
incrBpCloudPath = '/Incremental'

tempPathToFullBackup = ''
pathToFullBackup = ''
pathToIncrementalBackup = ''
argUsername = ''
backuper = ''
postgresqlUsername = ''
postgresqlPassword = ''
label = ''  # Сейчас в ней нет особой надобности, но будет при реализации функционала распихивания WAL-файлов(инкрементных копий) по каталогам с полными копиями


def init():
    global tempPathToFullBackup
    global pathToFullBackup
    global pathToIncrementalBackup
    global argUsername
    global backuper
    global postgresqlPassword
    global postgresqlUsername

    postgresqlIsntancePath = "C:\\Program Files\\PostgreSQL\\14.4-1.1C\\"
    backuper = postgresqlIsntancePath + "bin\\pg_basebackup.exe"
    tempPathToFullBackup = 'F:\\pgBackIncr'
    pathToFullBackup = 'F:\\Postgresql full backups'
    pathToIncrementalBackup = 'F:\\pg_log_archive'
    postgresqlUsername = "postgres"
    postgresqlPassword = '1122'


def generateLabel():
    now = datetime.now()
    label = now.strftime("%Y_%m_%d__%H-%M-%S") + '_' + str(random.randint(1, 100))
    return label


def fileOperationsFullBackup():
    files = ['backup_manifest', "base.tar" if os.path.exists(tempPathToFullBackup + '\\base.tar') else "base.tar.gz"]
    if not os.path.exists(f'{pathToFullBackup}\\{label}'):
        os.makedirs(f'{pathToFullBackup}\\{label}')

    # move & rename
    for file in files:
        os.replace(f'{tempPathToFullBackup}\\{file}', f'{pathToFullBackup}\\{label}\\{label}__{file}')


def clearTempDirFullBackup():
    # clear the directory of any files
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

    if not process.stderr or 'exists but is not empty' not in process.stderr:
        fileOperationsFullBackup()
        clearTempDirFullBackup()
        uploadOnYandexCloud()

def writeLog(text):
    print(text)


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
            res = requests.get(f'{URL}/upload?path=/{rootDir}/{dirName}/{fileName}', headers=headers)
            if not res.status_code == 200:
                if not res.status_code == 404:
                    raise Exception('Не удалось расчитать файлы для выгрузки в облако')
            toUpload.append(backup)
        except Exception:
            raise Exception('Не удалось расчитать файлы для выгрузки в облако')

    return toUpload


def uploadOnYandexCloud():

    error = ''

    fullBackups = getFilesListOnDisk(pathToFullBackup)
    incrBackups = getFilesListOnDisk(pathToIncrementalBackup)

    fullBackupsToUpload = getFilesToUpload(fullBackups,fullBpCloudPath)
    incrBackupsToUpload = getFilesToUpload(fullBackups,incrBpCloudPath)

    uploadSize = 0
    for filePath1,filePath2 in fullBackupsToUpload,incrBackupsToUpload:
        uploadSize = + os.stat(filePath1).st_size
        uploadSize = + os.stat(filePath2).st_size

    avMemory = getAvailableMemory()

    if uploadSize ==0:
        writeLog('Нет новых файлов для выгрузки')
        return


    toClear = uploadSize - avMemory
    if not toClear > 0:
        raise (f'Недостаточно места в облаке. Требуется еще {toClear/1024/1024} мб')



    prepareDirOnCloud(errorText = error)


    for filePath in  filesToUpload:
        upload_file(,filePath)



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
            requests.put(res['href'], files={'file': f})
        except KeyError:
            writeLog(res)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    init()
    createFullBackup()
