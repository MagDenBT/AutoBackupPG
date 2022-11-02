# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import os
import pathlib
import shutil
import subprocess
import traceback
from datetime import datetime
import random
import requests as requests

args = {}

def main():
    # 123
    setParam()

    message = ''
    if not checkParams(message):
        writeLog('backup-', False, message)

    try:
         createFullBackup()
        writeLog('backup-', True, '')
    except Exception as e:
        writeLog('backup-', False, str(e))

    try:
        uploadOnYandexCloud()
        writeLog('upload-', True, '')
    except Exception as e:
        writeLog('upload-', False, str(e))

def checkParams(message):
    result = True
    for item in args.items():
        if item[1] == '':
            message += f'Parameter "{item[0]}" is not set + \n'
            result = False

    return result

def setParam(newArgs=None):

    global args

    if not newArgs == None:
        args = newArgs
    else:
        # Generating Required Parameters

        # CLoud settings
        args.update({'TOKEN': '1111'})

        # Paths settings
        """
        The directory structure is as follows:
        On the local machine - disk:\rootDir\customDir\fullBpDir and for WAL-files - the one you specify
        In the cloud - /rootDir/customDir/fullBpDir and for WAL-files - /rootDir/customDir/"LastDirectoryIn(localPathToWALFiles)"
        """
        args.update({'disk': 'C'})
        args.update({'rootDir': 'Postgresql backups'})
        args.update({'customDir': 'Accounting department'})
        args.update({'fullBpDir': 'Full'})
        args.update({'localPathToWALFiles': 'C:\\pg_log_archive'})    # The path of the WAL files.(aka Incremental backups)

        # PostgreSQL settings
        args.update({'postgresqlIsntancePath': "C:\\Program Files\\PostgreSQL\\14.4-1.1C\\"})  # The path to PostgreSQL
        args.update({'postgresqlUsername': "postgres"})
        args.update({'postgresqlPassword': '1122'})

    _addParams(args)


def _addParams(args):
    args.update({'headers': {'Content-Type': 'application/json', 'Accept': 'application/json',
                            'Authorization': f'OAuth {args["TOKEN"]}'}})
    args.update({'pathToFullBackupLocal':
                f'{args["disk"]}:\\{args["rootDir"]}\\{args["customDir"]}\\{args["fullBpDir"]}'})  # The path to the permanent  directory for full backup



    # args.update({'incrBpDir': incrBpDir})

    args.update({'pathToFullBackupCloud':
                     f'/{args["rootDir"]}/{args["customDir"]}/{args["fullBpDir"]}'})

    incrBpDir = os.path.basename(args["localPathToWALFiles"])
    args.update({'pathToIncrBackupCloud':
                     f'/{args["rootDir"]}/{args["customDir"]}/{incrBpDir}'})

    if args.get('backuper') == None:
        args.update({'backuper': args["postgresqlIsntancePath"] + 'bin\\pg_basebackup.exe'})

    if args.get('tempPath') == None:
        args.update({'tempPath': './temp'})  # The path to the temporary directory for full backup

    if args.get('logPath') == None:
        args.update({'logPath': './logs'})  # The path to the script logs

    if args.get('URL') == None:
        args.update({'URL': 'https://cloud-api.yandex.net/v1/disk/resources'})

def generateLabel(millisec=False):
    if millisec:
        timeStamp = datetime.utcnow().strftime('%Y_%m_%d____%H-%M-%S.%f')[:-3]
    else:
        timeStamp = datetime.now().strftime('%Y_%m_%d____%H-%M-%S')

    # label = now.strftime(f"%Y_%m_%d____%H-%M-%S{ssss}") + '_' + str(random.randint(1, 100))
    return timeStamp + '_' + str(random.randint(1, 100))

def fileOperationsFullBackup():
    files = getFilesListOnDisk(args["tempPath"])
    if not os.path.exists(f'{args["pathToFullBackupLocal"]}\\{label}'):
        os.makedirs(f'{args["pathToFullBackupLocal"]}\\{label}')

    # move & rename
    for file in files:
        shutil.move(file, f'{args["pathToFullBackupLocal"]}\\{label}\\{label}__{os.path.basename(file)}')

def clearTempDirFullBackup():
    # clear the directory of any files
    if os.path.exists(args["tempPath"]):
        for path in os.listdir(args["tempPath"]):
            if os.path.exists(f'{args["tempPath"]}\\{path}'):
                os.remove(f'{args["tempPath"]}\\{path}')

def createFullBackup():
    global label
    label = generateLabel()

    my_env = os.environ.copy()
    my_env["PGPASSWORD"] = args["postgresqlPassword"]

    clearTempDirFullBackup()

    process = subprocess.run(
        [args["backuper"],
         '-D', args["tempPath"],
         '-X', 'fetch',
         '-F', 'tar',
         '--label', label,
         '--gzip',
         '--no-password',
         '--username', args["postgresqlUsername"],
         ],
        stderr=subprocess.PIPE,
        env=my_env,
    )
    textError = process.stderr.decode()
    if textError == "":
        fileOperationsFullBackup()
        clearTempDirFullBackup()
    else:
        raise Exception(textError)

def writeLog(filePref, success, text=''):
    result = "Success_" if success else 'FAIL_'
    if not os.path.exists(args["logPath"]):
        os.makedirs(args["logPath"])
    path = f'{args["logPath"]}\\{filePref}{result}{generateLabel(True)}.txt'
    file = open(path, "w")
    file.write(text)
    file.close()

def getFilesListOnDisk(*args):
    filesList = []

    for path in args:
        for root, dirs, files in os.walk(path):
            for filename in files:
                filesList.append(os.path.join(root,filename))

    return filesList

def getFilesToUpload(backups, pathCloud, withLastDir = True):
    toUpload = []
    for backup in backups:
        dir = os.path.dirname(backup)
        dirName = os.path.basename(dir)
        fileName = os.path.basename(backup)
        if withLastDir:
            fileName = f'{dirName}/{fileName}'

        try:
            res = requests.get(f'{args["URL"]}?path={pathCloud}/{fileName}', headers=args["headers"])
            if not res.status_code == 200:
                if res.status_code == 404:
                    toUpload.append(backup)
                else:
                    raise Exception(f'При синхронизации с облаком не удалось определить файлы для выгрузки. {res.text}')
        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')

    return toUpload

def getAvailableMemory():
    try:
        res = requests.get(f'https://cloud-api.yandex.net/v1/disk?fields=used_space%2Ctotal_space', headers=args["headers"])
        if not res.status_code == 200:
            raise Exception(f'Не удалось получить инфо о свободном месте в облаке. {res.text}')
        data = res.json()
        return data['total_space'] - data['used_space']
    except Exception as e:
        raise Exception(f'{traceback.format_exc()}\n{e}')

def prepareDirOnCloud(backups, pathCloud, withLastDir = True):

    requerdPath = pathlib.Path(pathCloud).parts
    i = 1
    step = ''
    while i < len(requerdPath):
        step += '/' + requerdPath[i]
        createDirOnCloud(step)
        i = i + 1


    if withLastDir:
        paths = []
        for backup in backups:
            dir = os.path.dirname(backup)
            dirName = os.path.basename(dir)
            paths.append(dirName)

        paths = set(paths)
        for dirName in paths:
            createDirOnCloud(f'{pathCloud}/{dirName}')

def createDirOnCloud(path):
    try:
        res = requests.put(f'{args["URL"]}?path={path}', headers=args["headers"])

        if not res.status_code == 201 and not res.status_code == 409:
            raise Exception(f'Не удалось создать каталог {path} в облаке. {res.text}')

    except Exception as e:
        raise Exception(f'{traceback.format_exc()}\n{e}')

def uploadOnYandexCloud():

    fullBackups = getFilesListOnDisk(args["pathToFullBackupLocal"])
    incrBackups = getFilesListOnDisk(args["localPathToWALFiles"])

    pathToFullBackupCloud = args["pathToFullBackupCloud"]
    pathToIncrBackupCloud = args["pathToIncrBackupCloud"]

    fullBackupsToUpload = getFilesToUpload(fullBackups, pathToFullBackupCloud)
    incrBackupsToUpload = getFilesToUpload(incrBackups, pathToIncrBackupCloud,False)

    uploadSize = 0
    for filePath in fullBackupsToUpload:
        uploadSize += os.stat(filePath).st_size

    for filePath in incrBackupsToUpload:
        uploadSize += os.stat(filePath).st_size

    if uploadSize == 0:
        writeLog('upload-', True, 'Нет новых файлов для выгрузки')
        return

    avMemory = getAvailableMemory()
    toClear = uploadSize - avMemory

    if toClear > 0:
        raise Exception(f'Недостаточно места в облаке. Требуется еще {toClear / 1024 / 1024} мб')


    prepareDirOnCloud(fullBackupsToUpload, pathToFullBackupCloud)
    prepareDirOnCloud(incrBackupsToUpload, pathToIncrBackupCloud,False)

    for backup in fullBackupsToUpload:
        dir = os.path.dirname(backup)
        dirName = os.path.basename(dir)
        filename = os.path.basename(backup)
        upload_file(backup, f'{pathToFullBackupCloud}/{dirName}/{filename}')

    for backup in incrBackupsToUpload:
        filename = os.path.basename(backup)
        upload_file(backup, f'{pathToIncrBackupCloud}/{filename}')

def create_folder(path):
    """Создание папки. \n path: Путь к создаваемой папке."""
    requests.put(f'{args["URL"]}?path={path}', headers=args["headers"])

def upload_file(loadfile, savefile, replace=False):
    """Загрузка файла.
    savefile: Путь к файлу на Диске
    loadfile: Путь к загружаемому файлу
    replace: true or false Замена файла на Диске"""
    res = requests.get(f'{args["URL"]}/upload?path={savefile}&overwrite={replace}', headers=args["headers"]).json()
    with open(loadfile, 'rb') as f:
        try:
            res = requests.put(res['href'], files={'file': f})
            if not res.status_code == 201:
                raise Exception(f'Не удалось выгрузить файл {loadfile} в облако. {res.text}')
        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
