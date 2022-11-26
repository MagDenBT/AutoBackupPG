import hashlib
import json
import os
import requests
import traceback
from datetime import datetime
from dateutil import parser
import tzlocal

args = {}


def main():
    storage_time = 1000

    now = datetime.now(tzlocal.get_localzone())
    expire_date = now - datetime.timedelta(seconds=storage_time)
    cleanLocal(expire_date)
    failedToDelete = cleanCloud()


def cleanCloud():
    extraBck = getExtraBckOnCloud()
    failedToDelete = []
    permanently = False
    for bck in extraBck:
        try:
            res = requests.get(f'{args["URL"]}?path={bck}&permanently={permanently}', headers=args["headers"])
            if not res.status_code == 202 or res.status_code == 204:
                failedToDelete.append(bck)
        except Exception as e:
            raise Exception(f'{traceback.format_exc()}\n{e}')
    return failedToDelete




def getExtraBckOnCloud():
    localPaths = [args["pathToFullBackupCloud"], args["pathToIncrBackupCloud"]]
    existingFilesMD5 = []
    extraBck = []

    for path in localPaths:
        files = getObjectsListOnDisk(path, onlyFiles=True)
        hashes = getMD5(files)
        existingFilesMD5.extend(hashes)

    try:
        res = requests.get(f'{args["URL"]}/files?preview_crop=true&sort=path', headers=args["headers"])
        if not res.status_code == 200:
            raise Exception(f'Очитка облака. Не удалось получить список файлов в облаке. {res.text}')
        data = res.json()
    except Exception as e:
        raise Exception(f'{traceback.format_exc()}\n{e}')

    for item in data.items:
        isBckDir = False
        for localPath in localPaths:
            if localPath in item.path:
                isBckDir = True
        if isBckDir:
            try:
                existingFilesMD5.index(item.md5)
                continue
            except ValueError:
                extraBck.append(item.path)

    return extraBck



def getMD5(files):
    hashes = []
    for path in files:
        hash_md5 = hashlib.md5()
        with open(path, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash_md5.update(chunk)
        hashes.append(hash_md5.hexdigest())
    return hashes

def cleanLocal(expire_date):
    full_bck = fullBckToRemove(expire_date)
    for obj in full_bck:
        os.remove(obj)

    mask = '__backup_manifest'
    full_bck = getObjectsListOnDisk(args.get('pathToFullBackupLocal'), mask)
    oldest_date = datetime.now(tzlocal.get_localzone())

    oldest_label = None
    for file in full_bck:
        fileName = os.path.basename(file)
        if mask in fileName:
            date_str = readCreateDate(file)
            bck_date = parser.parse(date_str)
            if oldest_date >= bck_date:
                oldest_date = bck_date
                oldest_label = fileName.replace(mask, '')

    if oldest_label is not None:
        inc_bck = incBckToRemove(oldest_label)
        for obj in inc_bck:
            os.remove(obj)

    deleteLocalEmptyBckDirs()


def numberIncBck(path, label):
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


def incBckToRemove(path, oldest_label, delete_unsuitable=False):
    oldest_number = numberIncBck(path, oldest_label)
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


def fullBckToRemove(exprireDate: datetime):
    fullBck = getObjectsListOnDisk(args.get('pathToFullBackupLocal'))
    result = []
    mask = '__backup_manifest'
    for file in fullBck:
        if mask in os.path.basename(file):
            dateStr = readCreateDate(file)
            bckDate = parser.parse(dateStr)
            if exprireDate <= bckDate:
                portion = getObjectsListOnDisk(os.path.dirname(file), mask)
                result.extend(portion)
    return result


def readCreateDate(backupManifest: str):
    with open(backupManifest) as json_file:
        data = json.load(json_file)
        for p in data['Files']:
            if p['Path'] == 'backup_label':
                return p['Last-Modified']


def getObjectsListOnDisk(path, mask=None, onlyFiles=False):
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


def deleteLocalEmptyBckDirs():
    arr = [args.get('pathToFullBackupLocal'), args.get('localPathToWALFiles')]
    for path in arr:
        if os.path.exists(path):
            for root, dirs, files in os.walk(path):
                for dir in dirs:
                    content = getObjectsListOnDisk(os.path.join(root, dir), onlyFiles=True)
                    if len(content) == 0:
                        os.remove(dir)


if __name__ == "__main__":
    main()
