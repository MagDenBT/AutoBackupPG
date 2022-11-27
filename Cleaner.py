import PostgreSQLBackuper
import hashlib
import json
import os
import requests
import traceback
import datetime
from dateutil import parser
import tzlocal

args = {}


def main():
    # Test.Delete before release
    global args
    PostgreSQLBackuper.setParam()
    file = open('./token')
    PostgreSQLBackuper.args.update({'CLOUDTOKEN': file.read()})
    PostgreSQLBackuper.args.update({'disk': 'C'})
    PostgreSQLBackuper.args.update({'rootDir': 'Postgresql backups'})
    PostgreSQLBackuper.args.update({'customDir': 'Sales dep'})
    PostgreSQLBackuper.args.update({'fullBpDir': 'Full'})
    PostgreSQLBackuper.args.update({'localPathToWALFiles': 'C:\\Postgresql backups\\pg_log_archive'})
    PostgreSQLBackuper.__add_params(PostgreSQLBackuper.args)

    args = PostgreSQLBackuper.args
    now = parser.parse("26.11.2022 00:00:00 +3")
    # Test.Delete before release

    storage_time = 1565100

    # now = datetime.datetime.now(tzlocal.get_localzone())
    expire_date = now - datetime.timedelta(seconds=storage_time)

    cleanLocal(expire_date)
    failedToDelete = clean_cloud()
    for v in failedToDelete:
        print(v)
    print(failedToDelete)



def __delete_obj_on_cloud(path, permanently=True):
    result = False
    try:
        res = requests.delete(f'{args["URL"]}?path={path}&permanently={permanently}', headers=args["headers"])
        if not res.status_code == 202 or not res.status_code == 204:
            result = True
    except Exception as e:
        raise Exception(f'{traceback.format_exc()}\n{e}')
    return result


def __delete_cloud_empty_bck_dirs(keepRoots=True):
    rootDirs = set(__get_root_dirs())
    emptyDirs = []
    for dir in rootDirs:
        path = '/' + dir
        __empty_cloud_dirs(path, emptyDirs)

    if keepRoots:
        for dir in rootDirs:
            path = '/' + dir
            try:
                emptyDirs.remove(path)
            except ValueError:
                continue
    failedToDelete = []
    emptyDirs = __optimize_remove_list(emptyDirs)
    for path in emptyDirs:
        if not __delete_obj_on_cloud(path, False):
            failedToDelete.append(path)
    return failedToDelete


def __optimize_remove_list(emptyDirs):
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


def __empty_cloud_dirs(path, emptyDirs: []):
    try:
        limit = 1000000000
        res = requests.get(f'{args["URL"]}?path={path}&limit={limit}',
                           headers=args["headers"])
        if not res.status_code == 200:
            raise Exception(f'Удаление пустых папок в облаке. Не удалось получить список папок в облаке. {res.text}')
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
            __empty_cloud_dirs(subPath, emptyDirs)


def clean_cloud():
    extraBck = __get_extra_bck_on_cloud()
    failedToDelete = []
    permanently = False
    for bck in extraBck:
        if not __delete_obj_on_cloud(bck, permanently):
            failedToDelete.append(bck)

    info = __delete_cloud_empty_bck_dirs()
    failedToDelete.extend(info)
    return failedToDelete


def __get_extra_bck_on_cloud():
    localPaths = [args["pathToFullBackupLocal"], args["localPathToWALFiles"]]
    existingFilesMD5 = []
    extraBck = []

    for path in localPaths:
        files = __get_objects_list_on_disk(path, onlyFiles=True)
        hashes = __get_md5(files)
        existingFilesMD5.extend(hashes)

    try:
        limit = 1000000000
        res = requests.get(f'{args["URL"]}/files?preview_crop=true&sort=path&limit={limit}', headers=args["headers"])
        if not res.status_code == 200:
            raise Exception(f'Очитка облака. Не удалось получить список файлов в облаке. {res.text}')
        data = res.json()
    except Exception as e:
        raise Exception(f'{traceback.format_exc()}\n{e}')

    rootDirs = __get_root_dirs()

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


def __get_root_dirs():
    rootDirs = [args["pathToFullBackupCloud"], args["pathToIncrBackupCloud"]]
    for i, val in enumerate(rootDirs):
        rootDirs[i] = val.split("/")[1]
    return rootDirs


def __get_md5(files):
    hashes = []
    for path in files:
        hash_md5 = hashlib.md5()
        with open(path, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash_md5.update(chunk)
        hashes.append(hash_md5.hexdigest())
    return hashes


def cleanLocal(expire_date):
    full_bck = __full_bck_to_remove(expire_date)
    for obj in full_bck:
        os.remove(obj)

    mask = '__backup_manifest'
    full_bck = __get_objects_list_on_disk(args.get('pathToFullBackupLocal'), mask)

    oldest_date = datetime.datetime.now(tzlocal.get_localzone())
    oldest_label = None
    for file in full_bck:
        fileName = os.path.basename(file)
        if mask in fileName:
            date_str = __read_create_date(file)
            bck_date = parser.parse(date_str)
            if oldest_date >= bck_date:
                oldest_date = bck_date
                oldest_label = fileName.replace(mask, '')

    if oldest_label is not None:
        inc_bck = __inc_bck_to_remove(args["localPathToWALFiles"], oldest_label)
        for obj in inc_bck:
            os.remove(obj)

    __delete_local_empty_bck_dirs()


def __number_inc_bck(path, label):
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


def __inc_bck_to_remove(path, oldest_label, delete_unsuitable=False):
    oldest_number = __number_inc_bck(path, oldest_label)
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


def __full_bck_to_remove(exprireDate: datetime):
    fullBck = __get_objects_list_on_disk(args.get('pathToFullBackupLocal'))
    result = []
    mask = '__backup_manifest'
    for file in fullBck:
        fileName = os.path.basename(file)
        if mask in fileName:
            dateStr = __read_create_date(file)
            bckDate = parser.parse(dateStr)
            if bckDate <= exprireDate:
                portion = __get_objects_list_on_disk(os.path.dirname(file), fileName.split(mask)[0])
                result.extend(portion)
    return result


def __read_create_date(backupManifest: str):
    with open(backupManifest) as json_file:
        data = json.load(json_file)
        for p in data['Files']:
            if p['Path'] == 'backup_label':
                return p['Last-Modified']


def __get_objects_list_on_disk(path, mask=None, onlyFiles=False):
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


def __delete_local_empty_bck_dirs():
    arr = [args.get('pathToFullBackupLocal'), args.get('localPathToWALFiles')]
    for path in arr:
        if os.path.exists(path):
            for root, dirs, files in os.walk(path):
                for dir in dirs:
                    dirPath = os.path.join(root, dir)
                    content = __get_objects_list_on_disk(dirPath, onlyFiles=True)
                    if len(content) == 0:
                        os.rmdir(dirPath)


if __name__ == "__main__":
    main()
