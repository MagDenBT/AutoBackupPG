import json
import os
from datetime import datetime

args = {}


def deleteListForWAL(path : str, maxWAL:str):
    if os.path.exists(path):
        for path in os.listdir(args["tempPath"]):
            if os.path.exists(f'{args["tempPath"]}\\{path}'):
                os.remove(f'{args["tempPath"]}\\{path}')

def printOb(list : {}):
    for ob in list:
        print(ob)


def getObjectsListOnDisk(path):
    ObjectsList = []

    for root, dirs, files in os.walk(path):
        for filename in files:
            ObjectsList.append(os.path.join(root,filename))

    return ObjectsList

def readCreateDate(backupManifest:str):
    with open(backupManifest) as json_file:
        data = json.load(json_file)
        for p in data['Files']:
           if p['Path'] == 'backup_label':
               return p['Last-Modified']

def test():
    src = [0x88,0x89,0x8A,0x8B,0x8C,0x8D,0x8E,0x8F,0x90]
    for n in src:

        print(int(n))


def main():
    l = getObjectsListOnDisk("F:\\Postgresql backups\\Отдел продаж\\Full")
    result = []
    for file in l:
        if 'backup_manifest' in os.path.basename(file):
         dateStr = readCreateDate(file)

         date = datetime.date(dateStr)
         result.append({'file': file,'date':date})
    print(result)

if __name__ == "__main__":
    main()