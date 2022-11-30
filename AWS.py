import boto3

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id = '',
    aws_secret_access_key = '',
)
# Получить список объектов в бакете
for key in s3.list_objects(Bucket='backuptest')['Contents']:
    print(key['Key'])

# Создать новый бакет
# s3.create_bucket(Bucket='bucket-name')

# Загрузить объекты в бакет

## Из строки
# s3.put_object(Bucket='bucket-name', Key='object_name', Body='TEST', StorageClass='COLD')
#
# ## Из файла
# s3.upload_file('this_script.py', 'bucket-name', 'py_script.py')
# s3.upload_file('this_script.py', 'bucket-name', 'script/py_script.py')


# Удалить несколько объектов
forDeletion = [{'Key':'Postgresql backups/2022_11_07____21-15-44_20__backup_manifest'}]
response = s3.delete_objects(Bucket='backuptest', Delete={'Objects': forDeletion})
print(str(response))
# Получить объект
# get_object_response = s3.get_object(Bucket='bucket-name',Key='py_script.py')
# print(get_object_response['Body'].read())