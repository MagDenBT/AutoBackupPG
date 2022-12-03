import datetime

from dateutil import parser

from PostgreSQLBackuper import Manager


def test():
    aws_access_key_id = open('./aws_access_key_id')
    aws_secret_access_key = open('./aws_secret_access_key')
    token = open('./token')
    args = {
        'disk': 'C',
        'root_dir': 'Postgresql backups',
        'custom_dir': 'Отдел продаж',
        'full_bp_dir': 'Full',
        'local_path_to_wal_files': 'C:\\Postgresql backups\\pg_log_archive',
        'aws_access_key_id': aws_access_key_id.read(),
        'aws_secret_access_key': aws_secret_access_key.read(),
        'aws_bucket': 'backuptest',
        'storage_time': 20*60,
        'postgresql_isntance_path': 'C:\\Program Files\\PostgreSQL\\14.4-1.1C\\',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'cloud_token': token.read()
    }
    now = parser.parse("26.11.2022 00:00:00 +3")
    # Test.Delete before release



    m = Manager(new_args=args, use_cleaner=True, use_backuper=True, use_yandex=True)
    m.create_full_backup(raise_exception=True)
    m.upload_on_cloud(raise_exception=True)
    storage_time = 1565100
    expire_date = now - datetime.timedelta(seconds=storage_time)
    m.clean_backups(raise_exception=True)


if __name__ == '__main__':
    test()
