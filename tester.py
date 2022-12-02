from PostgreSQLBackuper import Manager


def test():
    aws_access_key_id = open('./aws_access_key_id')
    aws_secret_access_key = open('./aws_secret_access_key')
    args = {
        'disk': 'C',
        'root_dir': 'Postgresql backups',
        'custom_dir': 'Отдел продаж',
        'full_bp_dir': 'Full',
        'local_path_to_wal_files': 'C:\\Postgresql backups\\pg_log_archive',
        'aws_access_key_id': aws_access_key_id.read(),
        'aws_secret_access_key': aws_secret_access_key.read(),
        'aws_bucket': 'backuptest',
    }

    m = Manager(new_args=args,use_cleaner=False,use_backuper=False, use_aws=True)
    m.upload_on_cloud(raise_exception=True)

if __name__ == '__main__':
    test()
