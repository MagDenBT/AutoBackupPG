import configparser

from AutoBackupPG.ds_database_backup.executor import DsBuilder, ModuleFinder


class CommonSet:
    _cfg = configparser.ConfigParser()
    _cfg.read(r'C:\backup\test_suite\test_store.cfg')

    path_to_backups = r'C:\backup'
    custom_dir = r'Sales depart'
    path_to_7zip = r'C:\backup\test_suite\7zip\x64'
    access_key_id = _cfg.get('Ob', 'key_id')
    secret_access_key = _cfg.get('Ob', 'key')
    bucket = _cfg.get('Ob', 'bucket')


class BackupersSet:
    base = {
        'path_to_backups': CommonSet.path_to_backups,
        'custom_dir': CommonSet.custom_dir,
    }

    archiver = {
        'path_to_7zip': CommonSet.path_to_7zip,
    }

    pg = {
        'postgresql_instance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
    }

    pg_base = {
        'pg_basebackup': r'C:\backup\test_suite\pg_basebackup_14.6\pg_basebackup.exe',
    }

    pg_dump = {
        'database_name': r'test_ds',
        'use_temp_dump': False,
    }

    one_c = {
        'path_to_1c_db': r'C:\backup\test_suite\one_c_base',
        'path_to_7zip': CommonSet.path_to_7zip,
    }


class SyncSet:
    config = {
        'path_to_backups': CommonSet.path_to_backups,
        'custom_dir': CommonSet.custom_dir,

        'access_key_id': CommonSet.access_key_id,
        'secret_access_key': CommonSet.secret_access_key,
        'bucket': CommonSet.bucket,
    }


class NonPgBaseCleanerSet:
    config = {
        'path_to_backups': CommonSet.path_to_backups,
        'custom_dir': CommonSet.custom_dir,

        'backups_leave_amount': 0,
        'keep_one_backup_per_day': True,
        'storage_time': 7 * 24 * 60 * 60,
    }


class PgBaseCleanerSet:
    config = NonPgBaseCleanerSet.config.update(
        {
            'path_to_wal_files': '',
            'use_simple_way_read_bck_date': True
        }
    )


class BackupTestCases:
    class PG:
        _config = BackupersSet.base.copy()
        _config.update(BackupersSet.pg)

        def create_dump_by_base_name(self):
            config = self._config.copy()
            config.update(BackupersSet.pg_dump)

            DsBuilder \
                .build(ModuleFinder.PG_DUMP_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_dump_by_base_name_with_archiver(self):
            config = self._config.copy()
            config.update(BackupersSet.pg_dump)
            config.update(BackupersSet.archiver)

            DsBuilder \
                .build(ModuleFinder.PG_DUMP_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_pg_base(self):
            config = self._config.copy()
            config.update(BackupersSet.pg_base)

            DsBuilder \
                .build(ModuleFinder.PG_BASE_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_pg_base_with_archiver(self):
            config = self._config.copy()
            config.update(BackupersSet.pg_base)
            config.update(BackupersSet.archiver)

            DsBuilder \
                .build(ModuleFinder.PG_BASE_BACKUPER) \
                .initialize_config(config) \
                .start()

    class OneC:
        _config = BackupersSet.base.copy()
        _config.update(BackupersSet.one_c)

        def create(self):
            config = self._config.copy()

            DsBuilder \
                .build(ModuleFinder.ONE_C_FB_BACKUPER) \
                .initialize_config(config) \
                .start()


class CleanerTestCases:
    class NonPgBase:
        def clean(self):
            pass

    class PgBase:
        def clean(self):
            pass


class SyncTestCases:
    def sync(self):
        pass



backupTestCases = BackupTestCases()

backupTestCases.PG().create_dump_by_base_name()
backupTestCases.PG().create_dump_by_base_name_with_archiver()

# backupTestCases.PG.create_pg_base()
# backupTestCases.PG.create_pg_base_with_archiver()
