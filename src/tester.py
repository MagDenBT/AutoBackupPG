import configparser
import os
import time

import psutil

from ds_database_backup.executor import DsBuilder, ModuleFinder


class CommonSet:
    _cfg = configparser.ConfigParser()
    _cfg.read(r'C:\backup\test_suite\test_store.cfg')

    path_to_backups = r'C:\backup'
    custom_dir = r'Sales depart'
    path_to_7zip = r'C:\backup\test_suite\7zip'
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
        'postgresql_instance_path': r'C:\Program Files\PostgreSQL\15.4-1.1C',
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
        'path_to_1c_db_dir': r'C:\backup\test_suite\one_c_base',
        'path_to_7zip': CommonSet.path_to_7zip,
    }

    ms_sql = {
        'database_name': r'kip',
    }

    git = {
        'path_to_git': r'C:\git\mpr_2_3_11_375\.git\logs',
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


class CleanerSet:
    config = {
        'path_to_backups': CommonSet.path_to_backups,
        'custom_dir': CommonSet.custom_dir,

        'backups_leave_amount': 3,
        'keep_one_backup_per_day': True,
        'storage_time': 7 * 24 * 60 * 60,
        'leave_only_last_full_pg_backup': True
    }

    wal_config = config.copy()
    wal_config.update({
        'path_to_wal_files': '',
    })


class BackupTestCases:
    class PG:
        _config = BackupersSet.base.copy()
        _config.update(BackupersSet.pg)

        def create_dump_by_base_name_rom(self):
            config = self._config.copy()
            config.update(BackupersSet.pg_dump)

            DsBuilder \
                .build(ModuleFinder.PG_DUMP_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_dump_by_base_name_with_archiver_rom(self):
            config = self._config.copy()
            config.update(BackupersSet.pg_dump)
            config.update(BackupersSet.archiver)

            DsBuilder \
                .build(ModuleFinder.PG_DUMP_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_dumps_for_all_bases_rom(self):
            config = self._config.copy()

            DsBuilder \
                .build(ModuleFinder.PG_DUMP_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_dumps_for_all_bases_with_archiver_rom(self):
            config = self._config.copy()
            config.update(BackupersSet.archiver)

            DsBuilder \
                .build(ModuleFinder.PG_DUMP_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_dump_by_base_name_RAM(self):
            config = self._config.copy()
            config.update(BackupersSet.pg_dump)
            config.update({'use_temp_dump': True})
            DsBuilder \
                .build(ModuleFinder.PG_DUMP_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_dump_by_base_name_with_archiver_RAM(self):
            config = self._config.copy()
            config.update(BackupersSet.pg_dump)
            config.update(BackupersSet.archiver)
            config.update({'use_temp_dump': True})
            DsBuilder \
                .build(ModuleFinder.PG_DUMP_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_dumps_for_all_bases_RAM(self):
            config = self._config.copy()
            config.update({'use_temp_dump': True})
            DsBuilder \
                .build(ModuleFinder.PG_DUMP_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_dumps_for_all_bases_with_archiver_RAM(self):
            config = self._config.copy()
            config.update(BackupersSet.archiver)
            config.update({'use_temp_dump': True})
            DsBuilder \
                .build(ModuleFinder.PG_DUMP_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_pg_base(self):
            config = self._config.copy()
            # config.update(BackupersSet.pg_base)

            DsBuilder \
                .build(ModuleFinder.PG_BASE_BACKUPER) \
                .initialize_config(config) \
                .start()

        def create_pg_base_with_archiver(self):
            config = self._config.copy()
            # config.update(BackupersSet.pg_base)
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

    class MsSql:
        _config = BackupersSet.base.copy()
        _config.update(BackupersSet.ms_sql)

        def create(self):
            config = self._config.copy()

            DsBuilder \
                .build(ModuleFinder.find_by_name('MS_SQL_BACKUPER')) \
                .initialize_config(config) \
                .start()

        def create_with_archiver(self):
            config = self._config.copy()
            config.update({'temp_path': "C:\\Program Files\\1C\AgentETP\\SystemAdministrationCenter\\Scripts\\temp"})
            config.update({'path_to_7zip': CommonSet.path_to_7zip})
            DsBuilder \
                .build(ModuleFinder.find_by_name('MS_SQL_BACKUPER')) \
                .initialize_config(config) \
                .start()

    class Git:
        _config = BackupersSet.base.copy()
        _config.update(BackupersSet.git)

        def create(self):
            config = self._config.copy()

            DsBuilder \
                .build(ModuleFinder.find_by_name('GIT_BACKUPER')) \
                .initialize_config(config) \
                .start()


class CleanerTestCases:
    class LC:

        def clean(self):
            config = CleanerSet.config

            DsBuilder \
                .build(ModuleFinder.CLEANER) \
                .initialize_config(config) \
                .start()

        def clean_with_wals(self):
            config = CleanerSet.wal_config

            DsBuilder \
                .build(ModuleFinder.CLEANER) \
                .initialize_config(config) \
                .start()


class SyncTestCases:
    class AWS:
        _config = SyncSet.config.copy()

        def sync(self):
            config = self._config.copy()
            DsBuilder \
                .build(ModuleFinder.AWS_CLIENT) \
                .initialize_config(config) \
                .start()


def run_backup_test_cases():
    backup_test_cases = BackupTestCases()

    # backup_test_cases.PG().create_dump_by_base_name_rom() #1
    # time.sleep(0)
    # backup_test_cases.PG().create_dump_by_base_name_with_archiver_rom() #1
    # time.sleep(0)
    # backup_test_cases.PG().create_dumps_for_all_bases_rom() #3
    # time.sleep(0)
    # backup_test_cases.PG().create_dumps_for_all_bases_with_archiver_rom() #3
    # time.sleep(0)
    #
    # backup_test_cases.PG().create_dump_by_base_name_RAM() #1
    # time.sleep(0)
    # backup_test_cases.PG().create_dump_by_base_name_with_archiver_RAM() #1
    # time.sleep(0)
    # backup_test_cases.PG().create_dumps_for_all_bases_RAM() #3
    # time.sleep(0)
    # backup_test_cases.PG().create_dumps_for_all_bases_with_archiver_RAM() #3
    # time.sleep(0)
    # #
    # backup_test_cases.PG().create_pg_base()
    # # time.sleep(1)
    # backup_test_cases.PG().create_pg_base_with_archiver()
    # time.sleep(0)
    #
    # backup_test_cases.OneC().create()
    # time.sleep(0)
    #
    # backup_test_cases.MsSql().create()
    # time.sleep(1)
    # backup_test_cases.MsSql().create_with_archiver()

    backup_test_cases.Git().create()
    time.sleep(0)


def run_cleaner_test_cases():
    cleaner_test_cases = CleanerTestCases()

    cleaner_test_cases.LC().clean()
    time.sleep(0)
    # Todo Проверить очистку WAL-файлов


def run_cloud_sync_test_cases():
    sync_test_cases = SyncTestCases()

    sync_test_cases.AWS().sync()
    time.sleep(0)


# run_cleaner_test_cases()
run_cloud_sync_test_cases()
# run_backup_test_cases()
