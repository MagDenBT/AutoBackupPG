

from PGSQL_Backuper import Manager as ManagerPostgreSQLBackuper


def test_pg():
 
    p_create_full_and_dump_STDOUT_without_7zip_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'database_name': r'secondBase',
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'handle_full_bcks': True,
        'local_path_to_wal_files': r'C:\backup\WAL',
    }
    p_create_full_and_dump_STDOUT_with_7zip_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'database_name': r'secondBase',
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'handle_full_bcks': True,
        'local_path_to_wal_files': r'C:\backup\WAL',
        'path_to_7zip': r'C:\backup\7zip',
    }

    p_create_dump_STDOUT_without_7zip_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'database_name': r'secondBase',
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
    }
    p_create_dump_ROM_without_7zip_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'database_name': r'secondBase',
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'use_temp_dump': True,
    }

    p_create_dump_STDOUT_with_7zip_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'database_name': r'secondBase',
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'path_to_7zip': r'C:\backup\7zip',
    }
    p_create_dump_ROM_with_7zip_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'database_name': r'secondBase',
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'path_to_7zip': r'C:\backup\7zip',
        'use_temp_dump': True,
    }

    p_create_ALL_BASES_dump_STDOUT_without_7zip_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
    }
    p_create_ALL_BASES_dump_ROM_without_7zip_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'use_temp_dump': True,
    }

    p_create_ALL_BASES_dump_STDOUT_with_7zip_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'potester.pystgresql_password': '1122',
        'path_to_7zip': r'C:\backup\7zip',
    }
    p_create_ALL_BASES_dump_ROM_with_7zip_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'path_to_7zip': r'C:\backup\7zip',
        'use_temp_dump': True,
    }

    p_sync_full_and_dump_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'handle_full_bcks': True,
        'local_path_to_wal_files': r'C:\backup\WAL',
        'aws_bucket': 'backuptest2',
        'aws_access_key_id': '66666666666666',
        'aws_secret_access_key': '777777777777777777',
    }
    p_sync_dump_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'aws_bucket': 'backuptest2',
        'aws_access_key_id': '66666666666666',
        'aws_secret_access_key': '777777777777777777',
    }

    p_clean_local_full_and_dump_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'handle_full_bcks': True,
        'storage_time': 7*24*60*60,
        'use_simple_way_read_bck_date': True,
        'local_path_to_wal_files': r'C:\backup\WAL',
    }

    p_clean_local_dump_args = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'storage_time': 7*24*60*60,
        'use_simple_way_read_bck_date': True,
    }

    p_clean_local_full_and_dump_args_with_leave_amount = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'handle_full_bcks': True,
        'storage_time': 7*24*60*60,
        'use_simple_way_read_bck_date': True,
        'local_path_to_wal_files': r'C:\backup\WAL',
        'dump_leave_amount': 3,
        'full_bck_leave_amount': 3,
    }

    p_clean_local_dump_args_with_leave_amount = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'storage_time': 7*24*60*60,
        'use_simple_way_read_bck_date': True,
        'dump_leave_amount': 3
    }

    p_clean_local_full_and_dump_args_with_leave_amount_and_keep_one_per_day = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'handle_full_bcks': True,
        'storage_time': 7 * 24 * 60 * 60,
        'use_simple_way_read_bck_date': True,
        'local_path_to_wal_files': r'C:\backup\WAL',
        'dump_leave_amount': 3,
        'full_bck_leave_amount': -1,
        'keep_one_full_bck_per_day': True,
        'keep_one_dump_per_day': True,
    }

    p_clean_local_dump_args_with_leave_amount_and_keep_one_per_day = {
        'path_to_backups': r'C:\backup',
        'custom_dir': r'Sales depart',
        'storage_time': 7 * 24 * 60 * 60,
        'use_simple_way_read_bck_date': True,
        'dump_leave_amount': 3,
        'keep_one_dump_per_day': True
    }





    # m = ManagerPostgreSQLBackuper(new_args=p_create_full_and_dump_STDOUT_without_7zip_args, create_backup=True)
    # m.create_backup(raise_exception=True)
    #
    # m = ManagerPostgreSQLBackuper(new_args=p_create_full_and_dump_STDOUT_with_7zip_args, create_backup=True)
    # m.create_backup(raise_exception=True)



    # m = ManagerPostgreSQLBackuper(new_args=p_create_dump_STDOUT_without_7zip_args, create_backup=True)
    # m.create_backup(raise_exception=True)

    # m = ManagerPostgreSQLBackuper(new_args=p_create_dump_ROM_without_7zip_args, create_backup=True)
    # m.create_backup(raise_exception=True)

    # m = ManagerPostgreSQLBackuper(new_args=p_create_dump_STDOUT_with_7zip_args, create_backup=True)
    # m.create_backup(raise_exception=True)
    #
    # m = ManagerPostgreSQLBackuper(new_args=p_create_dump_ROM_with_7zip_args, create_backup=True)
    # m.create_backup(raise_exception=True)




    # m = ManagerPostgreSQLBackuper(new_args=p_create_ALL_BASES_dump_STDOUT_without_7zip_args, create_backup=True)
    # m.create_backup(raise_exception=True)
    #
    # # m = ManagerPostgreSQLBackuper(new_args=p_create_ALL_BASES_dump_ROM_without_7zip_args, create_backup=True)
    # # m.create_backup(raise_exception=True)
    #
    #
    # m = ManagerPostgreSQLBackuper(new_args=p_create_ALL_BASES_dump_STDOUT_with_7zip_args, create_backup=True)
    # m.create_backup(raise_exception=True)

    # m = ManagerPostgreSQLBackuper(new_args=p_create_ALL_BASES_dump_ROM_with_7zip_args, create_backup=True)
    # m.create_backup(raise_exception=True)




    # m = ManagerPostgreSQLBackuper(new_args=p_clean_local_dump_args, clean_backups=True )
    # m.clean_backups(raise_exception=True)

    # m = ManagerPostgreSQLBackuper(new_args=p_clean_local_full_and_dump_args, clean_backups=True)
    # m.clean_backups(raise_exception=True)


    # m = ManagerPostgreSQLBackuper(new_args=p_clean_local_dump_args_with_leave_amount, clean_backups=True )
    # m.clean_backups(raise_exception=True)

    # m = ManagerPostgreSQLBackuper(new_args=p_clean_local_full_and_dump_args_with_leave_amount, clean_backups=True)
    # m.clean_backups(raise_exception=True)


    # m = ManagerPostgreSQLBackuper(new_args=p_clean_local_dump_args_with_leave_amount_and_keep_one_per_day, clean_backups=True )
    # m.clean_backups(raise_exception=True)

    m = ManagerPostgreSQLBackuper(new_args=p_clean_local_full_and_dump_args_with_leave_amount_and_keep_one_per_day, clean_backups=True)
    m.clean_backups(raise_exception=True)



    # m = ManagerPostgreSQLBackuper(new_args=p_sync_full_and_dump_args, sync_backups=True)
    # m = ManagerPostgreSQLBackuper(new_args=p_sync_dump_args, sync_backups=True)
    #m.sync_with_cloud(raise_exception=True)





def test_base_bck():

    create_1c_args = {
    'custom_dir': r'Sales depart',
    'path_to_onec_fb_backups': r'C:\backup',
    'path_to_source_OneC': r'C:\backup\base1c_test',
    'path_to_7zip': r'C:\backup\7zip',
    }

    sync_1c_args = {
        'custom_dir': r'Sales depart',
        'path_to_onec_fb_backups': r'C:\backup',
        'aws_bucket': 'backuptest2',
        'aws_access_key_id': '66666666666666',
        'aws_secret_access_key': '777777777777777777',
    }
    sync_1c_and_MSSQL_args = {
        'custom_dir': r'Sales depart',
        'path_to_onec_fb_backups': r'C:\backup',
        'path_to_mssql_backups': r'C:\MSSQL backups',
        'aws_bucket': 'backuptest2',
        'aws_access_key_id': '66666666666666',
        'aws_secret_access_key': '777777777777777777',
    }

    clean_1c_and_MSSQL_args = {
    'custom_dir': r'Sales depart',
    'path_to_onec_fb_backups': r'C:\backup',
    'path_to_mssql_backups': r'C:\MSSQL backups',
        'storage_time': 3 * 60,
        'aws_bucket': 'backuptest2',
        'aws_access_key_id': '66666666666666',
        'aws_secret_access_key': '777777777777777777',
    }
    clean_1c_args = {
    'custom_dir': r'Sales depart',
    'path_to_OneC_FB_backups': r'C:\backup',
        'storage_time': 3 * 6000000,
        'aws_bucket': 'backuptest2',
        'aws_access_key_id': '66666666666666',
        'aws_secret_access_key': '777777777777777777',
    }



    # m = ManagerBaseBackuper(new_args=create_1c_args, create_backup=True)
    # m.create_backup(raise_exception=True)

    # m = ManagerBaseBackuper(new_args=sync_1c_args, sync_backups=True)
    # m = ManagerBaseBackuper(new_args=sync_1c_and_MSSQL_args, sync_backups=True)
    # m.sync_with_cloud(raise_exception=True)
    # return
    # m = ManagerBaseBackuper(new_args=clean_1c_and_MSSQL_args, clean_backups=True)
    # m = ManagerBaseBackuper(new_args=clean_1c_args, clean_backups=True)
    # m.clean_backups(raise_exception=True)
    # return

  



def test_commander():

    correct_params_args = {
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'param1': f'archive_command= \'"C:\\PGBackups\\OLD\\Testing\\Script\\7zip\\7za.exe" a -mx9 "C:\\PGBackups\\WAL\\%f.xz" "%p"\'',
        'param2': r'archive_mode = on',
        'param3': r'wal_compression = on',
        'param4': r'archive_timeout = 180',
    }

    wrong_params_args = {
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'param1': f'archive_command= \'"C:\\PGBackups\\OLD\\Testing\\Script\\7zip\\7za.exe" a -mx9 "C:\\PGBackups\\WAL\\%f.xz" "%p"\'',
        'param2': r'archive_mode = on',
    }

    no_7zip_params_args = {
        'postgresql_isntance_path': r'C:\Program Files\PostgreSQL\13',
        'postgresql_username': 'postgres',
        'postgresql_password': '1122',
        'param1': f'archive_command = \'copy "%p" "C:\\PGBackups\\WAL\\%f"\'',
    }
   

    # PGSQL_param_setter(ArgsParamSetter(correct_params_args )).set_parametrs()
    # PGSQL_param_setter(ArgsParamSetter(wrong_params_args)).set_parametrs()
    # PGSQL_param_setter(ArgsParamSetter(no_7zip_params_args)).set_parametrs()

test_pg()







