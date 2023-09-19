import os
import shutil
import subprocess
from abc import ABC, abstractmethod

from AutoBackupPG.ds_database_backup.exceptions import PgBaseBackupNotFound, PgBaseBackupCreateError, \
    ArchiveCreateError, PgDumpRunError, PgDumpCreateError, OneCFBBackupCreateError
from AutoBackupPG.ds_database_backup.settings import SettingPgBaseBackuper, SettingPgDumpBackuper, Setting1CFBBackuper
from AutoBackupPG.ds_database_backup.utils import Utils


class AbstractBackuper(ABC):

    @abstractmethod
    def create_backup(self) -> None:
        pass


class PgBaseBackuper(AbstractBackuper):

    def __init__(self, settings: SettingPgBaseBackuper):
        self._settings = settings

    def create_backup(self):
        self._clear_dir(self._settings.temp_path)

        if not os.path.exists(self._settings.pg_basebackup):
            raise PgBaseBackupNotFound(pg_basebackup_path=self._settings.pg_basebackup,
                                       sql_instance_path=self._settings.postgresql_instance_path)

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self._settings.postgresql_password
        comm_args = [self._settings.pg_basebackup,
                     '-D', self._settings.temp_path,
                     '-X', 'fetch',
                     '-F', 'tar',
                     '--label', self._settings.label,
                     '--no-password',
                     '--username', self._settings.postgresql_username,
                     ]

        if not self._settings.use_external_archiver:
            comm_args.append('--gzip')

        if self._settings.pg_port is not None and self._settings.pg_port != '':
            comm_args.extend(['-p', self._settings.pg_port])

        process = subprocess.run(
            comm_args,
            stderr=subprocess.PIPE,
            env=my_env,
        )

        text_error = process.stderr.decode(errors='replace')
        if text_error:
            raise PgBaseBackupCreateError(text_error)

        if self._settings.use_external_archiver:
            self._archive_with_external_tool()
        else:
            self._move_to_permanent_dir()
            self._clear_dir(self._settings.temp_path)

    @staticmethod
    def _clear_dir(path):
        # clear the directory of any files
        if os.path.exists(path):
            for _obj in os.listdir(path):
                if os.path.exists(f'{path}\\{_obj}'):
                    os.remove(f'{path}\\{_obj}')

    def _archive_with_external_tool(self):
        comm_args = f'"{self._settings.path_to_7zip}" a -ttar -so -sdel -an "{self._settings.temp_path}\\"*' \
                    f' | "{self._settings.path_to_7zip}" a -si' \
                    f' "{self._settings.full_path_to_backups}\\{self._settings.label}__base.txz" '

        try:
            subprocess.check_output(comm_args, stderr=subprocess.PIPE, shell=True)
        except Exception as e:
            os.remove(self._settings.temp_path)
            raise ArchiveCreateError(e)

    def _move_to_permanent_dir(self, create_subdir=True):
        label = self._settings.label()
        files = Utils.get_objects_on_disk(self._settings.temp_path)
        target_dir = self._settings.full_path_to_backups

        if create_subdir:
            target_dir += f'\\{label}'

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        # move & rename
        for file in files:
            shutil.move(file,
                        f'{target_dir}\\{label}__{os.path.basename(file)}')


class PgDumpBackuper(AbstractBackuper):

    def __init__(self, settings: SettingPgDumpBackuper):
        self._settings = settings

    def create_backup(self):

        if not os.path.exists(self._settings.pg_dump):
            raise Exception(
                f'pg_dump по адресу {self._settings.pg_dump()} не найден. Проверьте правильность пути до каталога '
                f'сервера PosgtrSQL или pg_dump(если он задан отдельно). Текущий путь до сервера в скрипте - '
                f'{self._settings.postgresql_instance_path}')

        all_bases = self._settings.database_name is None or self._settings.database_name == ""

        base_name = "all_bases" if all_bases else self._settings.database_name

        dump_name = f'{base_name}_{self._settings.label()}.dump'
        dump_full_path = f'{self._settings.full_path_to_backups}\\{dump_name}'
        if not os.path.exists(self._settings.full_path_to_backups):
            os.makedirs(self._settings.full_path_to_backups)

        if self._settings.use_temp_dump:
            self._create_through_rom(dump_full_path, all_bases)
        else:
            self._create_through_stdout(dump_full_path, all_bases)

    def _create_through_stdout(self, dump_full_path, all_bases: bool):
        if all_bases:
            comm_args = self._all_bases_command_through_stdout(dump_full_path)
        else:
            comm_args = self._specific_base_command_through_stdout(dump_full_path)

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self._settings.postgresql_password

        try:
            output = subprocess.check_output(comm_args, stderr=subprocess.STDOUT, env=my_env, shell=True)
        except Exception as e:
            raise PgDumpRunError(e)

        self._throw_error_if_create_process_failed(output)

    def _all_bases_command_through_stdout(self, dump_full_path):
        port_arg = ''
        if self._settings.pg_port is not None and self._settings.pg_port != '':
            port_arg = f' -p {self._settings.pg_port}'
        comm_args = f'"{self._settings.pg_dumpall()}"{port_arg} -U {self._settings.postgresql_username}'
        if self._settings.use_external_archiver:
            comm_args = comm_args + f' | "{self._settings.path_to_7zip}" a -si "{dump_full_path}.xz"'
        else:
            comm_args += f' > "{dump_full_path}"'
        return comm_args

    def _specific_base_command_through_stdout(self, dump_full_path):
        port_arg = ''
        if self._settings.pg_port is not None and self._settings.pg_port != '':
            port_arg = f' -p {self._settings.pg_port}'

        comm_args = f'"{self._settings.pg_dump()}"{port_arg}' \
                    f' -U {self._settings.postgresql_username}' \
                    f' -Fc {self._settings.database_name}'

        if self._settings.use_external_archiver:
            comm_args = comm_args + f' | "{self._settings.path_to_7zip}" a -si "{dump_full_path}.xz"'
        else:
            comm_args += f' > "{dump_full_path}"'

        return comm_args

    @staticmethod
    def _throw_error_if_create_process_failed(subprocess_output):
        result = subprocess_output.decode(errors='replace')
        pg_error = ''
        if len(result.splitlines()) > 0:
            pg_error = result.splitlines()[0]
        if pg_error:
            raise PgDumpCreateError(pg_error)

    def _create_through_rom(self, finish_dump_path, all_bases: bool):
        if self._settings.use_external_archiver:
            if not os.path.exists(self._settings.temp_path):
                os.makedirs(self._settings.temp_path)
            dump_full_path = f'{self._settings.temp_path}\\{os.path.basename(finish_dump_path)}'
        else:
            dump_full_path = finish_dump_path

        comm_args = self._all_bases_command_through_rom(
            dump_full_path) if all_bases else self._specific_base_command_through_rom(dump_full_path)

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self._settings.postgresql_password
        process = subprocess.run(
            comm_args,
            stderr=subprocess.PIPE,
            env=my_env,
        )

        text_error = process.stderr.decode(errors='replace')
        if text_error:
            raise PgDumpCreateError(text_error)

        if self._settings.use_external_archiver:
            self._archive_with_external_tool(dump_full_path, finish_dump_path)

    def _all_bases_command_through_rom(self, dump_full_path):
        comm_args = [self._settings.pg_dumpall(),
                     '-U', self._settings.postgresql_username,
                     '-f', dump_full_path
                     ]
        if self._settings.pg_port is not None and self._settings.pg_port != '':
            comm_args.insert(1, '-p')
            comm_args.insert(2, self._settings.pg_port)
        return comm_args

    def _specific_base_command_through_rom(self, dump_full_path):
        comm_args = [self._settings.pg_dump(),
                     '-U', self._settings.postgresql_username,
                     '-Fc',
                     '-f', dump_full_path,
                     self._settings.database_name
                     ]
        if self._settings.pg_port is not None and self._settings.pg_port != '':
            comm_args.insert(1, '-p')
            comm_args.insert(2, self._settings.pg_port)
        return comm_args

    def _archive_with_external_tool(self, source, target):
        comm_args = f'"{self._settings.path_to_7zip}" a -sdel "{target}.xz" "{source}"'
        try:
            subprocess.check_output(comm_args, stderr=subprocess.PIPE, shell=True)
        except Exception as e:
            os.remove(source)
            raise ArchiveCreateError(e)


class OneCFbBackuper:
    _settings = None

    def __init__(self, settings: Setting1CFBBackuper):
        self._settings = settings

    def _create_backup(self):
        target_file = f'{self._settings.full_path_to_backups}\\{self._settings.label}_{self._settings.path_to_1c_db}.xz'

        comm_args = [f'{self._settings.path_to_7zip}', 'a', target_file, '-ssw', self._settings.path_to_1c_db]

        process = subprocess.run(
            comm_args,
            stderr=subprocess.PIPE
        )

        text_error = process.stderr.decode(errors='replace')
        if text_error:
            raise OneCFBBackupCreateError(text_error)
