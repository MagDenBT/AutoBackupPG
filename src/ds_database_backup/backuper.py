import os
import shutil
import subprocess
from .exceptions import PgBaseBackupCreateError, \
    ArchiveCreateError, PgDumpRunError, PgDumpCreateError, OneCFBBackupCreateError, MsSqlCreateError, \
    GitBackupCreateError
from .configs import ConfigPgBaseBackuper, ConfigPgDumpBackuper, Config1CFBBackuper, ConfigMsSqlBackuper, \
    ConfigGitBackuper
from .executor import Executor
from .utils import Utils


class PgBaseBackuper(Executor):
    _gzip_backup_name = 'base.tar.gz'

    def __init__(self, config: ConfigPgBaseBackuper):
        super(PgBaseBackuper, self).__init__(config)
        self._config = config

    @staticmethod
    def config_class():
        return ConfigPgBaseBackuper

    def start(self):
        self._create_backup()

    def _create_backup(self):
        Utils.delete_unused_temp_dirs(self._config.temp_path)
        if not os.path.exists(self._config.temp_path):
            os.makedirs(self._config.temp_path)

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self._config.postgresql_password
        comm_args = [self._config.pg_basebackup,
                     '-D', self._config.temp_path,
                     '-X', 'fetch',
                     '-F', 'tar',
                     '--label', self._config.label,
                     '--no-password',
                     '--username', self._config.postgresql_username,
                     '--gzip']
        self._add_no_manifest_key(comm_args)

        if self._config.pg_port is not None and self._config.pg_port != '':
            comm_args.extend(['-p', self._config.pg_port])

        process = subprocess.run(
            comm_args,
            stderr=subprocess.PIPE,
            env=my_env,
        )

        text_error = Utils.decode_text_or_return_error_msg(process.stderr)
        if text_error:
            raise PgBaseBackupCreateError(text_error)

        if self._config.use_external_archiver:
            self._archive_with_external_tool()
        else:
            self._move_to_permanent_dir()
        Utils.delete_unused_temp_dirs(self._config.temp_path)

    def _archive_with_external_tool(self):
        comm_args = f'"{self._config.path_to_7zip}" x -so -sdel "{self._config.temp_path}\\{self._gzip_backup_name}"' \
                    f' | "{self._config.path_to_7zip}" a -si' \
                    f' "{self._config.full_path_to_backups}\\{self._config.label}__base.txz" '

        try:
            subprocess.check_output(comm_args, stderr=subprocess.PIPE, shell=True)
        except Exception as e:
            try:
                os.remove(self._config.temp_path)
            finally:
                raise ArchiveCreateError(e)

    def _move_to_permanent_dir(self, create_subdir=False):
        label = self._config.label
        files = Utils.get_objects_on_disk(self._config.temp_path)
        target_dir = self._config.full_path_to_backups

        if create_subdir:
            target_dir += f'\\{label}'

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        # move & rename
        for file in files:
            shutil.move(file,
                        f'{target_dir}\\{label}__{os.path.basename(file)}')

    def _add_no_manifest_key(self, target_args):
        current_pg_version = self._get_pg_version()
        min_version_supports_manifest = 13
        if current_pg_version >= min_version_supports_manifest:
            target_args.append('--no-manifest')

    def _get_pg_version(self):
        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self._config.postgresql_password
        psql = self._config.postgresql_instance_path + '\\bin\\psql'
        comm_args = [psql,
                     '-U', self._config.postgresql_username,
                     '-V']

        process = subprocess.run(
            comm_args,
            stdout=subprocess.PIPE,
            env=my_env,
        )

        message_from_server = Utils.decode_text_or_return_error_msg(process.stdout).strip()
        normalized_message = [s.strip() for s in message_from_server.split(' ')]
        pg_version_str = normalized_message[2]
        return int(pg_version_str.split('.')[0])


class PgDumpBackuper(Executor):

    def __init__(self, config: ConfigPgDumpBackuper):
        super().__init__(config)
        self._config = config

    @staticmethod
    def config_class():
        return ConfigPgDumpBackuper

    def start(self):
        self._create_backup()

    def _create_backup(self):
        Utils.delete_old_temp_dir()
        Utils.delete_unused_temp_dirs(self._config.temp_path)
        for base_name in self._get_bases_list():
            dump_name = Utils.create_backup_name(base_name, self._config.label, 'dump')
            dump_full_path = f'{self._config.full_path_to_backups}\\{dump_name}'
            if not os.path.exists(self._config.full_path_to_backups):
                os.makedirs(self._config.full_path_to_backups)

            if self._config.use_temp_dump:
                self._create_through_rom(dump_full_path, base_name)
                Utils.delete_unused_temp_dirs(self._config.temp_path)
            else:
                self._create_through_stdout(dump_full_path, base_name)

    def _get_bases_list(self) -> [str]:
        if self._config.database_name is not None and self._config.database_name != "":
            return [self._config.database_name]

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self._config.postgresql_password
        # noinspection SqlNoDataSourceInspection
        comm_args = [self._config.pgsql,
                     '-U', self._config.postgresql_username,
                     '-c',
                     "SELECT string_agg(datname, ', ') FROM pg_database "
                     "WHERE datistemplate = false AND datname != 'postgres'",
                     '--pset', "footer=off",
                     '-t']

        if self._config.pg_port is not None and self._config.pg_port != '':
            comm_args.insert(1, '-p')
            comm_args.insert(2, self._config.pg_port)

        process = subprocess.run(
            comm_args,
            stdout=subprocess.PIPE,
            env=my_env,
        )

        message_from_server = Utils.decode_text_or_return_error_msg(process.stdout).strip()
        bases_list = [s.strip() for s in message_from_server.split(',') if s.strip()]
        return bases_list

    def _create_through_stdout(self, dump_full_path, base_name: str):
        comm_args = self._specific_base_command_through_stdout(dump_full_path, base_name)

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self._config.postgresql_password

        try:
            output = subprocess.check_output(comm_args, stderr=subprocess.STDOUT, env=my_env, shell=True)
        except Exception as e:
            raise PgDumpRunError(e)

        self._throw_error_if_create_process_failed(output)

    def _specific_base_command_through_stdout(self, dump_full_path, base_name):
        port_arg = ''
        if self._config.pg_port is not None and self._config.pg_port != '':
            port_arg = f' -p {self._config.pg_port}'

        comm_args = f'"{self._config.pg_dump}"{port_arg}' \
                    f' -U {self._config.postgresql_username}' \
                    f' -Fc {base_name}'

        if self._config.use_external_archiver:
            comm_args = comm_args + f' | "{self._config.path_to_7zip}" a -si "{dump_full_path}.xz"'
        else:
            comm_args += f' > "{dump_full_path}"'

        return comm_args

    @staticmethod
    def _throw_error_if_create_process_failed(subprocess_output):
        result = Utils.decode_text_or_return_error_msg(subprocess_output)
        pg_error = ''
        if len(result.splitlines()) > 0:
            pg_error = result.splitlines()[0]
        if pg_error:
            raise PgDumpCreateError(pg_error)

    def _create_through_rom(self, finish_dump_path, base_name: str):
        if self._config.use_external_archiver:
            if not os.path.exists(self._config.temp_path):
                os.makedirs(self._config.temp_path)
            dump_full_path = f'{self._config.temp_path}\\{os.path.basename(finish_dump_path)}'
        else:
            dump_full_path = finish_dump_path

        comm_args = self._specific_base_command_through_rom(dump_full_path, base_name)

        my_env = os.environ.copy()
        my_env["PGPASSWORD"] = self._config.postgresql_password
        process = subprocess.run(
            comm_args,
            stderr=subprocess.PIPE,
            env=my_env,
        )

        text_error = Utils.decode_text_or_return_error_msg(process.stderr)
        if text_error:
            raise PgDumpCreateError(text_error)

        if self._config.use_external_archiver:
            self._archive_with_external_tool(dump_full_path, finish_dump_path)

    def _specific_base_command_through_rom(self, dump_full_path, base_name):
        comm_args = [self._config.pg_dump,
                     '-U', self._config.postgresql_username,
                     '-Fc',
                     '-f', dump_full_path,
                     base_name
                     ]
        if self._config.pg_port is not None and self._config.pg_port != '':
            comm_args.insert(1, '-p')
            comm_args.insert(2, self._config.pg_port)
        return comm_args

    def _archive_with_external_tool(self, source, target):
        comm_args = f'"{self._config.path_to_7zip}" a -sdel "{target}.xz" "{source}"'
        try:
            subprocess.check_output(comm_args, stderr=subprocess.PIPE, shell=True)
        except Exception as e:
            try:
                os.remove(source)
            finally:
                raise ArchiveCreateError(e)


class OneCFbBackuper(Executor):

    def __init__(self, config: Config1CFBBackuper):
        super().__init__(config)
        self._config = config

    @staticmethod
    def config_class():
        return Config1CFBBackuper

    def start(self):
        self._create_backup()

    def _create_backup(self) -> None:
        # noinspection DuplicatedCode
        backup_name = Utils.create_backup_name(self._config.cd_file_name, self._config.label, 'xz')
        target_file = f'{self._config.full_path_to_backups}\\{backup_name}'
        comm_args = [f'{self._config.path_to_7zip}', 'a', target_file, '-ssw', self._config.path_to_1c_db_dir]

        process = subprocess.run(
            comm_args,
            stderr=subprocess.PIPE
        )

        text_error = Utils.decode_text_or_return_error_msg(process.stderr)
        if text_error:
            raise OneCFBBackupCreateError(text_error)


class MsSqlBackuper(Executor):

    def __init__(self, config: ConfigMsSqlBackuper):
        super().__init__(config)
        self._config = config
        self._authorize_by_os_user = len(config.ms_sql_username) + len(config.ms_sql_password) == 0

    @staticmethod
    def config_class():
        return ConfigMsSqlBackuper

    def start(self):
        self._create_backup()

    def _create_backup(self):
        finish_bak_path = self._get_existing_finish_path()
        comm_args = self._create_args(finish_bak_path)
        process = subprocess.run(
            comm_args,
            stdout=subprocess.PIPE,
        )

        self._throw_error_if_create_process_failed(process.stdout)

        if self._config.use_external_archiver:
            self._archive_with_external_tool(finish_bak_path)

    def _get_existing_finish_path(self) -> str:
        bak_file_name = Utils.create_backup_name(self._config.database_name, self._config.label, 'bak')
        bak_full_path = f'{self._config.full_path_to_backups}\\{bak_file_name}'
        if not os.path.exists(self._config.full_path_to_backups):
            os.makedirs(self._config.full_path_to_backups)
        return bak_full_path

    def _create_args(self, bak_full_path) -> [str]:
        comm_args = ['sqlcmd']
        if self._authorize_by_os_user:
            comm_args.append('-E')
        else:
            user_name = 'sa' if len(self._config.ms_sql_username) == 0 else self._config.ms_sql_username
            comm_args.extend([
                '-U', user_name,
                '-P', self._config.ms_sql_password])

        comm_args.extend(
            ['-Q',
             f"BACKUP DATABASE [{self._config.database_name}] TO DISK = '{bak_full_path}' WITH FORMAT, COMPRESSION"]
        )
        return comm_args

    @staticmethod
    def _throw_error_if_create_process_failed(subprocess_output):
        message = Utils.decode_text_or_return_error_msg(subprocess_output)
        error_key_ru = 'не существует'
        error_key_eng = 'does not exist'
        error_key = 'Msg '
        if error_key_ru in message.lower() or error_key_eng in message.lower() or error_key in message:
            raise MsSqlCreateError(message)

    def _archive_with_external_tool(self, source):
        target = source + '.xz'
        comm_args = f'"{self._config.path_to_7zip}" a -sdel "{target}" "{source}"'
        try:
            subprocess.check_output(comm_args, stderr=subprocess.PIPE, shell=True)
        except Exception as e:
            raise ArchiveCreateError(e)


class GitBackuper(Executor):

    def __init__(self, config: ConfigGitBackuper):
        super().__init__(config)
        self._config = config

    @staticmethod
    def config_class():
        return ConfigGitBackuper

    def start(self):
        self._create_backup()

    def _create_backup(self) -> None:
        # noinspection DuplicatedCode
        backup_name = Utils.create_backup_name(self._config.backup_type_dir, self._config.label, '7z')
        target_file = f'{self._config.full_path_to_backups}\\{backup_name}'
        comm_args = [f'{self._config.path_to_7zip}', 'a', target_file, '-ssw', self._config.path_to_git]

        process = subprocess.run(
            comm_args,
            stderr=subprocess.PIPE
        )

        text_error = Utils.decode_text_or_return_error_msg(process.stderr)
        if text_error:
            raise GitBackupCreateError(text_error)
