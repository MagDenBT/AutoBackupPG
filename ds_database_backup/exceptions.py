import subprocess
from typing import List


class DriveNotExist(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. Параметр {parameter_name} - Корневой диск в {path} не существует'
    )

    def __init__(self, parameter_name: str, path: str):
        msg = self.MSG_TEMPLATE.format(
            parameter_name=parameter_name,
            path=path,
        )
        super().__init__(msg)


class ArchiverNotFound(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек - не найден архиватор ({archiver_path})'
    )

    def __init__(self, archiver_path: str):
        msg = self.MSG_TEMPLATE.format(
            archiver_path=archiver_path,
        )
        super().__init__(msg)


class OneCDbNotFound(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек - не найден файл базы данных 1с ({db_path})'
    )

    def __init__(self, db_path: str):
        msg = self.MSG_TEMPLATE.format(
            db_path=db_path,
        )
        super().__init__(msg)


class MandatoryPropertiesNotPresent(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. Отстутствуют обязательные параметры - {failed_properties}'
    )

    def __init__(self, failed_properties: List[str]):
        msg = self.MSG_TEMPLATE.format(
            failed_properties=', '.join([f" {value}" for value in failed_properties]),
        )
        super().__init__(msg)


class PathNotExist(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. Пути не существуют: {parameter_and_path}'
    )

    def __init__(self, parameter_and_path: {str:str}):
        msg = self.MSG_TEMPLATE.format(
            parameter_and_path = '\n'.join([f"Параметр - {key}, путь - {value}" for key, value in parameter_and_path.items()]),
        )
        super().__init__(msg)


class PgBaseBackupNotFound(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек - не найден pg_basebackup\n'
        'Проверьте путь до каталога сервера SQL ({sql_instance_path}) \n'
        'или pg_basebackup({pg_basebackup_path})'
    )

    def __init__(self, pg_basebackup_path: str, sql_instance_path: str, ):
        msg = self.MSG_TEMPLATE.format(
            pg_basebackup_path=pg_basebackup_path,
            sql_instance_path=sql_instance_path,
        )
        super().__init__(msg)


class PgDumpNotFound(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек - не найден pg_dump\n'
        'Проверьте путь до каталога сервера SQL ({sql_instance_path}) \n'
        'или pg_dump({pg_dump_path})'
    )

    def __init__(self, pg_dump: str, sql_instance_path: str, ):
        msg = self.MSG_TEMPLATE.format(
            pg_dump=pg_dump,
            sql_instance_path=sql_instance_path,
        )
        super().__init__(msg)


class PgBaseBackupCreateError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при создании полного бэкапа PostgreSQl - {error_text}'
    )

    def __init__(self, error_text: str):
        msg = self.MSG_TEMPLATE.format(
            error_text=error_text,
        )
        super().__init__(msg)


class ArchiveCreateError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при архивировании бэкапа - {error_text}'
    )

    def __init__(self, exception):
        if isinstance(exception, subprocess.CalledProcessError):
            error_text = exception.stderr.decode(errors='replace')
        else:
            error_text = str(exception)

        msg = self.MSG_TEMPLATE.format(
            error_text=error_text,
        )
        super().__init__(msg)


class PgDumpRunError(Exception):
    MSG_TEMPLATE = (
        'Ошибка запуска pg_dump - {error_text}'
    )

    def __init__(self, exception):
        if isinstance(exception, subprocess.CalledProcessError):
            error_text = exception.output.decode(errors='replace')
        else:
            error_text = str(exception)

        msg = self.MSG_TEMPLATE.format(
            error_text=error_text,
        )
        super().__init__(msg)


class PgDumpCreateError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при создании дампа PostgreSQl - {error_text}'
    )

    def __init__(self, error_text):
        msg = self.MSG_TEMPLATE.format(
            error_text=error_text,
        )
        super().__init__(msg)


class OneCFBBackupCreateError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при создании бэкапа файловой базы 1с - {error_text}'
    )

    def __init__(self, error_text):
        msg = self.MSG_TEMPLATE.format(
            error_text=error_text,
        )
        super().__init__(msg)


class AWSTimeTooSkewedError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при синхронизации с облаком - локальное время слишком сильно отличается от облачного'
    )

    def __init__(self):
        super().__init__(self.MSG_TEMPLATE)


class AWSBucketError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при синхронизации с облаком - {error_message}'
    )

    def __init__(self, exception):
        from botocore.exceptions import (
            ConnectionClosedError,
        )
        error_message = 'не удалось проверить бакет'
        if isinstance(exception, ConnectionClosedError):
            error_message = 'не удалось проверить бакет, т.к. соединение было закрыто'
        else:
            err_code = exception.response.get('Error').get('Code')
            if err_code == '404':
                error_message = 'бакет не существует'
            elif err_code == '403':
                error_message = 'к бакету нет доступа'

        msg = self.MSG_TEMPLATE.format(
            error_message=error_message,
        )
        super().__init__(msg)


class RansomwareVirusTracesFound(Exception):
    MSG_TEMPLATE = (
        'Выгрузка в облако была выполнена, '
        'но обнаружены файлы с нетипичными расширениями (вирус-шифровальщик?) - {corrupt_files}'
    )

    def __init__(self, corrupt_files: List[str]):
        msg = self.MSG_TEMPLATE.format(
            corrupt_files=', '.join([f" {value}" for value in corrupt_files]),
        )
        super().__init__(msg)


class AWSSpeedAutoAdjustmentError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при синхронизации с облаком - нестабильный интернет, автопонижение скорости выгрузки до'
        ' {speed_kbit_s} Кбит/с не решило проблему'
    )

    def __init__(self, speed_kbit_s):
        msg = self.MSG_TEMPLATE.format(
            speed_kbit_s=speed_kbit_s,
        )
        super().__init__(msg)
