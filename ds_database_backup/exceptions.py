import subprocess
from typing import List


class ModuleNotFound(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. Модуль скрипта {module_name} не найден'
    )

    def __init__(self, module_name: str):
        msg = self.MSG_TEMPLATE.format(
            module_name=module_name,
        )
        super().__init__(msg)


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


class DrivesNotExist(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. В параметрах указаны несуществующие корневые диски:\n{parameter_and_path}'
    )

    def __init__(self, parameter_and_path: {str: str}):
        msg = self.MSG_TEMPLATE.format(
            parameter_and_path='\n'.join(
                [f"Параметр - {key}, путь - {value}" for key, value in parameter_and_path.items()]),
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
        'Ошибка инициализации настроек. Пути не существуют:\n{parameter_and_path}'
    )

    def __init__(self, parameter_and_path: {str: str}):
        msg = self.MSG_TEMPLATE.format(
            parameter_and_path='\n'.join(
                [f"Параметр - {key}, путь - {value}" for key, value in parameter_and_path.items()]),
        )
        super().__init__(msg)


class ConfigTypeMismatch(Exception):
    MSG_TEMPLATE = (
        'Ошибка создания модуля {module_name}. Получены аргументы типа {received_type}, а ожидалось - {expected_type}'
    )

    def __init__(self, received_type: str, expected_type: str, module_name: str):
        msg = self.MSG_TEMPLATE.format(
            received_type=received_type,
            expected_type=expected_type,
            module_name=module_name
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
