# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# Этот модуль требуется импортировать в начале каждого скрипта.
import json

from lib.common import bootstrap
# Экспорт базового класса для сценария. Он отвечает за настройку логгирования,
# базовую валидацию параметров, перехват исключений и корректное завершение
# скрипта и т.д.
from lib.common.base_scenario import BaseScenario
# Экспорт класса ошибок.
from lib.common.errors import SACError
from lib.common.logger import global_logger
from lib.common.config import StrPathExpanded


from lib.utils.fs import remove_file_or_directory

import pickle
import os
import subprocess
from datetime import datetime
import random
import requests as requests

# CLoud settings
# from lib.common.logger import global_logger

import PostgreSQLBackuper


class LauncherPostgreSQLUploadToCloudFor1cKiP(BaseScenario):

    def _validate_specific_data(self):
        pass

        ## Метод, который выполняется после инициализации скрипта и прохождения
        # валидации. Тут могут быть выполнены любые действия, которые нужны перед
        # началом теста.

    def _after_init(self):
        pass

        ## Метод, который возвращает список тестов, которые необходимо провести.
        # Каждый тест должен быть функцией, не принимающей параметры.
        # Возвращаемый список должен содержать кортежи (имя теста, функция,
        # выполнять только если тест не ограничивается).

    def _get_available_tests(self):
        return [
            ("test-name", lambda: True, False),
        ]

    def _real(self):

        global_logger.info(message="897979797979Starting backup")

        PostgreSQLBackuper.setParam(self.config.scenario_context,True)

        # For debug
        # path = f'./logPath\\1111.json'
        # if not os.path.exists("./logPath"):
        #     os.makedirs("./logPath")
        # with open(path, 'w') as fp:
        #     for key, value in PostgreSQLBackuper.args.items():
        #          fp.write(f'{key} ---- {value}\n')
        # fp.close()
        # path = f'./logPath\\222.json'
        # with open(path, 'w') as fp:
        #     for key, value in self.config.scenario_context.items():
        #         fp.write(f'{key} ---- {value}\n')
        # fp.close()

        writeLog = self.config['writetologfile']
        message = ''
        if not PostgreSQLBackuper.checkParams(message):
            global_logger.error(message=f"Скрипт остановлен после проверки параметров.Причина - {message}")
            if writeLog:
                PostgreSQLBackuper.writeLog('checkParams-', False, message)
            raise SACError("Скрипт остановлен после проверки параметров",message)

        try:
            global_logger.info(message="Starting upload to Cloud")
            PostgreSQLBackuper.uploadOnYandexCloud()
            global_logger.info(message="Бэкапы PostgreSQL успешно выгружены в облако")
            if writeLog:
                PostgreSQLBackuper.writeLog('upload-', True, '')
        except Exception as e:
            error = str(e)
            global_logger.error(message=f"Выгрузка бэкапов PostgreSQL в облако не удалась. Причина - {error}")
            if writeLog:
                PostgreSQLBackuper.writeLog('upload-', False, error)
            raise SACError("Выгрузка бэкапов PostgreSQL в облако не удалась",error)


# Позволяет запускать сценарий, если данный файл был запущен напрямую.
if __name__ == "__main__":
    LauncherPostgreSQLUploadToCloudFor1cKiP.main()
