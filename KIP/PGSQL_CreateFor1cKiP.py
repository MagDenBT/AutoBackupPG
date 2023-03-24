# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# Этот модуль требуется импортировать в начале каждого скрипта.
import json
import traceback

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

from PGSQL_Backuper import Manager


class PGSQL_CreateFor1cKiP(BaseScenario):

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

        write_to_log_file = self.config['write_to_log_file']
        manager = Manager(new_args=self.config.scenario_context, create_backup=True)
        global_logger.info(message="Запущен процесс создания бэкапа PostgreSQL")
        dump_error = None
        for name, backuper in manager.backupers().items():
            try:
                backuper._create_backup()
                global_logger.info(message=f'{name}- успех!')
                if write_to_log_file:
                    manager.write_log(f'{name}-', True, '')
            except Exception as e:
                if "dump" in name:
                    dump_error = str(e)
                global_logger.warning(message=f'{name}- ОШИБКА! - {str(e)}. {traceback.format_exc()}')
                if write_to_log_file:
                    manager.write_log(f'{name}-', False, str(e))

        if dump_error is not None:
            raise SACError(24, f'Дамп не создан! - {dump_error}')






# Позволяет запускать сценарий, если данный файл был запущен напрямую.
if __name__ == "__main__":
    PGSQL_CreateFor1cKiP.main()
