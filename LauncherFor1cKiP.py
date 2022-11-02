# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# Этот модуль требуется импортировать в начале каждого скрипта.
from lib.common import bootstrap
# Экспорт базового класса для сценария. Он отвечает за настройку логгирования,
# базовую валидацию параметров, перехват исключений и корректное завершение
# скрипта и т.д.
from lib.common.base_scenario import BaseScenario
# Экспорт класса ошибок.
from lib.common.errors import SACError

import os
import subprocess
from datetime import datetime
import random
import requests as requests

# CLoud settings
import PosgreSQLBackuper

class LauncherFor1cKiP(BaseScenario):

def _real(self):

        PosgreSQLBackuper.setParam()

        message = ''
        if not PosgreSQLBackuper.checkParams(message):
            PosgreSQLBackuper.writeLog('backup-', False, message)

        try:
            PosgreSQLBackuper.createFullBackup()
            PosgreSQLBackuper.writeLog('backup-', True, '')
        except Exception as e:
            PosgreSQLBackuper.writeLog('backup-', False, str(e))

        try:
            PosgreSQLBackuper.uploadOnYandexCloud()
            PosgreSQLBackuper.writeLog('upload-', True, '')
        except Exception as e:
            PosgreSQLBackuper.writeLog('upload-', False, str(e))




# Позволяет запускать сценарий, если данный файл был запущен напрямую.
if __name__ == "__main__":
    PosgreSQLBackuper.main()

