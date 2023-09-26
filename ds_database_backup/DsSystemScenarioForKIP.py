from AutoBackupPG.src.lib.common.base_scenario import BaseScenario
# Экспорт класса ошибок.
from AutoBackupPG.src.lib import SACError
from AutoBackupPG.src.lib import global_logger

from AutoBackupPG.ds_database_backup.executor import Builder, ModuleFinder


class DsSystemScenarioForKIP(BaseScenario):

    def _get_available_tests(self):
        return [
            ("__check_config", self.__check_config, False),
        ]

    def __check_config(self):
        module_name = self.config["module_name"]
        try:
            Builder\
                .build(ModuleFinder.find_by_name(module_name))\
                .initialize_config(self.config.scenario_context)
        except Exception as e:
            raise SACError(code='ARGS_ERROR', args=str(e))

    def _real(self):
        module_name = self.config["module_name"]
        try:
            global_logger.info(message=f'Старт {module_name}')

            Builder \
                .build(ModuleFinder.find_by_name(module_name)) \
                .initialize_config(self.config.scenario_context)\
                .start()

            global_logger.info(message=f'Завершено {module_name}')
        except Exception as e:
            error = str(e)
            # global_logger.error(message=error)
            raise SACError(code="RUNTIME_ERROR", args=f'{module_name}: {error}')
