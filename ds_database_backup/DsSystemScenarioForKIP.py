from lib.common.base_scenario import BaseScenario
from lib import SACError
from lib import global_logger

from executor import DsBuilder, ModuleFinder


class DsSystemScenarioForKIP(BaseScenario):

    def _get_available_tests(self):
        return [
            ("__check_config", self.__check_config, False),
        ]

    def __check_config(self):
        module_name = self.config["module_name"]
        try:
            DsBuilder\
                .build(ModuleFinder.find_by_name(module_name))\
                .initialize_config(self.config.scenario_context)
        except Exception as e:
            raise SACError(code='ARGS_ERROR', args=str(e))

    def _real(self):
        module_name = self.config["module_name"]
        try:
            global_logger.info(message=f'Старт {module_name}')

            DsBuilder \
                .build(ModuleFinder.find_by_name(module_name)) \
                .initialize_config(self.config.scenario_context)\
                .start()

            global_logger.info(message=f'Завершено {module_name}')
        except Exception as e:
            error = str(e)
            # global_logger.error(message=error)
            raise SACError(code="RUNTIME_ERROR", args=f'{module_name}: {error}')
