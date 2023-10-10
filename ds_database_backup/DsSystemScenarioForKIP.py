from lib.common.base_scenario import BaseScenario
from lib import SACError
from lib import global_logger

from executor import DsBuilder, ModuleFinder, DS_VERSION


class DsSystemScenarioForKIP(BaseScenario):

    def _get_available_tests(self):
        return [
            ("__check_ds_version", self.__check_ds_scripts_version, False),
            ("__check_config", self.__check_config, False),
        ]

    def __check_ds_scripts_version(self):
        version_must_be = self.config["ds_scripts_version"]

        if DS_VERSION != version_must_be:
            raise SACError(code='ARGS_ERROR', args=f'Версия ds-скриптов на машине - {DS_VERSION},'
                                                   f' а должна быть - {version_must_be}')

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
        self._prepare_scenario_config()
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

    def _prepare_scenario_config(self):
        if self.config.scenario_context.get('handle_full_bcks'):
            self.config.scenario_context.update({'exclude_dirs': ['Full']})

