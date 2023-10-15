from executor import DsBuilder, ModuleFinder, DS_VERSION
import os
import psutil
from datetime import datetime
import speedtest

from lib.common.base_scenario import BaseScenario
from lib import SACError
from lib import global_logger


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
            DsBuilder \
                .build(ModuleFinder.find_by_name(module_name)) \
                .initialize_config(self.config.scenario_context)
        except Exception as e:
            raise SACError(code='ARGS_ERROR', args=str(e))

    def _real(self):
        self.ds_module_name = self.config["module_name"]
        try:
            global_logger.info(message=f'Старт {self.ds_module_name}')

            self._prepare_scenario_config()
            DsBuilder \
                .build(ModuleFinder.find_by_name(self.ds_module_name)) \
                .initialize_config(self.config.scenario_context) \
                .start()

            global_logger.info(message=f'Завершено {self.ds_module_name}')
        except Exception as error:
            raise SACError(code="RUNTIME_ERROR", args=f'{self.ds_module_name}: {str(error)}')

    def _prepare_scenario_config(self):
        kip_context = self.config.scenario_context
        handle_full_bcks = kip_context.get('handle_full_bcks')
        if handle_full_bcks is False:
            kip_context.update({'exclude_dirs': ['Full']})

        self._restrict_resource_consumption()

    def _restrict_resource_consumption(self):
        if self._should_restrict():
            self._restrict_cpu_consumption()
            if ModuleFinder.find_by_name(self.ds_module_name) is ModuleFinder.AWS_CLIENT:
                self._limit_bandwidth()

    def _should_restrict(self) -> bool:
        kip_context = self.config.scenario_context
        customer_work_start = kip_context.get('customer_work_start')
        customer_work_end = kip_context.get('customer_work_end')

        if self._customer_schedule_is_correct(customer_work_start, customer_work_end):
            data = datetime.now()
            current_hour = data.time().hour
            if customer_work_start <= current_hour <= customer_work_end:
                global_logger.info(message=f'Время машины: {str(data)}')
                return True
        return False

    @staticmethod
    def _customer_schedule_is_correct(start, end) -> bool:
        return start is not None and end is not None \
               and start >= 0 and end > 0

    @staticmethod
    def _restrict_cpu_consumption():
        current_process = psutil.Process(os.getpid())
        # noinspection PyUnresolvedReferences
        current_process.nice(psutil.IDLE_PRIORITY_CLASS)
        global_logger.info(message=f'Активировано ограничение потребления ресурсов CPU')

    def _limit_bandwidth(self):
        kip_context = self.config.scenario_context
        max_bandwidth_percent = kip_context.get('max_bandwidth_percent')
        if max_bandwidth_percent is None:
            return

        st = speedtest.Speedtest()
        st.get_servers()
        current_upload_speed_bits = st.upload()
        current_upload_speed_mbit = round(current_upload_speed_bits / 1000000)

        limited_upload_speed_bits = round(current_upload_speed_bits / 100 * max_bandwidth_percent)
        limited_upload_speed_bytes = round(limited_upload_speed_bits / 8)
        kip_context.update({'max_bandwidth_bytes': limited_upload_speed_bytes})

        limited_upload_speed_mbit = round(limited_upload_speed_bits / 1000000)
        global_logger.info(message=f'Максимальная скорость выгрузки {current_upload_speed_mbit} Мбит/с'
                                   f' ограничена до {limited_upload_speed_mbit} Мбит/с')
