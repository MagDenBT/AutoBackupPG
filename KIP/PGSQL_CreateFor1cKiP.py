import traceback

from lib.common.base_scenario import BaseScenario
from lib.common.errors import SACError
from lib.common.logger import global_logger

from PGSQL_Backuper import Manager


class PGSQL_CreateFor1cKiP(BaseScenario):

    def _validate_specific_data(self):
        pass

    def _after_init(self):
        pass


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
        try:
            if(self.config.scenario_context['database_name'].lower() == '_all_'):
                self.config.scenario_context['database_name'] = ''
        except:
            global_logger.warning(message=f'Не удалось настроить параметры на ALL_BASES')

        try:
            manager = Manager(new_args=self.config.scenario_context, create_backup=True)
        except Exception as e:
            error = str(e)
            global_logger.error(message=error)
            raise SACError(24, error)

        global_logger.info(message="Запущен процесс создания бэкапа PostgreSQL")
        exceptions = {}
        for name, backuper in manager.backupers().items():
            try:
                backuper.create_backup()
                global_logger.info(message=f'{name}- успех!')
                if write_to_log_file:
                    manager.write_log(f'{name}-', True, '')
            except Exception as e:
                exceptions.update({name: str(e)})
                if write_to_log_file:
                    manager.write_log(f'{name}-', False, str(e))

        if len(exceptions) > 0:
            exc_texts = '\n'.join([f"{key}: {value}" for key, value in exceptions.items()])
            dump_error = exceptions.get("pg_dump")

            if len(exceptions) == len(manager.backupers()):
                raise SACError(24, f'Бэкапы не созданы! - {exc_texts}')
            elif dump_error is not None:
                raise SACError(24, f'Дамп не создан! - {dump_error}')
            else:
                global_logger.warning(message=f'{exc_texts}. {traceback.format_exc()}')




if __name__ == "__main__":
    PGSQL_CreateFor1cKiP.main()
