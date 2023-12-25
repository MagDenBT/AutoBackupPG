from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict

from .exceptions import ConfigTypeMismatch, ModuleNotFound

DS_VERSION = '2.01.004'


class Executor(ABC):

    def __init__(self, config):
        if not isinstance(config, self.config_class()):
            raise ConfigTypeMismatch(config.__class__.__name__, self.config_class().__name__, self.__class__.__name__)

    @staticmethod
    @abstractmethod
    def config_class():
        pass

    @abstractmethod
    def start(self):
        pass


class ModuleFinder(Enum):
    from .backuper import PgBaseBackuper, PgDumpBackuper, OneCFbBackuper, MsSqlBackuper
    from .local_cleaner import Cleaner
    from .cloud_sync import AWSClient

    PG_BASE_BACKUPER = PgBaseBackuper
    PG_DUMP_BACKUPER = PgDumpBackuper
    ONE_C_FB_BACKUPER = OneCFbBackuper
    MS_SQL_BACKUPER = MsSqlBackuper

    CLEANER = Cleaner

    AWS_CLIENT = AWSClient

    @staticmethod
    def find_by_name(module_name: str):
        try:
            return ModuleFinder[module_name.upper()]
        except KeyError:
            pass

        for item in list(ModuleFinder):
            if item.name.upper() == module_name.upper():
                return item

        raise ModuleNotFound(module_name)


class DsBuilder(object):
    class Prototype(object):

        def __init__(self, module_type: ModuleFinder):
            self._module: Executor = module_type.value

        def initialize_config(self, params: Dict[str, str]) -> Executor:
            config_cls = self._module.config_class()
            config = config_cls(params)
            return self._module(config)

    @staticmethod
    def build(module_type: ModuleFinder):
        return DsBuilder.Prototype(module_type)
