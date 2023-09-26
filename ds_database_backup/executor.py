from abc import ABC, abstractmethod
from enum import Enum

from AutoBackupPG.ds_database_backup.configs import AbstractConfig


class Executor(ABC):

    @abstractmethod
    def start(self):
        pass


class ModuleType(Enum):
    from AutoBackupPG.ds_database_backup.backuper import PgBaseBackuper, PgDumpBackuper

    PG_BASE_BACKUPER = PgBaseBackuper
    PG_DUMP_BACKUPER = PgDumpBackuper


class Builder(object):

    class Prototype(object):
        def __init__(self, module_type: ModuleType):
            self._module = module_type

        def create(self, config: AbstractConfig) -> Executor:
            return self._module.value(config)

    @staticmethod
    def build(module_type: ModuleType):
        return Builder.Prototype(module_type)
