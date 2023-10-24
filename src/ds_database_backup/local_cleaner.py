import lzma
from .configs import *
from .executor import Executor
from .utils import Utils


class Cleaner(Executor):

    def __init__(self, config: ConfigCleaner):
        super().__init__(config)
        self._config = config
        self._expire_date = Utils.calculate_expire_date(config.storage_time)

    @staticmethod
    def config_class():
        return ConfigCleaner

    def start(self):
        self._clean_backups_of_all_types()

    def _clean_backups_of_all_types(self) -> None:
        for backup_type_dir in self._config.backup_type_dirs.values():
            self._clean_backups(f'{self._config.full_path_to_backups}\\{backup_type_dir}')

        self._delete_manifest_files_without_backup()

        Utils.delete_local_empty_dirs([self._config.full_path_to_backups])
        
        if self._config.handle_wal_files:
            self._clean_wals()

    def _clean_backups(self, path_to_backups: str):
        backups = Utils.get_objects_on_disk(path_to_backups, only_files=True)
        backups_with_dates = self._get_backups_with_dates(backups)

        # noinspection PyTypeChecker
        sorted_backups = dict(sorted(backups_with_dates.items(), key=lambda x: x[1]))
        self._delete_outdated_backups(sorted_backups)

        if self._config.keep_one_backup_per_day:
            self._remove_extra_per_day_backups(sorted_backups)

        if self._config.backups_leave_amount > 0:
            self._backups_leave_n_plus_1(sorted_backups)

    @staticmethod
    def _get_backups_with_dates(backups: {str}) -> {str: datetime}:
        result = {}
        for backup in backups:
            backup_date = datetime.datetime.fromtimestamp(os.path.getmtime(backup), Utils.get_local_zone())
            result.update({backup: backup_date})
        return result

    def _delete_outdated_backups(self, sorted_backups: {str, datetime}):
        items = list(sorted_backups.items())
        i = 0
        while i < len(items):
            backup, backup_date = items[i]
            if backup_date < self._expire_date:
                os.remove(backup)
                sorted_backups.pop(backup)
            i += 1

    @staticmethod
    def _remove_extra_per_day_backups(sorted_backups: {str, datetime}):
        items = list(sorted_backups.items())
        i = 0
        suitable_backups = {}

        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' not in backup:
                older_backup_per_day = next(
                    (key for key, value in suitable_backups.items() if value == backup_date.date()), None)

                if older_backup_per_day is not None:
                    os.remove(older_backup_per_day)
                    sorted_backups.pop(older_backup_per_day)
                    suitable_backups.pop(older_backup_per_day)

                suitable_backups.update({backup: backup_date.date()})
            i += 1

    def _backups_leave_n_plus_1(self, sorted_backups: {str, datetime}):
        items = list(sorted_backups.items())
        i = 0
        total_backups = 0
        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' not in backup:
                total_backups += 1
            i += 1

        leave_amount = self._config.backups_leave_amount
        amount_to_delete = total_backups - leave_amount - 1 if total_backups >= leave_amount else 0
        items = list(sorted_backups.items())
        is_first = True
        i = 0
        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' not in backup:
                if is_first:
                    is_first = False
                elif amount_to_delete > 0:
                    os.remove(backup)
                    sorted_backups.pop(backup)
                    amount_to_delete -= 1
            i += 1

    def _delete_manifest_files_without_backup(self):
        manifests_list = Utils.get_objects_on_disk(self._config.full_path_to_backups, mask='_backup_manifest',
                                                   only_files=True)

        for manifest in manifests_list:
            manifest_root_dir = os.path.dirname(manifest)
            files_in_manifest_root_dir = Utils.get_objects_on_disk(manifest_root_dir, only_files=True)
            if len(files_in_manifest_root_dir) == 1:
                os.remove(manifest)

    def _clean_wals(self):
        wals = Utils.get_objects_on_disk(self._config.path_to_wal_files, only_files=True)
        if len(wals) == 0:
            return

        mask = '_backup_manifest'
        second_mask = '_base.'
        full_bck = Utils.get_objects_on_disk(self._config.full_path_to_backups, mask=mask,
                                             or_second_mask=second_mask, only_files=True)
        oldest_date = datetime.datetime.now(Utils.get_local_zone())
        oldest_label = None
        for file in full_bck:
            file_name = os.path.basename(file)
            bck_date = datetime.datetime.fromtimestamp(os.path.getmtime(file), Utils.get_local_zone())
            if bck_date is not None and oldest_date >= bck_date:
                oldest_date = bck_date
                if mask in file_name:
                    current_mask = mask
                else:
                    current_mask = second_mask

                oldest_label = file_name.split(current_mask)[0]
        if oldest_label is not None:
            wals = self._wals_to_remove(self._config.path_to_wal_files, oldest_label)
            for wal in wals:
                os.remove(wal)

    def _wals_to_remove(self, path, oldest_label, delete_unsuitable=False):
        oldest_number = self._wal_decimal_number(path, oldest_label)
        to_remove = []
        if oldest_number is None:
            return to_remove

        for root, dirs, files in os.walk(path):
            for file in files:
                filename = file
                if '.' in filename:
                    filename = filename.split('.')[0]
                filename = '0x' + filename
                # noinspection PyBroadException
                try:
                    cur_number = int(filename, base=16)
                    if cur_number < oldest_number:
                        to_remove.append(os.path.join(root, file))
                except:
                    if delete_unsuitable:
                        to_remove.append(os.path.join(root, file))

        return to_remove

    @staticmethod
    def _wal_decimal_number(path, label):
        result = None
        for root, dirs, files in os.walk(path):
            for file in files:
                if result is not None:
                    break
                if '.backup' not in file:
                    continue
                try:
                    if file.endswith('.backup'):
                        content = open(os.path.join(root, file))
                    else:
                        content = lzma.open(os.path.join(root, file), "rt")

                    temp_res = ''
                    for line in content:
                        if 'START WAL LOCATION' in line:
                            temp_res = line.split('file ')[1]
                        if label in line:
                            if ')' in temp_res:
                                temp_res = temp_res.split(')')[0]
                            result = '0x' + temp_res
                            break
                    content.close()

                except Exception as e:
                    print("TROUBLE - " + str(e))
                    continue

        if result is not None:
            result = int(result, base=16)
        return result
