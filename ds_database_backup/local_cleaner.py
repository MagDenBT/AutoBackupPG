import lzma
import tarfile
import json
from dateutil import parser

from typing import Dict

from AutoBackupPG.ds_database_backup.configs import *
from AutoBackupPG.ds_database_backup.utils import Utils


class AbstractCleaner(ABC):
    @abstractmethod
    def delete_outdated_backups(self) -> None:
        pass

    @staticmethod
    def _delete_local_empty_dirs(paths: List[str]) -> None:
        for path in paths:
            if os.path.exists(path):
                for root, dirs, files in os.walk(path):
                    for _dir in dirs:
                        dir_path = os.path.join(root, _dir)
                        AbstractCleaner._safely_delete_dir(dir_path)

    @staticmethod
    def _safely_delete_dir(path: str) -> bool:
        delete_it = True
        for root, dirs, files in os.walk(path):
            for _dir in dirs:
                delete_it = AbstractCleaner._safely_delete_dir(os.path.join(root, _dir))
            for _ in files:
                delete_it = False
                break
        if delete_it:
            os.rmdir(path)
        return delete_it


class Cleaner(AbstractCleaner):

    def __init__(self, config: ConfigNonPgBaseCleaner):
        self._config = config

    def delete_outdated_backups(self) -> None:
        expire_date = Utils.calculate_expire_date(self._config.storage_time)
        self._clean_backups(expire_date)
        super()._delete_local_empty_dirs([self._config.full_path_to_backups])

    def _clean_backups(self, expire_date: datetime):
        backups = Utils.get_objects_on_disk(self._config.path_to_backups, only_files=True)
        dic_backups = {}

        for backup in backups:
            backup_date = datetime.datetime.fromtimestamp(os.path.getmtime(backup), Utils.get_local_zone())
            dic_backups.update({backup: backup_date})

        # noinspection PyTypeChecker
        dic_backups = dict(sorted(dic_backups.items(), key=lambda x: x[1]))

        items = list(dic_backups.items())
        i = 0
        suitable_backups = {}
        while i < len(items):
            backup, backup_date = items[i]

            if backup_date < expire_date:
                os.remove(backup)
                dic_backups.pop(backup)
            elif self._config.keep_one_backup_per_day:
                older_backup_per_day = next(
                    (key for key, value in suitable_backups.items() if value == backup_date.date()), None)

                if older_backup_per_day is not None:
                    os.remove(older_backup_per_day)
                    dic_backups.pop(older_backup_per_day)
                    suitable_backups.pop(older_backup_per_day)

                suitable_backups.update({backup: backup_date.date()})
            i += 1

        leave_amount = self._config.backups_leave_amount
        if leave_amount > 0:
            self._backups_leave_n_plus_1(dic_backups, leave_amount)

    @staticmethod
    def _backups_leave_n_plus_1(sorted_backups: Dict[str, datetime], leave_amount: int):
        i = 0
        items = list(sorted_backups.items())
        while i < len(items):
            backup, backup_date = items[i]
            if 0 < i < len(items) - leave_amount:
                os.remove(backup)
                sorted_backups.pop(backup)
            i += 1


class CleanerPgBaseBackups(AbstractCleaner):

    def __init__(self, config: ConfigPgBaseCleaner):
        self._config = config
        self._timezone_map = self._get_timezone_map()

    @staticmethod
    def _get_timezone_map() -> Dict[str:int]:
        return {
            "A": 1 * 3600,
            "ACDT": 10.5 * 3600,
            "ACST": 9.5 * 3600,
            "ACT": -5 * 3600,
            "ACWST": 8.75 * 3600,
            "ADT": 4 * 3600,
            "AEDT": 11 * 3600,
            "AEST": 10 * 3600,
            "AET": 10 * 3600,
            "AFT": 4.5 * 3600,
            "AKDT": -8 * 3600,
            "AKST": -9 * 3600,
            "ALMT": 6 * 3600,
            "AMST": -3 * 3600,
            "AMT": -4 * 3600,
            "ANAST": 12 * 3600,
            "ANAT": 12 * 3600,
            "AQTT": 5 * 3600,
            "ART": -3 * 3600,
            "AST": 3 * 3600,
            "AT": -4 * 3600,
            "AWDT": 9 * 3600,
            "AWST": 8 * 3600,
            "AZOST": 0 * 3600,
            "AZOT": -1 * 3600,
            "AZST": 5 * 3600,
            "AZT": 4 * 3600,
            "AoE": -12 * 3600,
            "B": 2 * 3600,
            "BNT": 8 * 3600,
            "BOT": -4 * 3600,
            "BRST": -2 * 3600,
            "BRT": -3 * 3600,
            "BST": 6 * 3600,
            "BTT": 6 * 3600,
            "C": 3 * 3600,
            "CAST": 8 * 3600,
            "CAT": 2 * 3600,
            "CCT": 6.5 * 3600,
            "CDT": -5 * 3600,
            "CEST": 2 * 3600,
            "CET": 1 * 3600,
            "CHADT": 13.75 * 3600,
            "CHAST": 12.75 * 3600,
            "CHOST": 9 * 3600,
            "CHOT": 8 * 3600,
            "CHUT": 10 * 3600,
            "CIDST": -4 * 3600,
            "CIST": -5 * 3600,
            "CKT": -10 * 3600,
            "CLST": -3 * 3600,
            "CLT": -4 * 3600,
            "COT": -5 * 3600,
            "CST": -6 * 3600,
            "CT": -6 * 3600,
            "CVT": -1 * 3600,
            "CXT": 7 * 3600,
            "ChST": 10 * 3600,
            "D": 4 * 3600,
            "DAVT": 7 * 3600,
            "DDUT": 10 * 3600,
            "E": 5 * 3600,
            "EASST": -5 * 3600,
            "EAST": -6 * 3600,
            "EAT": 3 * 3600,
            "ECT": -5 * 3600,
            "EDT": -4 * 3600,
            "EEST": 3 * 3600,
            "EET": 2 * 3600,
            "EGST": 0 * 3600,
            "EGT": -1 * 3600,
            "EST": -5 * 3600,
            "ET": -5 * 3600,
            "F": 6 * 3600,
            "FET": 3 * 3600,
            "FJST": 13 * 3600,
            "FJT": 12 * 3600,
            "FKST": -3 * 3600,
            "FKT": -4 * 3600,
            "FNT": -2 * 3600,
            "G": 7 * 3600,
            "GALT": -6 * 3600,
            "GAMT": -9 * 3600,
            "GET": 4 * 3600,
            "GFT": -3 * 3600,
            "GILT": 12 * 3600,
            "GMT": 0 * 3600,
            "GST": 4 * 3600,
            "GYT": -4 * 3600,
            "H": 8 * 3600,
            "HDT": -9 * 3600,
            "HKT": 8 * 3600,
            "HOVST": 8 * 3600,
            "HOVT": 7 * 3600,
            "HST": -10 * 3600,
            "I": 9 * 3600,
            "ICT": 7 * 3600,
            "IDT": 3 * 3600,
            "IOT": 6 * 3600,
            "IRDT": 4.5 * 3600,
            "IRKST": 9 * 3600,
            "IRKT": 8 * 3600,
            "IRST": 3.5 * 3600,
            "IST": 5.5 * 3600,
            "JST": 9 * 3600,
            "K": 10 * 3600,
            "KGT": 6 * 3600,
            "KOST": 11 * 3600,
            "KRAST": 8 * 3600,
            "KRAT": 7 * 3600,
            "KST": 9 * 3600,
            "KUYT": 4 * 3600,
            "L": 11 * 3600,
            "LHDT": 11 * 3600,
            "LHST": 10.5 * 3600,
            "LINT": 14 * 3600,
            "M": 12 * 3600,
            "MAGST": 12 * 3600,
            "MAGT": 11 * 3600,
            "MART": 9.5 * 3600,
            "MAWT": 5 * 3600,
            "MDT": -6 * 3600,
            "MHT": 12 * 3600,
            "MMT": 6.5 * 3600,
            "MSD": 4 * 3600,
            "MSK": 3 * 3600,
            "MST": -7 * 3600,
            "MT": -7 * 3600,
            "MUT": 4 * 3600,
            "MVT": 5 * 3600,
            "MYT": 8 * 3600,
            "N": -1 * 3600,
            "NCT": 11 * 3600,
            "NDT": 2.5 * 3600,
            "NFT": 11 * 3600,
            "NOVST": 7 * 3600,
            "NOVT": 7 * 3600,
            "NPT": 5.5 * 3600,
            "NRT": 12 * 3600,
            "NST": 3.5 * 3600,
            "NUT": -11 * 3600,
            "NZDT": 13 * 3600,
            "NZST": 12 * 3600,
            "O": -2 * 3600,
            "OMSST": 7 * 3600,
            "OMST": 6 * 3600,
            "ORAT": 5 * 3600,
            "P": -3 * 3600,
            "PDT": -7 * 3600,
            "PET": -5 * 3600,
            "PETST": 12 * 3600,
            "PETT": 12 * 3600,
            "PGT": 10 * 3600,
            "PHOT": 13 * 3600,
            "PHT": 8 * 3600,
            "PKT": 5 * 3600,
            "PMDT": -2 * 3600,
            "PMST": -3 * 3600,
            "PONT": 11 * 3600,
            "PST": -8 * 3600,
            "PT": -8 * 3600,
            "PWT": 9 * 3600,
            "PYST": -3 * 3600,
            "PYT": -4 * 3600,
            "Q": -4 * 3600,
            "QYZT": 6 * 3600,
            "R": -5 * 3600,
            "RET": 4 * 3600,
            "ROTT": -3 * 3600,
            "S": -6 * 3600,
            "SAKT": 11 * 3600,
            "SAMT": 4 * 3600,
            "SAST": 2 * 3600,
            "SBT": 11 * 3600,
            "SCT": 4 * 3600,
            "SGT": 8 * 3600,
            "SRET": 11 * 3600,
            "SRT": -3 * 3600,
            "SST": -11 * 3600,
            "SYOT": 3 * 3600,
            "T": -7 * 3600,
            "TAHT": -10 * 3600,
            "TFT": 5 * 3600,
            "TJT": 5 * 3600,
            "TKT": 13 * 3600,
            "TLT": 9 * 3600,
            "TMT": 5 * 3600,
            "TOST": 14 * 3600,
            "TOT": 13 * 3600,
            "TRT": 3 * 3600,
            "TVT": 12 * 3600,
            "U": -8 * 3600,
            "ULAST": 9 * 3600,
            "ULAT": 8 * 3600,
            "UTC": 0 * 3600,
            "UYST": -2 * 3600,
            "UYT": -3 * 3600,
            "UZT": 5 * 3600,
            "V": -9 * 3600,
            "VET": -4 * 3600,
            "VLAST": 11 * 3600,
            "VLAT": 10 * 3600,
            "VOST": 6 * 3600,
            "VUT": 11 * 3600,
            "W": -10 * 3600,
            "WAKT": 12 * 3600,
            "WARST": -3 * 3600,
            "WAST": 2 * 3600,
            "WAT": 1 * 3600,
            "WEST": 1 * 3600,
            "WET": 0 * 3600,
            "WFT": 12 * 3600,
            "WGST": -2 * 3600,
            "WGT": -3 * 3600,
            "WIB": 7 * 3600,
            "WIT": 9 * 3600,
            "WITA": 8 * 3600,
            "WST": 14 * 3600,
            "WT": 0 * 3600,
            "X": -11 * 3600,
            "Y": -12 * 3600,
            "YAKST": 10 * 3600,
            "YAKT": 9 * 3600,
            "YAPT": 10 * 3600,
            "YEKST": 6 * 3600,
            "YEKT": 5 * 3600,
            "Z": 0 * 3600,
        }

    def delete_outdated_backups(self) -> None:
        expire_date = Utils.calculate_expire_date(self._config.storage_time)

        self._clean_backups(expire_date)
        if self._config.handle_wal_files:
            self._clean_wals()

        to_clean_paths = [self._config.full_path_to_backups]
        if self._config.handle_wal_files:
            to_clean_paths.append(self._config.path_to_wal_files)
        super()._delete_local_empty_dirs(to_clean_paths)

    def _clean_backups(self, expire_date):
        expired_backups = self._get_expired_full_backups(expire_date)

        for backup in expired_backups:
            os.remove(backup)

        if self._config.keep_one_backup_per_day:
            self._remove_extra_per_day_backups()

        leave_amount = self._config.backups_leave_amount
        if leave_amount > 0:
            self._backups_leave_n_plus_1(leave_amount)

    def _remove_extra_per_day_backups(self):
        backups = self._get_backups_with_dates()
        # noinspection PyTypeChecker
        backups_sorted_by_date = dict(sorted(backups.items(), key=lambda x: x[1]))

        items = list(backups_sorted_by_date.items())

        i = 0
        suitable_backups = {}

        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' not in backup:
                older_backup_per_day = next(
                    (key for key, value in suitable_backups.items() if value == backup_date.date()), None)

                if older_backup_per_day is not None:
                    os.remove(older_backup_per_day)
                    backups_sorted_by_date.pop(older_backup_per_day)
                    suitable_backups.pop(older_backup_per_day)

                suitable_backups.update({backup: backup_date.date()})
            i += 1

        self._delete_manifest_files_without_backup(backups_sorted_by_date)

    def _backups_leave_n_plus_1(self, leave_amount: int):
        backups = self._get_backups_with_dates()
        # noinspection PyTypeChecker
        backups_sorted_by_date = dict(sorted(backups.items(), key=lambda x: x[1]))

        items = list(backups_sorted_by_date.items())
        i = 0
        total_backups = 0
        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' not in backup:
                total_backups += 1
            i += 1

        amount_to_delete = total_backups - leave_amount - 1 if total_backups >= leave_amount else 0
        items = list(backups_sorted_by_date.items())
        is_first = True
        i = 0
        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' not in backup:
                if is_first:
                    is_first = False
                elif amount_to_delete > 0:
                    os.remove(backup)
                    backups_sorted_by_date.pop(backup)
                    amount_to_delete -= 1
            i += 1

        self._delete_manifest_files_without_backup(backups_sorted_by_date)

    def _get_backups_with_dates(self):
        mask = '_backup_manifest'
        second_mask = '_base.'
        backups = Utils.get_objects_on_disk(self._config.full_path_to_backups, mask=mask,
                                            or_second_mask=second_mask, only_files=True)
        result = {}
        for file in backups:
            file_name = os.path.basename(file)
            bck_date = self._read_full_bck_create_date(file)
            if bck_date is not None:
                current_mask = mask if mask in file_name else second_mask
                portion = Utils.get_objects_on_disk(os.path.dirname(file),
                                                    mask=file_name.split(current_mask)[0], only_files=True)
                for bck_file in portion:
                    result.update({bck_file: bck_date})
        return result

    @staticmethod
    def _delete_manifest_files_without_backup(backups_sorted_by_date):
        items = list(backups_sorted_by_date.items())
        i = 0
        while i < len(items):
            backup, backup_date = items[i]
            if '_backup_manifest' in backup:
                dir_path = os.path.dirname(backup)
                base_bcks = Utils.get_objects_on_disk(dir_path, mask='_base', only_files=True)
                if len(base_bcks) == 0:
                    os.remove(backup)
                    backups_sorted_by_date.pop(backup)
            i += 1

    def _get_expired_full_backups(self, expire_date: datetime):
        mask = '_backup_manifest'
        second_mask = '_base.'
        full_bck = Utils.get_objects_on_disk(self._config.full_path_to_backups, mask=mask,
                                             or_second_mask=second_mask, only_files=True)
        result = []

        for file in full_bck:
            file_name = os.path.basename(file)
            bck_date = self._read_full_bck_create_date(file)

            if bck_date is not None and bck_date < expire_date:
                current_mask = mask if mask in file_name else second_mask
                portion_if_has_subdir = Utils.get_objects_on_disk(os.path.dirname(file),
                                                                  mask=file_name.split(current_mask)[0],
                                                                  only_files=True)
                result.extend(portion_if_has_subdir)

        return list(set(result))

    def _read_full_bck_create_date(self, backup: str):
        if self._config.use_simple_way_read_bck_date:
            return datetime.datetime.fromtimestamp(os.path.getmtime(backup), Utils.get_local_zone())

        date_str = None
        # noinspection PyBroadException
        try:

            if not tarfile.is_tarfile(backup):
                if backup.endswith('backup_manifest'):
                    with open(backup) as json_file:
                        data = json.load(json_file)
                        for p in data['Files']:
                            if p['Path'] == 'backup_label':
                                date_str = p['Last-Modified']
                                break
                    json_file.close()
            else:
                tar = tarfile.open(backup, "r")
                members = tar.getmembers()
                if len(members) == 1:
                    if members[0].isfile():
                        tar = tar.extractfile(members[0])
                    ex_file = tar.extractfile('backup_label')
                    date_str = self._read_time_from_backup_label(ex_file)
                elif len(members) == 2:
                    for member in members:
                        if member.name.endswith('backup_manifest'):
                            ex_file = tar.extractfile(member.name)
                            data = json.load(ex_file)
                            for p in data['Files']:
                                if p['Path'] == 'backup_label':
                                    date_str = p['Last-Modified']
                                    break
                            ex_file.close()
                            break
                else:
                    ex_file = tar.extractfile('backup_label')
                    date_str = self._read_time_from_backup_label(ex_file)
        except:
            date_str = None

        if date_str is None:
            return None

        tz = date_str.split(' ')[2]
        tzinfos = ''
        for item in self._timezone_map.items():
            if item[0] == tz:
                tzinfos = {item[0]: item[1]}
                break
        result = parser.parse(date_str, tzinfos=tzinfos)
        return result

    @staticmethod
    def _read_time_from_backup_label(file_obj):
        date_str = None
        while True:
            # считываем строку
            line = file_obj.readline()
            # прерываем цикл, если строка пустая
            if not line:
                break
            # выводим строку
            text = str(line.strip())
            if 'START TIME:' in text:
                date_str = text.split('START TIME:')[1]
                date_str = date_str.replace('\'', '')
                date_str = date_str.replace(' ', '', 1)
                break

            # закрываем файл
        file_obj.close()
        return date_str

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
            bck_date = self._read_full_bck_create_date(file)
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
