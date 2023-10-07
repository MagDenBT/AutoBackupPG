import os
from typing import List
import boto3
from boto3.s3.transfer import TransferConfig
from exceptions import RansomwareVirusTracesFound, AWSSpeedAutoAdjustmentError, \
    AWSConnectionError
from configs import ConfigAWSClient
from executor import Executor
from utils import Utils


class AWSClient(Executor):

    def __init__(self, config: ConfigAWSClient):
        super().__init__(config)
        self._config = config
        session = boto3.session.Session()
        self._aws_client = session.client(
            service_name='s3',
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key
        )
        self._cloud_backups: [{str: str}] = []

    @staticmethod
    def config_class():
        return ConfigAWSClient

    def start(self):
        self._sync()

    def _sync(self) -> None:
        self._check_connection()

        all_cloud_backups = self._get_objects_on_aws(with_hash=self._config.with_hash)
        for cloud_backup in all_cloud_backups:
            if cloud_backup['path'].startswith(self._config.path_to_backups_cloud):
                self._cloud_backups.append(cloud_backup)

        corrupt_files = self._get_corrupt_files()
        local_backups = Utils.get_objects_on_disk(self._config.full_path_to_backups)
        if len(local_backups) > 0:
            if len(corrupt_files) == 0:
                self._clean_cloud()

            self._upload_to_cloud()

        if len(corrupt_files) > 0:
            raise RansomwareVirusTracesFound(corrupt_files)

    def _check_connection(self) -> None:
        try:
            self._aws_client.list_objects(Bucket=self._config.bucket)
        except Exception as e:
            raise AWSConnectionError(e)

    def _get_objects_on_aws(self, only_files=True, with_hash=True) -> [{str: str}]:
        result: [{str: str}] = []
        # noinspection PyBroadException
        try:
            for obj in \
                    self._aws_client.list_objects_v2(Bucket=self._config.bucket,
                                                     Prefix=self._config.path_to_backups_cloud)[
                        'Contents']:
                resource_name = obj['Key']
                if resource_name.endswith('/') and only_files:
                    continue

                file_hash = Utils.get_md5_aws(self._aws_client, self._config.bucket, resource_name) if with_hash else ''
                result.append({'hash': file_hash, 'path': resource_name})
        except:
            pass
        return result

    def _get_corrupt_files(self) -> List[str]:
        loca_files = Utils.get_objects_on_disk(self._config.full_path_to_backups, only_files=True)
        corrupt_files = []
        for file in loca_files:
            if self._is_corrupt_extension(file):
                corrupt_files.append(file)
        return corrupt_files

    def _is_corrupt_extension(self, file: str) -> bool:
        arr = os.path.splitext(file)
        extension = arr[len(arr) - 1]
        if extension == '' and not file.endswith('backup_manifest'):
            try:
                int('0x' + os.path.basename(file), base=16)
            except ValueError:
                return True
        elif extension != '' and extension not in self._get_valid_extensions():
            return True

        return False

    @staticmethod
    def _get_valid_extensions():
        return ['.gz', '.xz', '.txz', '.backup', '.dump']

    def _upload_to_cloud(self):

        local_backups = Utils.get_objects_on_disk(self._config.full_path_to_backups)
        to_upload = self._compute_files_to_upload(local_backups)

        if len(to_upload) == 0:
            return

        for backup_local, savefile in to_upload.items():
            self._upload_file(backup_local, savefile)

    def _upload_file(self, local_file, target_file, adjust_bandwidth=True):

        upload_config = TransferConfig(multipart_chunksize=self._config.chunk_size,
                                       max_bandwidth=self._config.max_bandwidth_bytes)

        from botocore.exceptions import (
            ConnectionClosedError,
            ConnectTimeoutError,
            ReadTimeoutError
        )

        try:
            self._aws_client.upload_file(local_file, self._config.bucket, target_file, Config=upload_config)
        except (
                ConnectionClosedError,
                ReadTimeoutError,
                ConnectTimeoutError,
        ):
            if self._config.max_bandwidth_bytes is None:
                new_max_bandwidth_bytes = self._config.bandwidth_limit
            else:
                new_max_bandwidth_bytes = self._config.max_bandwidth_bytes / 100 * 70

            if adjust_bandwidth:
                if self._config.threshold_bandwidth < new_max_bandwidth_bytes:
                    self._config.set_max_bandwidth_bytes(new_max_bandwidth_bytes)
                    self._upload_file(local_file, target_file, True)
                else:
                    raise AWSSpeedAutoAdjustmentError(self._config.max_bandwidth_bytes / 125)

    def _compute_files_to_upload(self, local_backups: [str]) -> {str: str}:
        result = {}
        for backup in local_backups:
            file_name = os.path.basename(backup)
            file_hash = Utils.get_md5(backup, self._config.chunk_size) if self._config.with_hash else ''

            if self._file_is_in_cloud(file_name, file_hash):
                continue

            full_path_cloud = self._transform_to_cloud_path(backup)
            result.update({backup: full_path_cloud})

        return result

    def _transform_to_cloud_path(self, local_path) -> str:
        # Преобразование пути в нормальный формат
        normalized_path = os.path.normpath(local_path)
        # Разделение пути на части
        path_parts = normalized_path.split(os.sep)
        # Находим индекс каталога идущего за custom_dir в пути
        after_custom_dir_index = path_parts.index(self._config.custom_dir) + 1
        # Обрезаем путь до custom_dir включительно и преобразуем его в нужный формат
        path_parts = path_parts[after_custom_dir_index:]
        # Преобразуем имена каталогов в корректные для S3
        i = 0
        while i < len(path_parts):
            if i == len(path_parts) - 1:
                break
            path_parts[i] = self._config.aws_correct_folder_name(path_parts[i])
            i += 1

        cloud_path = '/'.join(path_parts)
        cloud_path = f'{self._config.path_to_backups_cloud}/{cloud_path}'
        return cloud_path

    def _file_is_in_cloud(self, file_name, file_hash: str) -> bool:
        for cloud_backup in self._cloud_backups:
            if cloud_backup['path'].endswith(file_name) and cloud_backup['hash'] == file_hash:
                return True
        return False

    def _clean_cloud(self):
        extra_backups = self._get_extra_bck_on_cloud()
        if len(extra_backups) > 0:
            objects = []
            for backup in extra_backups:
                objects.append({'Key': backup})
            self._aws_client.delete_objects(Bucket=self._config.bucket, Delete={'Objects': objects})

    def _get_extra_bck_on_cloud(self) -> [str]:
        result = []
        loca_files = Utils.get_objects_on_disk(self._config.full_path_to_backups, only_files=True)
        loca_files_and_hashes = Utils.add_hashes_to_local_files(loca_files, self._config.chunk_size)

        for cloud_file in self._cloud_backups:
            cloud_file_name = os.path.basename(cloud_file['path'])
            to_delete = True
            for file, file_hash in loca_files_and_hashes.items():
                if file.endswith(cloud_file_name):
                    if self._config.with_hash:
                        if file_hash == cloud_file['hash']:
                            to_delete = False
                            break
                    else:
                        to_delete = False
                        break
            if to_delete:
                result.append(cloud_file['path'])
        return result

    def _delete_empty_dirs_on_aws(self):
        empty_dirs = self._empty_aws_cloud_dirs()
        try:
            empty_dirs.remove(self._config.path_to_backups_cloud + '/')
        except KeyError:
            pass

        empty_dirs = Utils.optimize_remove_list_dir(empty_dirs)

        for_deletion = []
        for path in empty_dirs:
            for_deletion.append({'Key': path})

        self._aws_client.delete_objects(Bucket=self._config.bucket, Delete={'Objects': for_deletion})

    def _empty_aws_cloud_dirs(self) -> [str]:
        obj_on_aws = self._get_objects_on_aws(only_files=False, with_hash=False)
        temp = []
        for pair in obj_on_aws:
            temp.append(pair['path'])
        obj_on_aws = set(temp)
        result = obj_on_aws.copy()

        for i, path in enumerate(obj_on_aws):
            if not path.endswith('/'):  # is file
                result.remove(path)

                dirs = path.split('/')
                firm_path = ''
                for z, val in enumerate(dirs):
                    if z == len(dirs) - 1:
                        break
                    firm_path += f'{val}/'
                    try:
                        result.remove(firm_path)
                        continue
                    except KeyError:
                        continue
        return result
