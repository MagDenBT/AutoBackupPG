import os
from typing import List
import boto3
from boto3.s3.transfer import TransferConfig
from AutoBackupPG.ds_database_backup.exceptions import AWSTimeTooSkewedError, AWSBucketError, \
    RansomwareVirusTracesFound, AWSSpeedAutoAdjustmentError
from AutoBackupPG.ds_database_backup.configs import ConfigAWSClient
from AutoBackupPG.ds_database_backup.executor import Executor
from AutoBackupPG.ds_database_backup.utils import Utils


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
        self._cloud_backups = []

    @staticmethod
    def config_class():
        return ConfigAWSClient

    def start(self):
        self._sync()

    def _sync(self) -> None:
        if self._local_time_is_too_skewed():
            raise AWSTimeTooSkewedError()

        self._check_bucket()

        all_cloud_backups = self._get_objects_on_aws(with_hash=self._config.with_hash)
        for cloud_backup in all_cloud_backups:
            if cloud_backup.startswith(self._config.path_to_backups_cloud):
                self._cloud_backups.append(cloud_backup)

        corrupt_files = self._get_corrupt_files()
        if len(corrupt_files) == 0:
            self._clean_cloud()

        self._upload_to_cloud()

        if len(corrupt_files) > 0:
            raise RansomwareVirusTracesFound(corrupt_files)

    def _local_time_is_too_skewed(self):
        from botocore.exceptions import ClientError
        is_too_skewed = False

        try:
            self._aws_client.list_objects(Bucket=self._config.bucket)
        except ClientError as e:
            is_too_skewed = e.response.get('Error').get('Code') == 'RequestTimeTooSkewed'

        return is_too_skewed

    def _check_bucket(self) -> None:
        try:
            self._aws_client.head_bucket(Bucket=self._config.bucket)
        except Exception as e:
            AWSBucketError(e)

    def _get_objects_on_aws(self, only_files=True, with_hash=True):
        result = []

        # noinspection PyBroadException
        try:
            for obj in \
                    self._aws_client.list_objects_v2(Bucket=self._config.bucket,
                                                     Prefix=self._config.path_to_backups_cloud)[
                        'Contents']:
                resource_name = obj['Key']
                if resource_name.endswith('/') and only_files:
                    continue
                if with_hash:
                    md5 = Utils.get_md5_aws(self._aws_client, self._config.bucket, resource_name)
                    item = {'Hash': md5, 'Path': resource_name}
                else:
                    item = resource_name
                result.append(item)
        except:
            pass
        return result

    def _get_corrupt_files(self) -> List[str]:
        loca_files = Utils.get_objects_on_disk(self._config.full_path_to_backups, only_files=True)
        corrupt_files = []
        for file in loca_files:
            if not self._check_extension(file):
                corrupt_files.append(file)
        return corrupt_files

    def _check_extension(self, path: str):
        arr = os.path.splitext(path)
        extension = arr[len(arr) - 1]
        if extension != '' and extension not in self._get_valid_extensions():
            return False
        elif extension == '' and not path.endswith('backup_manifest'):
            try:
                int('0x' + os.path.basename(path), base=16)
            except ValueError:
                return False
        return True

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

    def _compute_files_to_upload(self, local_backups: [str]):
        if self._config.with_hash:
            return self._compute_files_to_upload_with_hash(local_backups)
        else:
            return self._compute_files_to_upload_no_hash(local_backups)

    def _compute_files_to_upload_no_hash(self, local_backups: [str]):
        path_cloud = self._config.path_to_backups_cloud
        root_local_path = self._config.full_path_to_backups
        result = {}
        for l_backup in local_backups:
            _dir = os.path.dirname(l_backup)
            dir_name = os.path.basename(_dir)
            file_name_for_cloud = file_name = os.path.basename(l_backup)
            if not root_local_path.endswith(_dir):
                file_name_for_cloud = f'{self._config.aws_correct_folder_name(dir_name)}/{file_name}'
            l_add = True
            for cloud_backup in self._cloud_backups:
                if cloud_backup.endswith(file_name):
                    l_add = False
                    break
            if l_add:
                full_path_cloud = f'{path_cloud}/{file_name_for_cloud}'
                result.update({l_backup: full_path_cloud})
        return result

    def _compute_files_to_upload_with_hash(self, local_backups: [str]):
        path_cloud = self._config.path_to_backups_cloud
        root_local_path = self._config.full_path_to_backups
        result = {}
        for l_backup in local_backups:
            path_to_dir = os.path.dirname(l_backup)
            dir_name = os.path.basename(path_to_dir)
            file_name_for_cloud = file_name = os.path.basename(l_backup)
            if not root_local_path.endswith(path_to_dir):
                file_name_for_cloud = f'{ConfigAWSClient.aws_correct_folder_name(dir_name)}/{file_name}'

            md5_local = Utils.get_md5(l_backup, self._config.chunk_size)
            l_add = True
            for cloud_backup in self._cloud_backups:
                if cloud_backup['Path'].endswith(file_name) and md5_local == cloud_backup['Hash']:
                    l_add = False
                    break
            if l_add:
                full_path_cloud = f'{path_cloud}/{file_name_for_cloud}'
                result.update({l_backup: full_path_cloud})
        return result

    def _clean_cloud(self):
        extra_bck = self._get_extra_bck_on_cloud()
        if len(extra_bck) > 0:
            objects = []
            for bck in extra_bck:
                objects.append({'Key': bck})
            self._aws_client.delete_objects(Bucket=self._config.bucket, Delete={'Objects': objects})

    def _get_extra_bck_on_cloud(self):
        if self._config.with_hash:
            return self._get_extra_bck_on_cloud_using_hash()
        else:
            return self._get_extra_bck_on_cloud_no_hash()

    def _get_extra_bck_on_cloud_no_hash(self):
        result = []
        loca_files = Utils.get_objects_on_disk(self._config.full_path_to_backups, only_files=True)
        for cloud_file in self._cloud_backups:
            cloud_file_name = os.path.basename(cloud_file)
            to_delete = True
            for local_file in loca_files:
                if local_file.endswith(cloud_file_name):
                    to_delete = False
                    break
            if to_delete:
                result.append(cloud_file)

        return result

    def _get_extra_bck_on_cloud_using_hash(self):
        result = []
        loca_files = Utils.get_objects_on_disk(self._config.full_path_to_backups, only_files=True)
        loca_files_and_hashes = Utils.add_hashs_to_local_files(loca_files, self._config.chunk_size)

        for cloud_file in self._cloud_backups:
            cloud_file_name = os.path.basename(cloud_file['Path'])
            to_delete = True
            for file, file_hash in loca_files_and_hashes.items():
                if file.endswith(cloud_file_name) and file_hash == cloud_file['Hash']:
                    to_delete = False
                    break
            if to_delete:
                result.append(cloud_file['Path'])
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

    def _empty_aws_cloud_dirs(self):
        obj_on_aws = self._get_objects_on_aws(only_files=False, with_hash=False)
        obj_on_aws = set(obj_on_aws)
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
