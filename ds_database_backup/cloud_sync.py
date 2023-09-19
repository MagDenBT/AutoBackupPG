import os
import boto3
from boto3.s3.transfer import TransferConfig
from abc import ABC, abstractmethod

from AutoBackupPG.ds_database_backup.settings import SettingAWSClient
from AutoBackupPG.ds_database_backup.utils import Utils


class BaseClient(ABC):
    @abstractmethod
    def sync(self) -> None:
        pass


class AWS_Connector:


    def __init__(self, args: SettingAWSClient):
        self._settings = args
        session = boto3.session.Session()
        self._aws_client = session.client(
            service_name='s3',
            endpoint_url=self._settings.endpoint_url,
            aws_access_key_id=self._settings.access_key_id,
            aws_secret_access_key=self._settings.secret_access_key
        )
        self._cloud_backups = []

    def sync_with_cloud(self):

        if self._local_time_is_too_skewed(): raise Exception("Локальное время слишком сильно отличается от облачного")

        error_message = self._bucket_exists_and_accessible()
        if error_message is not None: raise Exception(error_message)

        local_cloud_paths = {}
        if Utils.contain_files(self._settings.full_path_to_backups):
            local_cloud_paths.update({self._settings.full_path_to_backups: self._settings.path_to_backups_cloud})

        with_hash = self._settings.with_hash
        all_cloud_backups = self._get_objects_on_aws(with_hash=with_hash)
        for local_path, cloud_path in local_cloud_paths.items():
            for cloud_backup in all_cloud_backups:
                if cloud_backup.startswith(cloud_path):
                    self._cloud_backups.append(cloud_backup)

        corrupt_files = self._get_corrupt_files(local_cloud_paths)
        if len(corrupt_files) == 0:
            self._clean_cloud(local_cloud_paths, with_hash)
        self._upload_to_cloud(local_cloud_paths, with_hash)
        if len(corrupt_files) > 0:
            raise Exception(
                f'Traces of a ransomware VIRUS may have been found. The following files have an unknown extension -{corrupt_files}')

    def _local_time_is_too_skewed(self):

        from botocore.exceptions import ClientError

        try:
            self._aws_client.list_objects(Bucket=self._settings.bucket)
        except ClientError as e:
            return e.response.get('Error').get('Code') == 'RequestTimeTooSkewed'
        except:
            return False

    def _bucket_exists_and_accessible(self):

        from botocore.exceptions import (
            ConnectionClosedError,
        )

        error_message = None
        try:
            self._aws_client.head_bucket(Bucket=self._settings.bucket)
        except ConnectionClosedError as e:
            error_message = e.fmt
        except Exception as e:
            # noinspection PyUnresolvedReferences
            err_code = e.response.get('Error').get('Code')
            if err_code == '404':
                error_message = f'Bucket "{self._settings.bucket}" does not exist'
            elif err_code == '403':
                error_message = f'Access to the bucket "{self._settings.bucket}" is forbidden'
        return error_message

    def _get_objects_on_aws(self, only_files=True, with_hash=True):
        result = []

        try:
            for obj in self._aws_client.list_objects_v2(Bucket=self._settings.bucket, Prefix=self._settings.path_to_backups_cloud)[
                'Contents']:
                resource_name = obj['Key']
                if resource_name.endswith('/') and only_files:
                    continue
                if with_hash:
                    md5 = Utils.get_md5_aws(self._aws_client, self._settings.bucket, resource_name)
                    item = {'Hash': md5, 'Path': resource_name}
                else:
                    item = resource_name
                result.append(item)
        except Exception:
            a = 1
        return result

    def _get_corrupt_files(self, local_cloud_paths: {}):
        loca_files = []
        for local_path, cloud_path in local_cloud_paths.items():
            loca_files.extend(Utils.get_objects_on_disk(local_path, only_files=True))
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

    def _get_valid_extensions(self):
        return ['.gz', '.xz', '.txz', '.backup', '.dump']

    def _upload_to_cloud(self, local_cloud_paths: {}, with_hash):
        to_upload = {}
        for local_path, cloud_path in local_cloud_paths.items():
            local_backups = Utils.get_objects_on_disk(local_path)
            result = self._compute_files_to_upload(local_backups, local_path, cloud_path, with_hash)
            to_upload.update(result)

        if len(to_upload) == 0:
            return 'Нет новых файлов для выгрузки'

        for backup_local, savefile in to_upload.items():
            self._upload_file(backup_local, savefile)

        return ''

    def _upload_file(self, local_file, target_file, adjust_bandwidth=True):

        upload_config = TransferConfig(multipart_chunksize=self._settings.chunk_size,
                                       max_bandwidth=self._settings.max_bandwidth_bytes)

        from botocore.exceptions import (
            ConnectionClosedError,
            ConnectTimeoutError,
            ReadTimeoutError
        )

        try:
            self._aws_client.upload_file(local_file, self._settings.bucket, target_file, Config=upload_config)
        except (
                ConnectionClosedError,
                ReadTimeoutError,
                ConnectTimeoutError,
        ) as e:
            new_max_bandwidth_bytes = self._get_bandwidth_limit() if self._settings.max_bandwidth_bytes is None else self._settings.max_bandwidth_bytes / 100 * 70

            if adjust_bandwidth:
                if self._threshold_bandwidth < new_max_bandwidth_bytes:
                    self._max_bandwidth_bytes = new_max_bandwidth_bytes
                    self._upload_file(local_file, target_file, True)
                else:
                    raise Exception(
                        f"Нестабильный интернет. Автопонижение скорости выгрузки до {self._max_bandwidth_bytes / 125} Кбит/с не решило проблему")

    def _compute_files_to_upload(self, local_backups: [], root_local_path, path_cloud, with_hash=False):
        if with_hash:
            return self._compute_files_to_upload_with_hash(local_backups, root_local_path, path_cloud)
        else:
            return self._compute_files_to_upload_no_hash(local_backups, root_local_path, path_cloud)



    def _compute_files_to_upload_no_hash(self, local_backups: [], root_local_path, path_cloud):
        result = {}
        for l_backup in local_backups:
            _dir = os.path.dirname(l_backup)
            dir_name = os.path.basename(_dir)
            file_name_for_cloud = file_name = os.path.basename(l_backup)
            if not root_local_path.endswith(_dir):
                file_name_for_cloud = f'{self._settings.aws_correct_folder_name(dir_name)}/{file_name}'
            l_add = True
            for cloud_backup in self._cloud_backups:
                if cloud_backup.endswith(file_name):
                    l_add = False
                    break
            if l_add:
                full_path_cloud = f'{path_cloud}/{file_name_for_cloud}'
                result.update({l_backup: full_path_cloud})
        return result

    def _compute_files_to_upload_with_hash(self, local_backups: [], root_local_path, path_cloud):
        result = {}
        for l_backup in local_backups:
            path_to_dir = os.path.dirname(l_backup)
            dir_name = os.path.basename(path_to_dir)
            file_name_for_cloud = file_name = os.path.basename(l_backup)
            if not root_local_path.endswith(path_to_dir):
                file_name_for_cloud = f'{SettingAWSClient.aws_correct_folder_name(dir_name)}/{file_name}'

            md5_local = Utils.get_md5(l_backup, self._settings.chunk_size)
            l_add = True
            for cloud_backup in self._cloud_backups:
                if cloud_backup['Path'].endswith(file_name) and md5_local == cloud_backup['Hash']:
                    l_add = False
                    break
            if l_add:
                full_path_cloud = f'{path_cloud}/{file_name_for_cloud}'
                result.update({l_backup: full_path_cloud})
        return result

    def _clean_cloud(self, local_cloud_paths, with_hash):
        extra_bck = self._get_extra_bck_on_cloud(local_cloud_paths, with_hash)
        if len(extra_bck) > 0:
            objects = []
            for bck in extra_bck:
                objects.append({'Key': bck})
            self._aws_client.delete_objects(Bucket=self._settings.bucket, Delete={'Objects': objects})

    def _get_extra_bck_on_cloud(self, local_cloud_paths: {}, with_hash=False):
        if with_hash:
            return self._get_extra_bck_on_cloud_with_hash(local_cloud_paths)
        else:
            return self._get_extra_bck_on_cloud_no_hash(local_cloud_paths)

    def _get_extra_bck_on_cloud_no_hash(self, local_cloud_paths: {}):
        result = []
        loca_files = []
        for local_path, cloud_path in local_cloud_paths.items():
            loca_files.extend(Utils.get_objects_on_disk(local_path, only_files=True))
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

    def _get_extra_bck_on_cloud_with_hash(self, local_cloud_paths: {}):
        result = []
        loca_files = {}
        for local_path, cloud_path in local_cloud_paths.items():
            loca_files_pre = Utils.get_objects_on_disk(local_path, only_files=True)
            loca_files.update(Utils.add_hashs_to_local_files(loca_files_pre, self._settings.chunk_size))
            for cloud_file in self._cloud_backups:
                cloud_file_name = os.path.basename(cloud_file['Path'])
                to_delete = True
                for local_file, local_file_hash in loca_files.items():
                    if local_file.endswith(cloud_file_name) and local_file_hash == cloud_file['Hash']:
                        to_delete = False
                        break
                if to_delete:
                    result.append(cloud_file['Path'])
        return result

    def _delete_empty_dirs_on_aws(self):
        empty_dirs = self._empty_aws_cloud_dirs()
        try:
            empty_dirs.remove(self._settings.path_to_backups_cloud(True) + '/')
        except KeyError:
            a = 1

        empty_dirs = Utils.optimize_remove_list_dir(empty_dirs)

        for_deletion = []
        for path in empty_dirs:
            for_deletion.append({'Key': path})

        self._aws_client.delete_objects(Bucket=self._settings.bucket, Delete={'Objects': for_deletion})

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

