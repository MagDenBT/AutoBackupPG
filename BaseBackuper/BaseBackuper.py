import boto3
import os
import subprocess
import datetime
import random
import hashlib


class Func:

    @staticmethod
    def get_objects_list_on_disk(path, mask=None, second_mask=None, only_files=True):
        objects_list = []
        for root, dirs, files in os.walk(path):
            total_files = len(files)
            i = 0
            temp = []
            for filename in files:
                i += 1
                if mask is not None:
                    if mask in filename:
                        objects_list.append(os.path.join(root, filename))
                        i = total_files + 1
                    else:
                        if second_mask is not None:
                            if i == total_files:
                                if len(temp) > 0:
                                    for val in temp:
                                        if second_mask in val:
                                            objects_list.append(os.path.join(root, val))
                                elif second_mask in filename:
                                    objects_list.append(os.path.join(root, filename))
                            else:
                                temp.append(filename)
                                continue
                        else:
                            continue
                else:
                    objects_list.append(os.path.join(root, filename))

            if not only_files:
                for _dir in dirs:
                    if mask is not None:
                        if mask not in _dir:
                            continue
                    objects_list.append(os.path.join(root, _dir))

        return objects_list

    @staticmethod
    def get_md5(file, chunk_size=None):
        if chunk_size is not None:
            md5s = []
            with open(file, 'rb') as fp:
                while True:
                    data = fp.read(chunk_size)
                    if not data:
                        break
                    md5s.append(hashlib.md5(data))

            if len(md5s) < 1:
                return '{}'.format(hashlib.md5().hexdigest())

            if len(md5s) == 1:
                return '{}'.format(md5s[0].hexdigest())

            digests = b''.join(m.digest() for m in md5s)
            digests_md5 = hashlib.md5(digests)
            return '{}-{}'.format(digests_md5.hexdigest(), len(md5s))

        hash_md5 = hashlib.md5()
        with open(file, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash_md5.update(chunk)
        _hash = hash_md5.hexdigest()
        return _hash

    @staticmethod
    def get_md5_aws(session_client, bucket_name, resource_name):

        try:
            md5sum = session_client.head_object(
                Bucket=bucket_name,
                Key=resource_name
            )['ETag'][1:-1]
        except:
            md5sum = None

        return md5sum

    @staticmethod
    def bucket_exists_and_accessible(s3_client, bucket_name):
        message = None
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            err_code = e.response.get('Error').get('Code')
            if err_code == '404':
                message = f'Bucket "{bucket_name}" does not exist'
            elif err_code == '403':
                message = f'Access to the bucket "{bucket_name}" is forbidden'
        return message

    @staticmethod
    def get_objects_on_aws(s3_client, bucket, verification_path, only_files=True, with_hash=True):
        result = []
        try:
            for obj in s3_client.list_objects(Bucket=bucket)['Contents']:
                resource_name = obj['Key']
                if resource_name.endswith('/') and only_files:
                    continue
                if resource_name.startswith(verification_path):
                    if with_hash:
                        md5 = Func.get_md5_aws(s3_client, bucket, resource_name)
                        item = {'Hash': md5, 'Path': resource_name}
                    else:
                        item = resource_name
                    result.append(item)
        except Exception:
            a = 1
        return result

    @staticmethod
    def optimize_remove_list_dir(empty_dirs):
        empty_dirs = set(empty_dirs)
        result = empty_dirs.copy()
        for i, sought in enumerate(empty_dirs):
            for z, target in enumerate(empty_dirs):
                if z == i:
                    continue
                if target.startswith(sought):
                    try:
                        result.remove(target)
                    except KeyError:
                        continue
        return result

    @staticmethod
    def add_hashs_to_local_files(local_files: [], chunk_size):
        result = {}
        for file in local_files:
            result.update({file: Func.get_md5(file, chunk_size)})
        return result


class Args(object):
    __custom_dir: str = None
    __path_to_onec_fb_backups: str = None
    __path_to_source_OneC: str = None
    __storage_time: int = None
    __path_to_mssql_backups: str = None
    __path_to_7zip: str = None

    __aws_access_key_id: str = None
    __aws_secret_access_key: str = None
    __aws_endpoint_url: str = None
    __aws_bucket: str = None
    __aws_chunk_size: int = None
    __with_hash: bool = None

    # Internal
    __label: str = None
    __log_path: str = None

    def __getitem__(self, item):
        return getattr(self, item)

    def __init__(self, create_backup=False, clean_backups=False, sync_backups=False, args=None):
        self.set_params(args, True)
        req_arg = self.__get_args_for_check(create_backup=create_backup, clean_backups=clean_backups,
                                            sync_backups=sync_backups)
        self.check_params(req_arg)

    def __get_args_for_check(self, create_backup=False, clean_backups=False, sync_backups=False):
        req_args = [ 'custom_dir',
                'path_to_onec_fb_backups',]
        if create_backup:
            req_args.extend([
                'path_to_source_OneC',
                'path_to_7zip',
            ])
        if clean_backups:
            req_args.extend([
                'storage_time',
            ])
        if sync_backups:
            req_args.extend([
                'aws_access_key_id',
                'aws_secret_access_key',
                'aws_bucket',
            ])
        return req_args

    def set_params(self, args=None, in_lower_case=True):
        if args is not None:
            for key, value in args.items():
                key_mod = str.lower(key) if in_lower_case else key
                try:
                    set_method = self[f'set_{key_mod}']
                    set_method(value)
                except:
                    continue

    def check_params(self, req_arg):
        args = {}

        for arg in req_arg:
            arg_mod = str.lower(arg)
            get_method = self[arg_mod]
            args.update({arg: get_method})
        _error = False
        message = 'The following arguments are not filled in -'
        for arg, get_method in args.items():
            val = get_method()
            if val is None or val == '':
                _error = True
                message += arg + ', '

        if _error:
            raise Exception(message)

    @staticmethod
    def _generate_label(use_millisec=False):
        if use_millisec:
            time_stamp = datetime.datetime.utcnow().strftime('%Y_%m_%d__%H-%M-%S.%f')[:-3]
        else:
            time_stamp = datetime.datetime.now().strftime('%Y_%m_%d__%H-%M-%S')

        return time_stamp + '_' + str(random.randint(1, 100))


    def with_hash(self):
        return self.__with_hash

    def set_with_hash(self, val: bool):
        self.__with_hash = val


    # Getters

    def aws_chunk_size(self):
        if self.__aws_chunk_size is not int or self.__aws_chunk_size < 1:
            return 8388608
        return self.__aws_chunk_size

    def aws_bucket(self):
        return self.__aws_bucket

    def aws_access_key_id(self):
        return self.__aws_access_key_id

    def aws_secret_access_key(self):
        return self.__aws_secret_access_key

    def aws_endpoint_url(self):
        if self.__aws_endpoint_url is None or self.__aws_endpoint_url == '':
            var = 'https://storage.yandexcloud.net'
            self.__aws_endpoint_url = var
        return self.__aws_endpoint_url

    def custom_dir(self, for_aws=False):
        if for_aws:
            return self.aws_correct_folder_name(self.__custom_dir)
        else:
            return self.__custom_dir

    def oneC_end_dir(self, for_aws=False):
        ldir = 'OneC_file_bases'
        if for_aws:
            return self.aws_correct_folder_name(ldir)
        else:
            return ldir

    def path_to_onec_fb_backups(self):
        if not self.__path_to_onec_fb_backups.endswith('\\'):
            self.__path_to_onec_fb_backups = self.__path_to_onec_fb_backups + '\\'
        return self.__path_to_onec_fb_backups

    def path_to_source_onec(self):
        return self.__path_to_source_OneC

    def log_path(self):
        if self.__log_path is None or self.__log_path == '':
            self.__log_path = './DSFileBackuperLogs'  # The path to the script logs
        return self.__log_path

    def storage_time(self):
        return self.__storage_time

    def full_path_onec_fb_backups_local(self):
        return f'{self.path_to_onec_fb_backups()}{self.custom_dir()}\\{self.oneC_end_dir()}'

    def path_to_mssql_backups(self):
        return self.__path_to_mssql_backups

    def full_path_onec_fb_backups_cloud(self):
        return f'{self.custom_dir(True)}/{self.oneC_end_dir(True)}'

    def full_path_to_mssql_backups_cloud(self):
        return f'{self.custom_dir(True)}/MSSQL'

    def path_to_cloud_custom_dir(self):
        return f'{self.custom_dir(True)}'

    def label(self):
        if self.__label is None or self.__label == '':
            self.__label = self._generate_label()
        return self.__label

    def path_to_7zip(self):
        return self.__path_to_7zip

    def sync_OneC_FB(self):
        return self.__path_to_onec_fb_backups is not None and self.__path_to_onec_fb_backups != ''

    def sync_MSSQL(self):
        return self.__path_to_mssql_backups is not None and self.__path_to_mssql_backups != ''

    def clean_on_AWS(self):
        return self.__aws_access_key_id is not None and self.__aws_access_key_id != '' and self.__aws_bucket is not None and self.__aws_bucket != '' and self.__aws_secret_access_key is not None and self.__aws_secret_access_key != ''

    # Setters

    def set_custom_dir(self, val: str):
        self.__custom_dir = str(val)

    def set_path_to_onec_fb_backups(self, val: str):
        self.__path_to_onec_fb_backups = str(val)

    def set_path_to_source_onec(self, val: str):
        self.__path_to_source_OneC = str(val)

    def set_path_to_mssql_backups(self, val: str):
        self.__path_to_mssql_backups = str(val)

    def set_log_path(self, val: str):
        self.__log_path = str(val)

    def set_storage_time(self, val: int):
        self.__storage_time = int(val)

    def set_path_to_7zip(self, val: str):
        self.__path_to_7zip = val

    def set_aws_bucket(self, val: str):
        self.__aws_bucket = str(val)

    def set_aws_chunk_size(self, val: int):
        self.__aws_chunk_size = int(val)

    def set_aws_access_key_id(self, val: str):
        self.__aws_access_key_id = str(val)

    def set_aws_secret_access_key(self, val: str):
        self.__aws_secret_access_key = str(val)

    def set_aws_endpoint_url(self, val: str):
        self.__aws_endpoint_url = str(val)

    def aws_correct_folder_name(self, _dir: str):
        valid_characters = '0123456789qwertyuiopasdfghjklzxcvbnmйцукенгшщзхъфывапролджэячсмитьбюё'
        if _dir[0].lower() not in valid_characters:
            _dir = 'A' + _dir
        return _dir


class OneC_FB_Backuper:
    args = None

    def __init__(self, args: Args):
        self.args = args

    def _create_backup(self):

        cd_file_name = f'1Cv8.1CD'
        source_file = f'{self.args.path_to_source_onec()}\\{cd_file_name}'
        if not os.path.exists(source_file):
            raise Exception(
                f'{source_file} - файл не найден. Проверьте правильность пути до каталога c базой 1с')

        if not os.path.exists(self.args.path_to_7zip()):
            raise Exception(
                f'{self.args.path_to_7zip()}\\7za.exe - архиватор 7zip не найден')

        target_file = f'{self.args.full_path_onec_fb_backups_local()}\\{self.args.label()}_{cd_file_name}.xz'

        comm_args = [f'{self.args.path_to_7zip()}\\7za.exe', 'a', target_file, '-ssw', source_file]

        process = subprocess.run(
            comm_args,
            stderr=subprocess.PIPE
        )

        text_error = process.stderr.decode(errors='replace')
        if not text_error == "":
            raise Exception(text_error)


class AWS_Connector:
    aws_client = None
    args: Args = None
    cloud_backups = []

    def __init__(self, args: Args):
        self.args = args
        session = boto3.session.Session()
        self.aws_client = session.client(
            service_name='s3',
            endpoint_url=self.args.aws_endpoint_url(),
            aws_access_key_id=self.args.aws_access_key_id(),
            aws_secret_access_key=self.args.aws_secret_access_key(),
        )

    def _sync_with_cloud(self):
        bucket = self.args.aws_bucket()
        message = Func.bucket_exists_and_accessible(self.aws_client, bucket)
        if message is not None:
            raise Exception(message)

        local_cloud_paths = {}
        if self.args.sync_OneC_FB():
            local_cloud_paths.update({self.args.full_path_onec_fb_backups_local(): self.args.full_path_onec_fb_backups_cloud()})
        if self.args.sync_MSSQL():
            local_cloud_paths.update(
                {self.args.path_to_mssql_backups(): self.args.full_path_to_mssql_backups_cloud()})

        with_hash = self.args.with_hash()
        all_cloud_backups = Func.get_objects_on_aws(self.aws_client, bucket, '', with_hash=with_hash)
        for local_path, cloud_path in local_cloud_paths.items():
            for cloud_backup in all_cloud_backups:
                if cloud_backup.startswith(cloud_path):
                    self.cloud_backups.append(cloud_backup)

        self._clean_cloud(local_cloud_paths, with_hash)
        self._upload_to_cloud(local_cloud_paths, with_hash)

    def _upload_to_cloud(self, local_cloud_paths: {}, with_hash):
        to_upload = {}
        for local_path, cloud_path in local_cloud_paths.items():
            local_backups = Func.get_objects_list_on_disk(local_path)
            result = self.__compute_files_to_upload(local_backups, local_path, cloud_path, with_hash)
            to_upload.update(result)

        if len(to_upload) == 0:
            return 'Нет новых файлов для выгрузки'

        upload_config = boto3.s3.transfer.TransferConfig(multipart_chunksize=self.args.aws_chunk_size())

        for backup_local, savefile in to_upload.items():
            self.aws_client.upload_file(backup_local, self.args.aws_bucket(), savefile, Config=upload_config)

        return ''

    def __compute_files_to_upload(self, local_backups: [], root_local_path, path_cloud, with_hash=False):
        if with_hash:
            return self.__compute_files_to_upload_with_hash(local_backups, root_local_path, path_cloud)
        else:
            return self.__compute_files_to_upload_no_hash(local_backups, root_local_path, path_cloud)


    def __compute_files_to_upload_no_hash(self, local_backups: [], root_local_path, path_cloud):
        result = {}
        for l_backup in local_backups:
            _dir = os.path.dirname(l_backup)
            dir_name = os.path.basename(_dir)
            file_name_for_cloud = file_name = os.path.basename(l_backup)
            if not root_local_path.endswith(_dir):
                file_name_for_cloud = f'{self.args.aws_correct_folder_name(dir_name)}/{file_name}'
            l_add = True
            for cloud_backup in self.cloud_backups:
                if cloud_backup.endswith(file_name):
                    l_add = False
                    break
            if l_add:
                full_path_cloud = f'{path_cloud}/{file_name_for_cloud}'
                result.update({l_backup: full_path_cloud})
        return result

    def __compute_files_to_upload_with_hash(self, local_backups: [], root_local_path, path_cloud):
        result = {}
        for l_backup in local_backups:
            path_to_dir = os.path.dirname(l_backup)
            dir_name = os.path.basename(path_to_dir)
            file_name_for_cloud = file_name = os.path.basename(l_backup)
            if not root_local_path.endswith(path_to_dir):
                file_name_for_cloud = f'{self.args.aws_correct_folder_name(dir_name)}/{file_name}'

            md5_local = Func.get_md5(l_backup, self.args.aws_chunk_size())
            l_add = True
            for cloud_backup in self.cloud_backups:
                if cloud_backup['Path'].endswith(file_name) and md5_local == cloud_backup['Hash']:
                    l_add = False
                    break
            if l_add:
                full_path_cloud = f'{path_cloud}/{file_name_for_cloud}'
                result.update({l_backup: full_path_cloud})
        return result

    def _clean_cloud(self, local_cloud_paths, with_hash):
        extra_bck = self.__get_extra_bck_on_cloud(local_cloud_paths, with_hash)
        if len(extra_bck) > 0:
            objects = []
            for bck in extra_bck:
                objects.append({'Key': bck})
            self.aws_client.delete_objects(Bucket=self.args.aws_bucket(), Delete={'Objects': objects})

    def __get_extra_bck_on_cloud(self, local_cloud_paths: {}, with_hash=False):
        if with_hash:
            return self.__get_extra_bck_on_cloud_with_hash(local_cloud_paths)
        else:
            return self.__get_extra_bck_on_cloud_no_hash(local_cloud_paths)

    def __get_extra_bck_on_cloud_no_hash(self, local_cloud_paths: {}):
        result = []
        loca_files = []
        for local_path, cloud_path in local_cloud_paths.items():
            loca_files.extend(Func.get_objects_list_on_disk(local_path, only_files=True))
        for cloud_file in self.cloud_backups:
            cloud_file_name = os.path.basename(cloud_file)
            to_delete = True
            for local_file in loca_files:
                if local_file.endswith(cloud_file_name):
                    to_delete = False
                    break
            if to_delete:
                result.append(cloud_file)

        return result

    def __get_extra_bck_on_cloud_with_hash(self, local_cloud_paths: {}):
        result = []
        loca_files = {}
        for local_path, cloud_path in local_cloud_paths.items():
            loca_files_pre = Func.get_objects_list_on_disk(local_path, only_files=True)
            loca_files.update(Func.add_hashs_to_local_files(loca_files_pre,self.args.aws_chunk_size()))
            for cloud_file in self.cloud_backups:
                cloud_file_name = os.path.basename(cloud_file['Path'])
                to_delete = True
                for local_file, local_file_hash in loca_files.items():
                    if local_file.endswith(cloud_file_name) and local_file_hash == cloud_file['Hash']:
                        to_delete = False
                        break
                if to_delete:
                    result.append(cloud_file['Path'])
        return result


class LocalCleaner:
    args: Args = None

    def __init__(self, args: Args):
        self.args = args

    def _clean_local(self):
        storage_time = self.args.storage_time()
        expire_date = datetime.datetime.now() - datetime.timedelta(seconds=storage_time)
        self.__clean_OneC_FB_backups(expire_date)

    def __clean_OneC_FB_backups(self, expire_date):
        backups = Func.get_objects_list_on_disk(self.args.full_path_onec_fb_backups_local(), only_files=True)
        dic_backups = {}
        for backup in backups:
            backup_date = datetime.datetime.fromtimestamp(os.path.getmtime(backup))
            dic_backups.update({backup: backup_date})

        dic_backups = dict(sorted(dic_backups.items(), key=lambda x: x[1]))

        items = list(dic_backups.items())
        i = 0
        while i < len(items):
            backup, backup_date = items[i]
            if backup_date <= expire_date:
                os.remove(backup)
                dic_backups.pop(backup)
            i += 1

        i = 0
        items = list(dic_backups.items())
        while i < len(items):
            backup, backup_date = items[i]
            if 0 < i < len(items) - 3:
                os.remove(backup)
            i += 1


class Manager:
    __connector = None
    __backupers = {}
    __cleaner = None
    __args: Args = None
    __aws_client = None

    def __init__(self, new_args=None, create_backup=False, clean_backups=False, sync_backups=False):

        try:
            self.__args = Args(args=new_args, create_backup=create_backup, clean_backups=clean_backups,
                               sync_backups=sync_backups)
        except Exception as e:
            self.write_log('backup-', False, str(e))
            raise e

        if create_backup:
            self.__backupers.update({'OneC_FB_Backuper': OneC_FB_Backuper(self.__args)})

        if clean_backups or sync_backups:
            self.__connector = AWS_Connector(self.__args)

        if clean_backups:
            self.__cleaner = LocalCleaner(self.__args)

    def clean_backups(self, write_to_log_file=True, raise_exception=False):
        try:
            self.__cleaner._clean_local()
            if self.__args.clean_on_AWS():
                self.__connector._clean_cloud()
                warning = ''
            else:
                warning = 'cleaning on AWS will not be performed, one or more of the parameters are missing - aws_access_key_id, aws_secret_access_key, aws_bucket'
            if write_to_log_file:
                self.write_log('cleaning-', True, warning)
        except Exception as e:
            if write_to_log_file:
                self.write_log('cleaning-', False, str(e))
            if raise_exception:
                raise e

    def create_backup(self, write_to_log_file=True, raise_exception=False):
        for name, backuper in self.__backupers.items():
            try:
                backuper._create_backup()
                if write_to_log_file:
                    self.write_log(f'{name}-', True, '')
            except Exception as e:
                if write_to_log_file:
                    self.write_log(f'{name}-', False, str(e))
                if raise_exception:
                    raise e

    def sync_with_cloud(self, write_to_log_file=True, raise_exception=False):
        try:
            message = self.__connector._sync_with_cloud()
            if write_to_log_file:
                self.write_log('syncr-', True, str(message))
        except Exception as e:
            if write_to_log_file:
                self.write_log('syncr-', False, str(e))
            if raise_exception:
                raise e

    def main(self):
        self.create_backup()
        self.sync_with_cloud()
        self.clean_backups()

    def write_log(self, file_pref, success, text=''):

        default_path = '../PostgreSQLBackuperLogs'
        try:
            path = self.__args.log_path()
            if path == '' or path is None:
                path = default_path
        except:
            path = default_path

        if not os.path.exists(path):
            os.makedirs(path)

        if self.__args is None:
            label = Args._generate_label(use_millisec=True)
        else:
            label = self.__args.label()
        result = "Success_" if success else 'FAIL_'
        path = f'{path}\\{file_pref}{result}{label}.txt'
        file = open(path, "w", encoding="utf-8")
        file.write(text)
        file.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    m = Manager()
    m.main()
