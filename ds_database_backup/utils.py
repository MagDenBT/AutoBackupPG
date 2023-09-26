import datetime
import hashlib
import os
from typing import List

import tzlocal


class DefaultTimezone(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=3)

    def dst(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "Default Timezone"


class Utils:

    @staticmethod
    def contain_files(path):
        files = Utils.get_objects_on_disk(path, only_files=True)
        return len(files) > 0

    @staticmethod
    def get_objects_on_disk(path, mask=None, or_second_mask=None, only_files=True) -> List[str]:
        objects_list = []
        for root, dirs, files in os.walk(path):
            total_files_in_dir = len(files)
            current_amount_in_dir = 0
            temp = []
            for filename in files:
                current_amount_in_dir += 1
                if mask is not None:
                    if mask in filename:
                        objects_list.append(os.path.join(root, filename))
                    elif or_second_mask is not None:
                        if current_amount_in_dir == total_files_in_dir:
                            if len(temp) > 0:
                                if or_second_mask in filename:
                                    objects_list.append(os.path.join(root, filename))
                                for val in temp:
                                    if or_second_mask in val:
                                        objects_list.append(os.path.join(root, val))
                            elif or_second_mask in filename:
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

        # noinspection PyBroadException
        try:
            md5sum = session_client.head_object(
                Bucket=bucket_name,
                Key=resource_name
            )['ETag'][1:-1]
        except:
            md5sum = None

        return md5sum

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
            result.update({file: Utils.get_md5(file, chunk_size)})
        return result

    @staticmethod
    def get_local_zone():
        try:
            localzone = tzlocal.get_localzone()
        except ValueError:
            localzone = DefaultTimezone()
        return localzone

    @staticmethod
    def calculate_expire_date(storage_time: int) -> datetime:
        return datetime.datetime.now(Utils.get_local_zone()) - datetime.timedelta(seconds=storage_time)

    @staticmethod
    def delete_local_empty_dirs(paths: List[str]) -> None:
        for path in paths:
            if os.path.exists(path):
                for root, dirs, files in os.walk(path):
                    for _dir in dirs:
                        dir_path = os.path.join(root, _dir)
                        Utils._delete_dir_if_empty(dir_path)

    @staticmethod
    def _delete_dir_if_empty(path: str) -> bool:
        delete_it = True
        for root, dirs, files in os.walk(path):
            for _dir in dirs:
                delete_it = Utils._delete_dir_if_empty(os.path.join(root, _dir))
            for _ in files:
                delete_it = False
                break
        if delete_it:
            os.rmdir(path)
        return delete_it
