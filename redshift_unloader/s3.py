import urllib.parse
from typing import List

import boto3
import boto3.resources
from redshift_unloader.logger import logger
from redshift_unloader.credential import Credential

MAX_DELETE_OBJECTS = 1000


class S3:
    __session: boto3.session.Session
    __s3: 'boto3.resources.factory.s3.ServiceResource'
    __bucket: 'boto3.resources.factory.s3.Bucket'
    __client: 'bott3.client'

    def __init__(self, credential: Credential, bucket: str, region: str) -> None:
        self.__session = boto3.session.Session(
            aws_access_key_id=credential.access_key_id,
            aws_secret_access_key=credential.secret_access_key,
            region_name=region
        )
        self.__s3 = self.__session.resource('s3')
        self.__bucket = self.__s3.Bucket(bucket)
        self.__client = boto3.client('s3')

    def __del__(self) -> None:
        pass

    def uri(self, path: str) -> str:
        return urllib.parse.urlunparse(['s3', self.__bucket.name, path, None, None, None])

    def list(self, path: str) -> List[str]:
        return [obj.key for obj in self.__bucket.objects.filter(Prefix=path)]

    def delete(self, keys: List[str]) -> None:
        logger.debug("Remove %s object(s) from S3", len(keys))
        for i in range(0, len(keys), MAX_DELETE_OBJECTS):
            sliced_keys = keys[i:i + MAX_DELETE_OBJECTS]
            self.__bucket.delete_objects(Delete={'Objects': [{'Key': key} for key in sliced_keys]})

    def download(self, key: str, filename: str) -> None:
        logger.debug("Download %s to %s", key, filename)
        self.__bucket.download_file(Key=key, Filename=filename)

    def download(self, key: str):
        s3 = boto3.client('s3')
        s3obj = s3.get_object(Bucket=self.__bucket.name, Key=key)
        return s3obj['Body'].read()
#        return self.__client.get_object(self.__bucket.name, key)['Body'].read()
