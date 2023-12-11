from minio import Minio



class MinioCore(object):
    def __init__(self, host, access_key, secret_key, http=None, secure=False):
        self.host = host
        self.acess_key = access_key
        self.secret_key = secret_key
        self.http = http
        self.secure = secure

    def init_connection():
        return Minio(
            self.host,
            access_key=self.access_key,
            secret_key=self.secret_key,
            http_client=self.http,
            secure=self.secure
        )