import logging
from typing import Optional
from urllib.parse import urlencode

import requests

logger = logging.getLogger(__name__)

WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"


class WebHDFSClient:
    def __init__(self, base_url: str = WEBHDFS_BASE_URL, user: str = WEBHDFS_USER):
        self.base_url = base_url.rstrip("/")
        self.user = user

    @staticmethod
    def _check_status(response: requests.Response, ok_codes: tuple[int, ...]) -> None:
        if response.status_code not in ok_codes:
            response.raise_for_status()

    def _url(self, path: str, op: str, **params) -> str:
        normalized_path = "/" + path.lstrip("/")
        all_params = {"op": op, "user.name": self.user, **params}
        return f"{self.base_url}{normalized_path}?{urlencode(all_params)}"

    def mkdirs(self, hdfs_path: str) -> bool:
        url = self._url(hdfs_path, "MKDIRS")
        response = requests.put(url, timeout=30)
        self._check_status(response, (200,))

        payload = response.json()
        created = payload.get("boolean")
        if created is not True:
            raise RuntimeError(f"MKDIRS failed for '{hdfs_path}': {payload}")
        return True

    def upload(self, hdfs_path: str, local_file_path: str) -> str:
        init_url = self._url(hdfs_path, "CREATE", overwrite="true")
        init_response = requests.put(init_url, allow_redirects=False, timeout=30)

        self._check_status(init_response, (307, 201))

        redirect_url: Optional[str] = init_response.headers.get("Location")
        if not redirect_url:
            if init_response.status_code == 201:
                return hdfs_path
            raise RuntimeError(f"No redirect URL returned by NameNode for '{hdfs_path}'.")

        with open(local_file_path, "rb") as file_data:
            upload_response = requests.put(redirect_url, data=file_data, timeout=120)
        self._check_status(upload_response, (201,))

        logger.info("Uploaded file '%s' to HDFS path '%s'.", local_file_path, hdfs_path)
        return hdfs_path

    def open(self, hdfs_path: str) -> bytes:
        url = self._url(hdfs_path, "OPEN")
        response = requests.get(url, allow_redirects=True, timeout=60)
        self._check_status(response, (200,))
        return response.content

    def exists(self, hdfs_path: str) -> bool:
        url = self._url(hdfs_path, "GETFILESTATUS")
        response = requests.get(url, allow_redirects=True, timeout=30)

        if response.status_code == 404:
            return False
        self._check_status(response, (200,))
        return True

    def list_status(self, hdfs_path: str) -> list:
        url = self._url(hdfs_path, "LISTSTATUS")
        response = requests.get(url, allow_redirects=True, timeout=30)
        self._check_status(response, (200,))

        payload = response.json()
        statuses = payload.get("FileStatuses", {}).get("FileStatus", [])
        if not isinstance(statuses, list):
            raise RuntimeError(f"Unexpected LISTSTATUS response for '{hdfs_path}': {payload}")
        return statuses
