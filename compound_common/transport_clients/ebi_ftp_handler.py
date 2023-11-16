import io
from typing import List
from retrying import retry
from configs.ftp_config import FTPConfig

import ftplib
import pandas as pd


class EBIFTPHandler:

    def __init__(self, config: FTPConfig):
        self.config = config
        self.ftp = ftplib.FTP(self.config.root)
        self.ftp.login(user=self.config.user, passwd=self.config.password)

    def get_assay_files(self, study: str) -> List[str]:
        """
        Get all assay filenames for a given study.
        :param study: Study accession IE MTBLS123
        :return: List of assay filenames as strings.
        """
        self.ftp.cwd(f'{self.config.study}{study}/')
        files = self.ftp.nlst()
        return [file for file in files if file.startswith('a_') and file.endswith('.txt')]


    def load_isa_file(self, isatab_file: str, study: str) -> pd.DataFrame:
        """

        :param assay_file:
        :return:
        """
        df = None
        # this shouldn't ever be out of step but just to be safe
        self.ftp.cwd(f'{self.config.study}{study}/')

        buffer = self.download_file(assay_file=isatab_file, buffer=io.BytesIO())
        # i am a beautiful genius
        try:
            df = pd.read_csv(buffer, sep='\t')
        except UnicodeDecodeError as e:
            print(f'{study} isatab file {isatab_file} not able to be decoded.')
        return df

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    def download_file(self, assay_file, buffer):
        """
        Download a given assay file from ftp. Uses @retry wrapper to retry download three times.
        :param assay_file: Given assay file.
        :param buffer: io.BytesIO buffer object to write to.
        :return: written buffer object (if successful)
        """
        self.ftp.retrbinary(f'RETR {assay_file}', buffer.write)
        buffer.seek(0)
        return buffer
