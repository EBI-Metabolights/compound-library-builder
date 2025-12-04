from accession_diff_analyzer.analyzer import Analyzer
from compound_common.argparse_classes import ArgParsers
import requests

from compound_common.transport_clients.ebi_ftp_handler import EBIFTPHandler
from compound_common.config_classes import FTPConfig


def main(args):
    parser = ArgParsers.accession_diff_parser()
    args = parser.parse_args(args)
    token = args.token

    Analyzer(
        session=requests.Session(),
        handler=EBIFTPHandler(
            config=FTPConfig(
                enabled=True,
                root="ftp.ebi.ac.uk",
                study="/pub/databases/metabolights/studies/public/",
                user="anonymous",
                password="",
            )
        ),
        token=token,
    ).go()
