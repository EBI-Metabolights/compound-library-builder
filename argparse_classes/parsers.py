import argparse

from argparse_classes.actions.readable_dir import ReadableDir


class ArgParsers:
    """
    Collection of argparsers
    """

    @staticmethod
    def compound_builder_parser() -> argparse.ArgumentParser:
        """
        Compound builder arg parser.
        :return: ArgumentParser object, initialised.
        """
        cal_default_dest = '/Users/cmartin/projects/fake_compound_dir/'
        cal_ftp = '/Users/cmartin/Projects/compound-directory-builder/ephemeral'

        parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('-w', '--destination', action=ReadableDir, help="Output directory",
                            default=cal_default_dest)
        parser.add_argument('-f', '--ftp', action=ReadableDir, default=cal_ftp, help="FTP directory")
        parser.add_argument('-n', '--new_compounds_only', action="store_true",
                            help="whether to only run the compound_dir_builder for new chebi entries")
        parser.add_argument('-q', '--queue', action="store_true",
                            help="Instruct compound_dir_builder to consume from queue")
        parser.add_argument('-qc', '--queue_config', default='/Users/cmartin/Projects/compound-directory-builder/.secrets/redis.yaml',
                            help="location of redis.yaml file")
        return parser
