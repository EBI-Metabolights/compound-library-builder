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
        :return: instantiated ArgumentParser
        """
        cal_default_dest = "/Users/cmartin/projects/fake_compound_dir/"
        cal_ftp = "/Users/cmartin/Projects/compound-directory-builder/ephemeral"

        parser = argparse.ArgumentParser(
            description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
        )
        parser.add_argument(
            "-w",
            "--destination",
            action=ReadableDir,
            help="Output directory",
            default=cal_default_dest,
        )
        parser.add_argument(
            "-r", "--ref", action=ReadableDir, default=cal_ftp, help="Reference file directory"
        )
        parser.add_argument(
            "-n",
            "--new_compounds_only",
            action="store_true",
            help="whether to only run the compound_dir_builder for new chebi entries",
        )
        parser.add_argument(
            "-q",
            "--queue",
            action="store_true",
            help="Instruct compound_dir_builder to consume from queue",
        )
        parser.add_argument(
            "-qc",
            "--queue_config",
            default="/Users/cmartin/Projects/compound-directory-builder/.secrets/redis.yaml",
            help="location of redis.yaml file",
        )
        return parser

    @staticmethod
    def compound_queue_parser() -> argparse.ArgumentParser:
        """
        Compound queue parser. First argument is to take in the config file for the redis client, which is purely
        redis config like port number, host, db and password. Second argument is the compound queue config, which
        includes chunk size (number of MTBLC accessions in a 'chunk' or message on the queue) and new compounds only,
        which if set to true will filter out any known compounds before they get pushed to the queue.
        :return: instantiated ArgumentParser
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-rc",
            "--redis_config",
            help="Absolute path to redis config.yaml file",
            default="/Users/cmartin/Projects/compound-directory-builder/.secrets/redis.yaml",
        )
        parser.add_argument(
            "-qc",
            "--compound_queue_config",
            help="Absolute path to the config file for the compound queue",
            default="/Users/cmartin/Projects/compound-directory-builder/"
        )
        return parser

    @staticmethod
    def mapping_file_builder_parser() -> argparse.ArgumentParser:
        """
        Mapping file builder parser. Has a single argument, which is a path to a config file for the mapping file
        builder.
        :return: instantiated ArgumentParser
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-c",
            "--config",
            help="Absolute path to the mapping_file_builder.yaml file",
            default="/Users/cmartin/Projects/compound-directory-builder/.secrets/mapping_file_builder.yaml",
        )
        return parser

