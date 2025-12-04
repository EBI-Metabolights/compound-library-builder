import argparse

from compound_common.argparse_classes.actions.readable_dir import ReadableDir


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

        parser = argparse.ArgumentParser(
            description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
        )
        parser.add_argument(
            "-w",
            "--destination",
            action=ReadableDir,
            help="Output directory",
        )
        parser.add_argument(
            "-r",
            "--ref",
            action=ReadableDir,
            help="Reference file directory",
        )
        parser.add_argument(
            "-n",
            "--new_compounds_only",
            action="store_true",
            help="whether to only run the compound_library_builder for new chebi entries",
        )
        parser.add_argument(
            "-q",
            "--queue",
            action="store_true",
            help="Instruct compound_library_builder to consume from queue",
        )
        parser.add_argument(
            "-rc",
            "--redis_config",
            help="location of redis.yaml file",
        )
        parser.add_argument(
            "-qc",
            "--compound_queue_config",
            help="Absolute path to the config file for the compound queue",
        )
        parser.add_argument("-db",
                            "--database",
                            action="store_true",
                            help="save compounds to database rather than to filesystem")
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
        )
        parser.add_argument(
            "-qc",
            "--compound_queue_config",
            help="Absolute path to the config file for the compound queue",
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
        )
        return parser

    @staticmethod
    def redis_config_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-rc",
            "--redis_config",
            help="Absolute path to redis config.yaml file",
        )
        return parser

    @staticmethod
    def reactome_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-c",
            "--reactome_config",
            help="Absolute path to reactome builder confing .yaml file",
        )
        return parser

    @staticmethod
    def accession_diff_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument("-t", "--token", help="MetaboLights API Token")
        return parser

    @staticmethod
    def mongo_to_elastic_parser():
        parser = argparse.ArgumentParser(
            description="Project Mongo compounds into Elasticsearch index"
        )
        parser.add_argument(
            "--mongo-uri",
            default="mongodb://localhost:27017",
            help="MongoDB connection string",
        )
        parser.add_argument(
            "--mongo-db",
            default="compound_library",
            help="Mongo database name",
        )
        parser.add_argument(
            "--mongo-coll",
            default="compounds",
            help="Mongo collection name",
        )
        parser.add_argument(
            "--es",
            required=True,
            help="Elasticsearch base URL, e.g. https://host/es",
        )
        parser.add_argument(
            "--index",
            default="compounds_search_v1",
            help="Elasticsearch index name",
        )
        parser.add_argument(
            "--api-key",
            help="Elasticsearch API key (preferred over basic auth)",
        )
        parser.add_argument(
            "--user",
            help="Elasticsearch username (if not using API key)",
        )
        parser.add_argument(
            "--password",
            help="Elasticsearch password (if not using API key)",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Number of documents per bulk request",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Delete and recreate index before reindexing",
        )
        return parser