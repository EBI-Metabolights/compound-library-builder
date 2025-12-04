import sys

from compound_common.argparse_classes.parsers import ArgParsers
from compound_common.config_classes.reactome_builder_config import ReactomeFileBuilderConfig
from reference_file_builders.reactome_file_builder.reactome_file_builder import (
    ReactomeFileBuilder,
)
from utils.general_file_utils import GeneralFileUtils


def main(args):
    parser = ArgParsers.reactome_parser()
    args = parser.parse_args(args)
    reactome_config = ReactomeFileBuilderConfig(
        **GeneralFileUtils.open_yaml_file(args.reactome_config)
    )
    rfb = ReactomeFileBuilder(config=reactome_config)
    __ = rfb.build()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
