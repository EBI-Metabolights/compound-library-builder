import sys

import yaml

from argparse_classes.parsers import ArgParsers
from mapping_file_builder.mapping_file_builder import MappingFileBuilderConfig, build


def main(args):

    parser = ArgParsers.mapping_file_builder_parser()
    args = parser.parse_args(args)
    with open(f"{args.config}", "r") as f:
        yaml_data = yaml.safe_load(f)
    config = MappingFileBuilderConfig(**yaml_data)
    build(config=config)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
