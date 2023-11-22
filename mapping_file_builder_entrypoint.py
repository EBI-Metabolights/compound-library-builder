import argparse
import sys

import yaml

from mapping_file_builder.mapping_file_builder import MappingFileBuilderConfig, build

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config",
    help="Absolute path to the mapping_file_builder.yaml file",
    default="/Users/cmartin/Projects/compound-directory-builder/.secrets/mapping_file_builder.yaml",
)
args = parser.parse_args(sys.argv[1:])
with open(f"{args.config}", "r") as f:
    yaml_data = yaml.safe_load(f)
config = MappingFileBuilderConfig(**yaml_data)
build()