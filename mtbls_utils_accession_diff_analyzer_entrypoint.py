import argparse
import sys

from accession_diff_analyzer.utils_analyzer import UtilsAnalyzer


def main(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--token", help="MetaboLights API Token")
    parser.add_argument("-s", "--study-root", help="Absolute path to study root")
    p_args = parser.parse_args(args)
    token = p_args.token
    study_root = p_args.study_root
    UtilsAnalyzer(
        token=token,
        study_root_path=study_root
    ).go()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
