from argparse import Namespace

class CommandLineUtils:
    """
    Collection of static command line related methods.
    """

    @staticmethod
    def print_line_of_token(token: str = "_"):
        """
        Print a line to the console with the given token, default is '_'
        :param token: Token of which the printed line will consist.
        :return: N/A prints line of tokens.
        """
        line_length = 101  # Length of the given line
        token_length = len(token)
        repeat_count = (
            line_length // token_length
        )  # Number of times the token needs to be repeated

        # Create and print the line
        line = token * repeat_count
        print(line)

    @staticmethod
    def readout(*args):
        """
        Print out the keys and values from all the arguments. This is a 'stupid' method, and assumes you have provided
        it with dicts or objects that can be turned into dicts. It will fail, as it should, if you don't.
        :param args: Iterable objects or dict objects.
        :return: N/A prints contents of args
        """

        CommandLineUtils.print_line_of_token("#")
        print("All config values and command line arguments:")
        for arg in args:
            if isinstance(arg, Namespace):
                arg = vars(arg)
            if not isinstance(arg, dict):
                arg = dict(arg)
            for key, value in arg.items():
                print(f"{key}: {value}")
        CommandLineUtils.print_line_of_token("#")
