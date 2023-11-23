

class CommandLineUtils:

    @staticmethod
    def print_line_of_token(token: str = '_'):
        line_length = 101  # Length of the given line
        token_length = len(token)
        repeat_count = line_length // token_length  # Number of times the token needs to be repeated

        # Create and print the line
        line = token * repeat_count
        print(line)

    @staticmethod
    def readout(*args):
        CommandLineUtils.print_line_of_token('#')
        print("all config values and command line arguments:")
        for arg in args:
            if not isinstance(arg, dict):
                arg = dict(arg)
            for key, value in arg:
                print(f"{key}: {value}")
        CommandLineUtils.print_line_of_token('#')
