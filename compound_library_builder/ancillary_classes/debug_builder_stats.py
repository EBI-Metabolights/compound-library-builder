class DebugBuilderStats:
    """
    Class to store statistics about a run of the CompoundBuilder. This class is initialised in shared_resources.py in
    global scope. Then using the @compound_debug_harness function wrapper (when enabled), the wrapper increments and
    updates various counters and statistics.
    """

    def __init__(self):
        self.count_total_compounds = 0
        self.count_total_complete_compounds = (
            0  # need to define complete first - some info is gone forever
        )
        self.count_compounds_with_ms = 0
        self.count_total_ms_files = 0
        self.count_compounds_with_nmr = 0
        self.count_total_nmr_files = 0
        self.count_wiki = 0
        self.count_reactomepathways = 0

    def increment(self, which: str, inc: int = 1):
        """
        Increment a given counter, 'which' dictating which one
        :param which: str representation of the counter to increment
        :param inc: int amount to increment by.
        :return: None
        """
        current = getattr(self, f"count_{which}", 0)
        setattr(self, f"count_{which}", current + inc)
