class DebugBuilderStats:

    def __init__(self):
        self.count_total_compounds = 0
        self.count_total_complete_compounds = 0 # need to define complete first - some info is gone forever
        self.count_compounds_with_ms = 0
        self.count_total_ms_files = 0
        self.count_compounds_with_nmr = 0
        self.count_total_nmr_files = 0
        self.count_wiki = 0
        self.count_reactomepathways = 0

    def increment(self, which: str, inc: int = 1):
        current = getattr(self, f'count_{which}', 0)
        setattr(self, f'count_{which}', current + inc)


