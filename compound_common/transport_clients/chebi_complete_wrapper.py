import logging

import pandas
from rdkit.Chem import PandasTools


class ChebiCompleteClient:
    def __init__(self, complete_sdf_loc: str):
        self.sdf = self.load_sdf(complete_sdf_loc)
        if self.sdf is None:
            raise FileNotFoundError

    def load_sdf(self, loc) -> pandas.DataFrame:
        sdf = None
        try:
            sdf = PandasTools.LoadSDF(loc)
        except Exception as e:
            logging.exception(f"A real bad thing happened: {str(e)}")
        return sdf

    def select_compound(self):
        pass
