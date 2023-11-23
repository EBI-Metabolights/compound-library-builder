from dataclasses import dataclass
from typing import List


@dataclass
class RefMapping:
    study_mapping: dict
    compound_mapping: dict
    species_list: List[str]
