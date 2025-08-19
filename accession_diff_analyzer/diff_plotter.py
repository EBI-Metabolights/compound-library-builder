import pickle
from typing import Dict

import requests

from accession_diff_analyzer.analyzer_dataclasses import IDRegistrySet
from accession_diff_analyzer.utils_analyzer import ReportedCompoundsStats, StudyBreakdown


def main():
    with open("../ephemeral/id_registry_set.pkl", "rb") as f:
        id_reg: IDRegistrySet = pickle.load(f)
    with open("../ephemeral/compound_statistics.pkl", "rb") as f:
        stats: ReportedCompoundsStats = pickle.load(f)
    dates = get_public_release_dates()
    print('loaded')

def get_public_release_dates() -> Dict[str, int]:
    release_date_dict = {}
    try:
        with open("dates.pkl", "r") as rf:
            release_date_dict: Dict[str, int] = pickle.load(rf)
    except OSError as e:
        print("dates file doesnt exist")


    s = requests.Session()
    response = s.get("https://www.ebi.ac.uk:443/metabolights/ws/studies")
    study_list = response.json()["content"]
    for study in study_list:
        study_details_response = s.get(f"https://www.ebi.ac.uk:443/metabolights/ws/studies/public/study/{study}")
        release_date = study_details_response.json()["content"]["studyPublicReleaseDate"]
        release_date_dict.update({f"{study}": release_date})
    with open("dates.pkl", "wb") as f:
        pickle.dump(release_date_dict, f)
    return release_date_dict


if __name__ == '__main__':
    main()
