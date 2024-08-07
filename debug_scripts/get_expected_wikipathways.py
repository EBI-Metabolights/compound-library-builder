import requests
from pydantic import BaseModel

from config_classes.builder_config_files import WikipathwaysConfig

compounds_endpoint = 'https://www.ebi.ac.uk:443/metabolights/ws/compounds/list'

config = WikipathwaysConfig()
session = requests.Session()
compounds_associated_with_pathways = []


def get_pathways_for_compound(compound_id: str):
    url = f'{config.pathways_by_x_ref}{compound_id.strip("MTBLC")}{config.xref_query_params}'
    wikipathways_response = session.get(url)
    wikipathways = wikipathways_response.json()['result']
    if len(wikipathways) > 0:
        print(f'Pathways found for {compound_id}')
        compounds_associated_with_pathways.append(compound_id)

def main():

    compounds = session.get(compounds_endpoint).json()['content']
    print(f'number of compounds: {len(compounds)}')
    for compound in compounds:
        get_pathways_for_compound(compound)
    with open("mtblcs_with_wikipathways.txt", "w") as file:
        file.write("\n".join(compounds_associated_with_pathways))
    print(f'number of compounds associated with pathways: {len(compounds_associated_with_pathways)}')

if __name__ == '__main__':
    main()


