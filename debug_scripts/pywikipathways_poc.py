from compound_common.config_classes import WikipathwaysConfig
import requests
import pywikipathways as pwpw
config = WikipathwaysConfig()
session = requests.Session()

def get_pathways_for_compound(compound_id: str):
    url = f'{config.pathways_by_x_ref}{compound_id.strip("MTBLC")}{config.xref_query_params}'
    wikipathways_response = session.get(url)
    wikipathways = wikipathways_response.json()['result']
    return wikipathways

def main():
    pathways = get_pathways_for_compound('10049')
    for pathway in pathways:
        result = pwpw.get_pathway_info(pathway['id'])
        print(result)
        outright_pathway = pwpw.get_pathway(pathway['id'])
        print(outright_pathway)




if __name__ == '__main__':
    main()