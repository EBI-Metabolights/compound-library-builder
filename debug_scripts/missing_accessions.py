from reference_file_builders.mapping_file_builder.managers.mapping_persistence_manager import MappingPersistenceManager

class Mock:

    def __init__(self):
        self.species = {}
        self.chebi_species_via_mapping_file_map = {
            "Species": None,
            "SpeciesAccession": "study",
            "MAFEntry": "mafEntry",
            "Assay": "assay",
        }


def get_species_via_compound_mapping(mock, mapping: dict, id: str):
    """
    I may rewrite this, passing the giant object to this method feels a bit wrong. This could be a crunch point also
    as python will have to search the giant mapping object each time. Can't conceive a different way of getting the
    information out of that mapping file currently.
    Retrieves the species from the study mapping file, and populates a bunch of dicts, one per species, using info
    from the mapping file.

    :param mapping: Big study-compound-species mapping object.
    :param id: CHeBI compound ID
    :return: Self to enable chain calling.
    """

    if f"CHEBI:{id}" in mapping["compound_mapping"]:
        study_species = mapping["compound_mapping"][f"CHEBI:{id}"]
        for study_s in study_species:
            temp_study_species = str(study_s["species"]).lower()
            if temp_study_species not in mock.species:
                mock.species[temp_study_species] = []
            origin_dict = {
                key: study_s[
                    mock.chebi_species_via_mapping_file_map[key]
                ]
                if key is not "Species"
                else temp_study_species
                for key in mock.chebi_species_via_mapping_file_map.keys()
            }
            mock.species[temp_study_species].append(origin_dict)
    return mock


def main():
    mpm = MappingPersistenceManager(root="/Users/cmartin/Projects/compound-library-builder/ephemeral", timers_enabled=False)
    ml_mapping = mpm.msgpack.load("mapping")
    chebi15345 = ml_mapping['compound_mapping']['CHEBI:15345']
    print('hola')
    mock = Mock()
    result = get_species_via_compound_mapping(mock, ml_mapping, '15345')

if __name__ == '__main__':
    main()
