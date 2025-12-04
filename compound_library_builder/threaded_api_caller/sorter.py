class ExternalAPIResultSorter:
    def __init__(self, mementos):
        self.mementos = mementos

    def __getattribute__(self, name):
        """
        Instead of just returning the attribute as normal, we want to return a function object that we can call.
        :param name: Name of the function we want to call.
        :return: Lamdba function that calls the requested handling_function.
        """
        try:
            return super().__getattribute__(name)
        except AttributeError:
            handling_function = super().__getattribute__(name)
            return lambda x, y: handling_function(x, y)

    def sort(self, metabolights_dict: dict) -> dict:
        """
        Entry method for the ExternalApiResultsSorter. Iterates over each memento returned by the multithreaded
        `ataronchronon` process, and retrieves and calls the handling method for that memento. The handling method
        updates the metabolights_dict with the results of the memento (with some kind of formatted null value if no
        results are presented
        :param metabolights_dict: The in progress metabolights compound dict.
        :return: The metabolights_dict updated with all results from the `ataronchronon` multithreaded process.
        """
        for memento in self.mementos:
            metabolights_dict = self.__getattribute__(f'handle_{memento["name"]}')(
                memento, metabolights_dict
            )
        return metabolights_dict

    def handle_cactus(self, cactus_memento, metabolights_dict: dict) -> dict:
        """
        Check if the cactus_memento has any empty result signifiers, and if it don't, assign the results
        to the metabolights_dict's structure field.
        :param cactus_memento: Output from the cactus thread of the ataronchronon process.
        :param metabolights_dict: The in progress metabolights compound dict.
        :return: The metabolights_dict with the structure field updated.
        """
        if cactus_memento["results"] is None or cactus_memento["results"] == []:
            metabolights_dict["structure"] = "NA"
            print(f'Compound Error {metabolights_dict["id"]} Structure not assigned.')
            return metabolights_dict

        metabolights_dict["structure"] = cactus_memento["results"]
        return metabolights_dict

    def handle_citations(self, citations_memento, metabolights_dict: dict) -> dict:
        """
        Check if the citations_memento has any empty result signifiers, and if it don't, assign the results
        to the metabolights_dict's citations field. Also set the citations flag accordingly
        :param citations_memento: Output from the citations thread of the ataronchronon process.
        :param metabolights_dict: The in progress metabolights compound dict.
        :return: The metabolights_dict with the citations field and flag updated.
        """
        if citations_memento["results"] is None or citations_memento["results"] == []:
            metabolights_dict["citations"] = []
            metabolights_dict["flags"]["hasLiterature"] = "false"
            return metabolights_dict

        metabolights_dict["citations"] = citations_memento["results"]
        metabolights_dict["flags"]["hasLiterature"] = "true"
        return metabolights_dict

    def handle_spectra(self, spectra_memento, metabolights_dict: dict) -> dict:
        """
        Check if the spectra memento has any empty result signifiers, and if it don't, assign the results
        to the metabolights_dict's spectra['MS'] field. Also set the hasMS flag accordingly
        :param spectra_memento: Output from the spectra thread of the ataronchronon process.
        :param metabolights_dict: The in progress metabolights compound dict.
        :return: The metabolights_dict with the spectra['MS'] field and flag updated.
        """
        if spectra_memento["results"] is None or spectra_memento["results"] == []:
            print(f'No MoNa info available for {metabolights_dict["id"]}')
            metabolights_dict["flags"]["hasMS"] = "false"
            return metabolights_dict

        metabolights_dict["spectra"]["MS"] = spectra_memento["results"]
        metabolights_dict["flags"]["MS"] = "true"
        return metabolights_dict

    def handle_kegg_pathways(self, kegg_memento, metabolights_dict: dict) -> dict:
        """
        Check if the kegg memento has any empty result signifiers, and if it don't, assign the results
        to the metabolight_dict's pathways['KEGGPathways'] field.
        :param kegg_memento: Output from the kegg thread of the ataronchronon process.
        :param metabolights_dict: The in progress metabolights compound dict.
        :return: The metabolights_dict with the pathways['KEGGPathways'] field updated.
        """
        if kegg_memento["results"] is None or kegg_memento["results"] == {}:
            print(f'No KEGG info for {metabolights_dict["id"]}')
            return metabolights_dict
        metabolights_dict["pathways"]["KEGGPathways"] = kegg_memento["results"]
        return metabolights_dict

    def handle_wikipathways(self, wiki_memento, metabolights_dict: dict) -> dict:
        """
        Check if the wikipathways memento has any empty result signifiers, and if it don't, assign the results
        to the metabolights_dict's pathways['WikiPathways'] field.
        :param wiki_memento: Output from the wikipathways thread of the ataronchronon process.
        :param metabolights_dict: The in progress metabolights compound dict.
        :return: The metabolights_dict with the pathways['WikiPathways'] field updated.
        """
        if wiki_memento["results"] is None or wiki_memento["results"] == {}:
            print(f'No WikiPathways info for {metabolights_dict["id"]}')
            return metabolights_dict
        metabolights_dict["pathways"]["WikiPathways"] = wiki_memento["results"]
        return metabolights_dict

    def handle_reactions(self, reactions_memento, metabolights_dict: dict) -> dict:
        """
        Check if the reactions memento has any empty result signifiers, and if it don't, assign the results
        to the metabolights_dict's reactions field. Also set the hasReactions flag accordingly.
        :param reactions_memento: Output from the reactions thread of the ataronchronon process.
        :param metabolights_dict: The in progress metabolights compound dict.
        :return: The metabolights_dict with the reactions field and flag updated.
        """
        if reactions_memento["results"] is None or reactions_memento == []:
            print(
                f'No Rhea info for {metabolights_dict["id"]}. Reactions not assigned.'
            )
            metabolights_dict["flags"]["hasReactions"] = "false"
            return metabolights_dict

        metabolights_dict["reactions"] = reactions_memento["results"]
        metabolights_dict["flags"]["hasReactions"] = "true"
        return metabolights_dict

