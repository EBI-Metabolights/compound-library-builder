from copy import deepcopy
from typing import Any, Dict, List


class MongoUtils:
    """
    Utility helpers for coercing compound documents into types
    expected by the MongoDB JSON schema.
    """

    # Class-level configuration: easier to reuse / test / extend.
    FLAG_KEYS: List[str] = [
        "hasLiterature",
        "hasReactions",
        "hasSpecies",
        "hasPathways",
        "hasNMR",
        "hasMS",
        "hasMolfile",
        "hasSmiles",
        "hasInchi",
        "hasSynonyms",
        "hasIupac",
        "hasCitations",
        "hasReactionsList",
        "hasSpeciesHits",
        "hasKegg",
        "hasReactome",
        "hasWikiPathways",
        "hasSpectraListed",
        "hasExactMass",
        "hasAverageMass",
        "hasCharge",
    ]

    COUNT_KEYS: List[str] = [
        "synonyms",
        "iupac",
        "citations",
        "reactions",
        "species_hits",
        "species_total_assays",
        "kegg",
        "reactome",
        "wikipathways",
        "spectra",
    ]

    @staticmethod
    def _coerce_float(value: Any) -> Any:
        """
        Coerce to float where possible; otherwise return original value.
        """
        if value is None:
            return value
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and value.strip():
            try:
                return float(value)
            except ValueError:
                return value
        return value

    @staticmethod
    def _coerce_int(value: Any) -> Any:
        """
        Coerce to int where possible; otherwise return original value.
        """
        if value is None:
            return value
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str) and value.strip():
            try:
                # handle "1", "1.0", etc.
                if "." in value:
                    return int(float(value))
                return int(value)
            except ValueError:
                return value
        return value

    @staticmethod
    def _coerce_bool(value: Any) -> Any:
        """
        Coerce common string representations to bool; otherwise return original value.
        """
        if isinstance(value, bool) or value is None:
            return value
        if isinstance(value, str):
            v = value.strip().lower()
            if v in {"true", "1", "yes", "y", "t"}:
                return True
            if v in {"false", "0", "no", "n", "f"}:
                return False
        return value

    @staticmethod
    def normalize_compound_for_mongo(doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Take a compound dict from the existing pipeline and coerce fields into the
        types expected by the Mongo JSON schema. Returns a *new* dict.
        """
        d = deepcopy(doc)

        # Top-level numeric fields
        if "averagemass" in d:
            d["averagemass"] = MongoUtils._coerce_float(d["averagemass"])
        if "exactmass" in d:
            d["exactmass"] = MongoUtils._coerce_float(d["exactmass"])
        if "charge" in d:
            d["charge"] = MongoUtils._coerce_int(d["charge"])

        # Flags -> bools
        flags = d.get("flags")
        if isinstance(flags, dict):
            for key in MongoUtils.FLAG_KEYS:
                if key in flags:
                    flags[key] = MongoUtils._coerce_bool(flags[key])

        # Counts -> ints
        counts = d.get("counts")
        if isinstance(counts, dict):
            for key in MongoUtils.COUNT_KEYS:
                if key in counts:
                    counts[key] = MongoUtils._coerce_int(counts[key])

        # species_hits[].assay_sum -> int
        species_hits = d.get("species_hits")
        if isinstance(species_hits, list):
            for sh in species_hits:
                if isinstance(sh, dict) and "assay_sum" in sh:
                    sh["assay_sum"] = MongoUtils._coerce_int(sh["assay_sum"])

        # spectra_count (if present) -> int
        if "spectra_count" in d:
            d["spectra_count"] = MongoUtils._coerce_int(d["spectra_count"])

        return d