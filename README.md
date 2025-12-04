
# 🧬 MetaboLights Compound Library Builder

### *Written by Callum Martin, March 2023*

This repository performs multiple stages of building up an **MTBLC compound directory**.
When run for all MTBLC IDs, the final output constitutes the **MetaboLights compound reference layer**.

If I am no longer at the EBI and you have questions, feel free to contact me at:
📧 **[callumceltic@gmail.com](mailto:callumceltic@gmail.com)**
I will do my best to help.

---

## 📌 Usage
This script and others in this repository require a venv to be maintained.
```bash
python3 compound_library_builder_entrypoint.py --ftp <path_to_reference_files> --destination <path_to_output_dir>
```

### 🕒 Cron Usage

```bash
bsub -u metabolights-dev \
     -J "newcompounddirbuild" \
     -q standard \
     -R "rusage[mem=128000]" \
     -M 128000 \
     /path_to_script.sh
```

---

## ⚙️ Parameters

### `--ftp: str`

Path to the reference files:

* **mapping.json**
  Maps *studies → compounds → species*
  (which studies are associated with which compounds and which species those compounds originate from).

* **reactome.json**
  Cache of Reactome pathway data per ChEBI compound.
  Learn more: [https://reactome.org/](https://reactome.org/)

### `--destination: str`

Directory where the processed compound directories and Mass Spec `.json` files will be saved.

---

# 🏗️ Stages of the Build Process

---

## **Stage 1: Initialisation**

This step prepares several key objects needed for the process:

* **CompoundBuilderConfig**
  Includes internal/external API endpoints, keys, and mappings used to translate API responses.

* **Shared HTTP session**
  Used for faster repeated API calls.

* **Mapping dict**
  Loaded from `mapping.json`.

* **Reactome dict**
  Loaded from `reactome.json`.

* **List of MTBLC IDs**
  Retrieved from our legacy Java webservice.

* **Initial empty compound dict**
  Used as the working structure through the build.

---

## **Stage 2: Get ChEBI Data**

The script now uses **ChEBI API v2 JSON output**.
ChEBI v2 supports **bulk retrieval**, so we fetch batches of compounds and feed them to the build pipeline.

We **cherry pick** relevant fields into two dicts:

* **basic** (straight mappings: definition, inchi, mass etc.)
* **advanced** (synonyms, species, formulae, citations, etc.)

These are combined via:

```python
{**basic, **advanced}
```

---

## **Stage 3: Merge ChEBI Dict With MetaboLights Dict**

This stage merges the ChEBI-derived dict with our own compound dict.

This happens via a (slightly wicked) **dict comprehension**, needed because the keys differ in the two dicts.

> Future improvement: restructure the ChEBI-population logic so the dicts could merge directly.

---

## **Stage 4: Multithreaded External API Calls**

Major improvement over version 1.
Each external API is queried **in parallel** using:

* `ThreadPoolExecutor`
* A wrapper function per API
* Runtime flags enabling/disabling API threads

If a flag is `False`, the thread returns an empty dict instead of making a request.

### External APIs hit:

* **KEGG** — pathway info
* **Wikipathways** — pathway info
* **EuropePMC** — citation info
* **CACTUS** — structure info
* **Rhea** — reaction info
* **MoNa** — spectra data *(heavy I/O — slowest step)*

### Adding a new external API

1. Implement the function in `ExternalAPIHitter`
2. Create a wrapper function
3. Configure inputs in `ataronchronon`
4. Add wrapper to the list of threaded functions
5. Add endpoint to config

Each thread returns:

```python
{"name": "<api_name>", "results": {...}}
```

---

## **Stage 5: Sorting Results from the Multithreaded Process**

The `ExternalAPIResultSorter`:

* Iterates over each thread result
* Calls a handler for that API (e.g. handling KEGG, MoNa, etc.)
* Integrates API results into the compound dict
* Sets *data presence flags* used later by the UI

Empty results are logged; missing data fields are populated with defaults.

---

## **Stage 6: Final Processing and Output**

Remaining tasks not handled earlier:

### ➤ Reactome pathways

Loaded using `get_reactome_data()` by slicing the Reactome dict for the compound ID.

### ➤ NMR data from the Java legacy webservice

Used only for populating the `NMR` section.

### ➤ Set the final UI flags

These flags indicate whether the UI should attempt rendering certain sections:

* `hasPathways`
* `hasNMR`
* `hasSpecies`

### ➤ Write output

Each compound is saved as:

```
<destination>/<CHEBI>/compound.json
```

The dict is returned for debugging but ignored by StartCompoundBuilder.

---

# 📎 Additional Notes

### 🧵 About Threading

You can force single-threaded mode by changing:

```python
ThreadPoolExecutor(max_workers=x)
```

to:

```python
max_workers=1
```

### ⚠️ Slowest Step

`get_ms_from_mona` performs file I/O and significantly slows down threading.
Future work: move it out of the threaded stage.

### 🛠️ Exception Decorators

Custom decorators handle common exceptions while keeping logs readable.
Useful for high-volume data processing, acceptable trade-off.

### 🧪 Testing

A useful test would build ~50 compound directories locally to verify content.
Not implemented due to time constraints.

### 💻 Virtual Environment

The venv is **manually maintained**.
Upon adding dependencies:

1. Update `requirements.txt`
2. Upload to Codon
3. Install manually there

---

# 📤 Output

The script writes:

* Final compound directories
* Intermediate mass spec `.json` files

all into the specified `--destination` directory.

---
