
import math

from utils.general_file_utils import GeneralFileUtils


class SpectraFileHandler:
    @staticmethod
    def save_spectra(spectra_id, spectra_data, mtbls_id, destination) -> None:
        """
        Parse a given spectral data file into a dict, and then save it as a .json file using
        `FileHandler.save_json_file`
        :param spectra_id: Unique identifier of this spectral data file.
        :param spectra_data: spectral data file to process.
        :param mtbls_id: the MTBLC accession associated with this spectral data file.
        :param destination: The MTBLC directory to save the .json file to.
        :return: None
        """
        final_destination = f"{destination}/{mtbls_id}/{mtbls_id}_spectrum/{spectra_id}/{spectra_id}.json"
        datapoints = spectra_data.split(" ")
        ml_spectrum = {"spectrumId": spectra_id, "peaks": []}
        mz_array = []

        float_round_lambda = lambda num, places, direction: direction(
            num * (10**places)
        ) / float(10**places)
        for datapoint in datapoints:
            temp_array = datapoint.split(":")
            temp_peak = {
                "intensity": float_round_lambda(
                    float(temp_array[1].strip()) * 9.99, 6, math.floor
                ),
                "mz": float(temp_array[0].strip()),
            }
            ml_spectrum["peaks"].append(temp_peak)
            mz_array.append(float(temp_peak["mz"]))
        ml_spectrum.update({"mzStart": min(mz_array), "mzStop": max(mz_array)})
        GeneralFileUtils.save_json_file(final_destination, ml_spectrum)
