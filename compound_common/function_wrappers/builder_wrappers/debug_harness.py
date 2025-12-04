from functools import wraps

from shared_resources import stats


def compound_debug_harness(enabled=False):
    """
    Function wrapper to capture the results of builder_compound_dir.build, and update various debug statistics, and then
    return the results.
    :param enabled: Whether the debug mode is actually enabled. If not, just return the function execution.
    :return: decorator
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not enabled:
                return func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
                try:
                    stats.increment("total_compounds")
                    if "spectra" in result:
                        stats.increment("compounds_with_ms") if len(
                            result["spectra"].get("MS", [])
                        ) > 0 else None
                        stats.increment("total_ms_files", len(result["spectra"].get("MS", [])))
                        stats.increment("compounds_with_nmr") if len(
                            result["spectra"].get("NMR", [])
                        ) > 0 else None
                        stats.increment("total_nmr_files", len(result["spectra"].get("NMR", [])))
                    if "pathways" in result:
                        stats.increment("wiki") if len(
                            result["pathways"].get("WikiPathways", [])
                        ) > 0 else None
                        stats.increment("reactomepathways") if len(
                            result["pathways"].get("ReactomePathways", [])
                        ) > 0 else None
                except KeyError as e:
                    print(f"KeyError encountered: {e}")
                print(f"Total: {stats.count_total_compounds}")
                return result

        return wrapper

    return decorator