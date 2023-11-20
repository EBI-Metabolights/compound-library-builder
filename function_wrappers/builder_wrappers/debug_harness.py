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
                stats.increment("total_compounds")
                stats.increment("compounds_with_ms") if len(
                    result["spectra"]["MS"]
                ) > 0 else None
                stats.increment("total_ms_files", len(result["spectra"]["MS"]))
                stats.increment("compounds_with_nmr") if len(
                    result["spectra"]["NMR"]
                ) > 0 else None
                stats.increment("total_nmr_files", len(result["spectra"]["NMR"]))
                stats.increment("wiki") if len(
                    result["pathways"]["WikiPathways"]
                ) > 0 else None
                stats.increment("reactomepathways") if len(
                    result["pathways"]["ReactomePathways"]
                ) > 0 else None
                print(f"Total: {stats.count_total_compounds}")
                return result

        return wrapper

    return decorator
