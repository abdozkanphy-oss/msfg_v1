from concurrent.futures import ThreadPoolExecutor
from utils.config_reader import ConfigReader
from thread.phase_3_correlation._3_2_correlations import execute_phase_three

cfg = ConfigReader()
threads = []

def _get_int(key: str, default: int = 0) -> int:
    try:
        return int(cfg[key])
    except Exception:
        return default

with ThreadPoolExecutor(max_workers=_get_int("thread_num", 4)) as executor:

    # Phase 3 (keep 1 thread if topic has 1 partition)
    phase3_n = _get_int("execute_phase_three_thread", 1)
    for _ in range(phase3_n):
        threads.append(executor.submit(execute_phase_three))

    # Retrain endpoint (import ONLY if enabled)
    retrain_n = _get_int("retrain_thread", 0)
    if retrain_n > 0:
        from thread.phase_3_correlation._3_2_handle_retrain import execute_retrain
        for _ in range(retrain_n):
            threads.append(executor.submit(execute_retrain))
