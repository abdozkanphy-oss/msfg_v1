# modules/offline_outonly_infer.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import joblib

from modules.model_registry import OutOnlyArtifact

def _auto_ffill_limit(obs_ts: pd.DatetimeIndex, resample_sec: int, n_lags: int, cap: int = 600) -> int:
    if len(obs_ts) < 2:
        return min(cap, n_lags + 1)
    gaps = obs_ts.to_series().diff().dt.total_seconds().dropna()
    if len(gaps) == 0:
        return min(cap, n_lags + 1)
    med = float(gaps.median())
    buckets = max(1, int(round(med / float(resample_sec))))
    return min(cap, max(n_lags + 1, buckets))

def _auto_max_gap_sec(obs_ts: pd.DatetimeIndex) -> int:
    if len(obs_ts) < 2:
        return 6 * 3600
    gaps = obs_ts.to_series().diff().dt.total_seconds().dropna()
    if len(gaps) == 0:
        return 6 * 3600
    med = float(gaps.median())
    return int(min(24 * 3600, max(3600, 6 * med)))

def predict_outonly_from_seed(
    artifact: OutOnlyArtifact,
    target_var: str,
    seed_history,
    now_ts: datetime,
    current_value: Optional[float] = None,
) -> Optional[float]:
    """
    Build lag features from (ts,row[target]) history + optional current value,
    then predict using the offline OUT_ONLY univariate model.
    """
    # Determine n_lags from feature names, fallback to 10
    n_lags = 10
    if artifact.feature_names:
        # expect ["lag_1", ...]
        n_lags = len(artifact.feature_names)

    # build series from seed_history
    ts_list = []
    val_list = []

    if seed_history:
        for ts, row in seed_history:
            if not isinstance(row, dict):
                continue
            if target_var not in row:
                continue
            v = row.get(target_var)
            try:
                fv = float(v)
            except Exception:
                continue
            if not np.isfinite(fv):
                continue
            try:
                t = pd.to_datetime(ts)
            except Exception:
                continue
            ts_list.append(t)
            val_list.append(fv)

    # include current point (optional)
    if current_value is not None:
        try:
            fv = float(current_value)
            if np.isfinite(fv):
                ts_list.append(pd.to_datetime(now_ts))
                val_list.append(fv)
        except Exception:
            pass

    if len(ts_list) < (n_lags + 2):
        return None

    s = pd.Series(val_list, index=pd.DatetimeIndex(ts_list)).sort_index()
    s = s[~s.index.duplicated(keep="last")]

    # resample at 60s because model was trained using resample_sec (stored implicitly in your config);
    # we assume your prediction_horizon_sec aligns with resample_sec for OUT_ONLY baseline.
    # If you later store resample_sec in artifact meta, we can use it directly.
    # For now: infer from horizon_sec (common case: 60s)
    resample_sec = int(getattr(artifact, "resample_sec", 60) or 60)
    try:
        # if horizon is 300 etc, you'd want to match your training resample; this is adjustable later
        resample_sec = 60
    except Exception:
        resample_sec = 60

    freq = f"{int(resample_sec)}s"
    sr = s.resample(freq).last()

    # bounded fill + gap guard
    ffill_limit = _auto_ffill_limit(s.index, resample_sec=resample_sec, n_lags=n_lags)
    max_gap_sec = _auto_max_gap_sec(s.index)

    # gap guard
    idx = pd.Series(sr.index, index=sr.index)
    last_obs = idx.where(sr.notna()).ffill()
    age_sec = (idx - last_obs).dt.total_seconds()
    sr = sr.where(age_sec <= float(max_gap_sec))

    sr = sr.ffill(limit=int(ffill_limit))

    # build lag vector for the *latest* timestamp
    # lag_1 = value one bucket back, ..., lag_n = n buckets back
    lags = []
    for k in range(1, n_lags + 1):
        v = sr.shift(k).iloc[-1]
        if v is None or not np.isfinite(v):
            return None
        lags.append(float(v))

    X = np.array(lags, dtype="float32").reshape(1, -1)

    try:
        model = joblib.load(artifact.model_path)
        y_hat = model.predict(X)
        return float(y_hat[0])
    except Exception:
        return None
