# modules/offline_outonly_trainer.py
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta
from math import ceil
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

from utils.config_reader import ConfigReader

# NEW: training-friendly table keyed by (plId,wcId,wsId)+time (no ALLOW FILTERING)
from cassandra_utils.models.dw_raw_by_ws import dw_tbl_raw_data_by_ws, ensure_dw_raw_by_ws

try:
    from tqdm import tqdm
except Exception:
    tqdm = None  # optional


_SAFE_TOKEN_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def safe_token(x: str, default: str = "UNKNOWN") -> str:
    if x is None:
        return default
    s = str(x).strip()
    if not s or s.lower() == "none":
        return default
    return _SAFE_TOKEN_RE.sub("_", s)


def _sf(x: float) -> float:
    try:
        v = float(x)
        if not np.isfinite(v):
            return float("nan")
        return v
    except Exception:
        return float("nan")


def fetch_output_rows_by_ws(
    pl_id: int,
    wc_id: int,
    ws_id: int,
    start_dt: datetime,
    end_dt: datetime,
    limit: int,
    st_no: Optional[str] = None,
    op_tc: Optional[str] = None,
    chunk_hours: int = 6,
    max_retries: int = 3,
    sleep_base_sec: float = 0.75,
    show_progress: bool = True,
) -> List[Any]:
    """
    Chunked fetch from dw_tbl_raw_data_by_ws.
    This table is keyed by (plant_id, work_center_id, work_station_id) + measurement_date,
    so reads are partition-bound and do NOT require ALLOW FILTERING.

    Note: st_no/op_tc filters are applied client-side after fetch to avoid secondary indexing.
    """
    import time

    rows: List[Any] = []
    remaining = int(limit)

    total_hours = max(0.0, (end_dt - start_dt).total_seconds() / 3600.0)
    n_chunks = max(1, int(ceil(total_hours / float(chunk_hours))))

    iterator = range(n_chunks)
    if show_progress and tqdm is not None:
        iterator = tqdm(iterator, desc="Fetch chunks", unit="chunk")

    t0 = start_dt
    for _ in iterator:
        if t0 >= end_dt or remaining <= 0:
            break
        t1 = min(end_dt, t0 + timedelta(hours=int(chunk_hours)))

        for attempt in range(int(max_retries)):
            try:
                q = dw_tbl_raw_data_by_ws.objects.filter(
                    plant_id=int(pl_id),
                    work_center_id=int(wc_id),
                    work_station_id=int(ws_id),
                )
                q = q.filter(measurement_date__gte=t0, measurement_date__lt=t1)

                per_chunk_cap = min(remaining, 50_000)
                q = q.limit(int(per_chunk_cap))

                part: List[Any] = []
                for r in q:
                    # client-side filters (optional)
                    if st_no and (getattr(r, "produced_stock_no", None) or None) != str(st_no):
                        continue
                    if op_tc and (getattr(r, "operationtaskcode", None) or None) != str(op_tc):
                        continue
                    # output-only: require counter_reading
                    y = getattr(r, "counter_reading", None)
                    if y is None:
                        continue

                    part.append(r)
                    if len(part) >= remaining:
                        break

                rows.extend(part)
                remaining = int(limit) - len(rows)
                break

            except Exception:
                if attempt >= int(max_retries) - 1:
                    raise
                time.sleep(float(sleep_base_sec) * (2 ** attempt))

        t0 = t1

    return rows


def rows_to_wide_df(rows: List[Any]) -> pd.DataFrame:
    """
    Convert rows into a wide time-indexed frame: index=ts, columns=equipment_name, values=counter_reading.
    """
    recs = []
    for r in rows:
        ts = getattr(r, "measurement_date", None)
        if ts is None:
            continue
        eq = getattr(r, "equipment_name", None) or "UNKNOWN_EQ"
        y = getattr(r, "counter_reading", None)
        if y is None:
            continue
        try:
            fy = float(y)
        except Exception:
            continue
        if not np.isfinite(fy):
            continue

        recs.append({"ts": ts, "eq": str(eq), "y": fy})

    if not recs:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(recs)
    df["ts"] = pd.to_datetime(df["ts"], utc=False)
    wide = df.pivot_table(index="ts", columns="eq", values="y", aggfunc="last").sort_index()
    return wide


def choose_targets_from_wide(
    wide: pd.DataFrame,
    resample_sec: int,
    top_k: int = 2,
    min_cov: float = 0.10,
) -> List[str]:
    """
    Select top-k columns by coverage after resampling.
    """
    if wide is None or wide.empty:
        return []

    freq = f"{int(resample_sec)}s"
    wr = wide.resample(freq).last()

    stats = []
    for col in wr.columns:
        cov = float(wr[col].notna().mean())
        cnt = int(wr[col].notna().sum())
        if cov >= float(min_cov):
            stats.append((cov, cnt, str(col)))

    stats.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [c for _, _, c in stats[: int(top_k)]]


def build_lagged_dataset_sparse_friendly(
    series: pd.Series,
    resample_sec: int,
    horizon_sec: int,
    n_lags: int,
    ffill_limit: int = 0,          # 0 => auto
    max_gap_sec: int = 0,          # 0 => auto
    max_ffill_cap: int = 600,      # cap fill to avoid huge bridging
    min_non_nan_lags: int = 0,     # 0 => require all lags; else allow partial then fill
) -> Tuple[pd.DataFrame, pd.Series, int, Dict[str, Any]]:
    """
    Sparse-friendly lag dataset builder:
    - auto ffill_limit based on median observed gap
    - gap guard prevents bridging across very large gaps
    - optional relaxed lag completeness for extremely sparse series
    """
    info: Dict[str, Any] = {
        "raw_points": int(series.notna().sum()),
        "resample_sec": int(resample_sec),
        "horizon_sec": int(horizon_sec),
        "n_lags": int(n_lags),
        "ffill_limit": int(ffill_limit),
        "max_gap_sec": int(max_gap_sec),
    }

    freq = f"{int(resample_sec)}s"
    s = series.resample(freq).last()

    obs = series.dropna().sort_index()
    med_gap = None
    if len(obs) >= 2:
        gaps = obs.index.to_series().diff().dt.total_seconds().dropna()
        if len(gaps):
            med_gap = float(gaps.median())
    info["median_gap_sec"] = med_gap

    if ffill_limit <= 0:
        if med_gap is not None and med_gap > 0:
            buckets = max(1, int(round(med_gap / float(resample_sec))))
            ffill_limit = max(int(n_lags) + 1, buckets)
        else:
            ffill_limit = int(n_lags) + 1
        ffill_limit = min(int(max_ffill_cap), int(ffill_limit))

    if max_gap_sec <= 0:
        if med_gap is not None and med_gap > 0:
            max_gap_sec = int(min(24 * 3600, max(3600, 6 * med_gap)))
        else:
            max_gap_sec = 6 * 3600

    info["ffill_limit"] = int(ffill_limit)
    info["max_gap_sec"] = int(max_gap_sec)

    # gap guard
    try:
        idx = pd.Series(s.index, index=s.index)
        last_obs_ts = idx.where(s.notna()).ffill()
        age_sec = (idx - last_obs_ts).dt.total_seconds()
        s = s.where(age_sec <= float(max_gap_sec))
    except Exception:
        pass

    if int(ffill_limit) > 0:
        s = s.ffill(limit=int(ffill_limit))

    horizon_steps = max(1, int(round(float(horizon_sec) / float(resample_sec))))
    y = s.shift(-horizon_steps)

    X = pd.DataFrame(index=s.index)
    for k in range(1, int(n_lags) + 1):
        X[f"lag_{k}"] = s.shift(k)

    mask = y.notna()
    X = X.loc[mask]
    y = y.loc[mask]

    if min_non_nan_lags and int(min_non_nan_lags) > 0:
        nn = X.notna().sum(axis=1)
        X = X.loc[nn >= int(min_non_nan_lags)]
        y = y.loc[X.index]
        X = X.fillna(0.0)
    else:
        X = X.dropna(axis=0, how="any")
        y = y.loc[X.index]

    info["aligned_rows"] = int(len(X))
    info["resampled_non_nan"] = int(s.notna().sum())

    return X, y, horizon_steps, info


def train_univariate_rf(X: pd.DataFrame, y: pd.Series) -> Tuple[Any, Dict[str, Any]]:
    """
    Simple univariate RF regression. Time-ordered split (80/20).
    """
    n = int(len(X))
    split = int(n * 0.8)
    X_tr, X_te = X.iloc[:split], X.iloc[split:]
    y_tr, y_te = y.iloc[:split], y.iloc[split:]

    model = RandomForestRegressor(
        n_estimators=300,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_tr, y_tr)

    mae = None
    rmse = None
    if len(X_te) >= 1:
        y_hat = model.predict(X_te)
        mae = float(mean_absolute_error(y_te.values, y_hat))
        rmse = float(mean_squared_error(y_te.values, y_hat, squared=False))

    meta = {
        "n_rows": int(n),
        "n_train": int(len(X_tr)),
        "n_test": int(len(X_te)),
        "mae": mae,
        "rmse": rmse,
        "feature_names": list(X.columns),
    }
    return model, meta


def main():
    # Ensure the training-friendly table exists (idempotent)
    ensure_dw_raw_by_ws()

    cfg = ConfigReader()
    resample_sec = int(getattr(cfg, "resample_seconds", 60) or 60)
    horizon_sec = int(getattr(cfg, "prediction_horizon_sec", 0) or resample_sec)

    ap = argparse.ArgumentParser()
    ap.add_argument("--plId", required=True, type=int)
    ap.add_argument("--wcId", required=True, type=int)
    ap.add_argument("--wsId", required=True, type=int)

    ap.add_argument("--days", default=7, type=int)
    ap.add_argument("--limit", default=200000, type=int)
    ap.add_argument("--chunk_hours", default=6, type=int)
    ap.add_argument("--max_retries", default=3, type=int)

    ap.add_argument("--stNo", default=None)
    ap.add_argument("--opTc", default=None)

    ap.add_argument("--targets", default=None, help="Comma-separated equipment_name targets. If omitted, auto-select.")
    ap.add_argument("--top_k", default=2, type=int, help="Auto-select top-k targets if --targets omitted.")
    ap.add_argument("--min_cov", default=0.10, type=float, help="Min coverage for auto target selection.")

    ap.add_argument("--n_lags", default=10, type=int)
    ap.add_argument("--min_rows", default=200, type=int)

    ap.add_argument("--ffill_limit", default=0, type=int, help="0=auto. Forward-fill limit in resample buckets.")
    ap.add_argument("--max_gap_sec", default=0, type=int, help="0=auto. Prevent filling across gaps larger than this (seconds).")
    ap.add_argument("--min_non_nan_lags", default=0, type=int, help="0=strict. If >0, allow partial lag rows and fill missing lags.")

    ap.add_argument("--out_dir", default="./models/offline_outonly")
    args = ap.parse_args()

    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=int(args.days))

    # Determine ws_uid / tags
    ws_uid = f"{args.plId}:WC{args.wcId}:WS{args.wsId}"
    st_tag = safe_token(args.stNo) if args.stNo else "ALL"
    op_tag = safe_token(args.opTc) if args.opTc else "ALL"

    # Targets: explicit or auto (later after we fetch)
    targets: List[str] = []
    if args.targets:
        targets = [t.strip() for t in str(args.targets).split(",") if t.strip()]

    # Fetch rows from training-friendly table
    rows = fetch_output_rows_by_ws(
        pl_id=args.plId,
        wc_id=args.wcId,
        ws_id=args.wsId,
        start_dt=start_dt,
        end_dt=end_dt,
        limit=int(args.limit),
        st_no=args.stNo,
        op_tc=args.opTc,
        chunk_hours=int(args.chunk_hours),
        max_retries=int(args.max_retries),
        show_progress=True,
    )

    print(
        f"FETCH_OK rows={len(rows)} start={start_dt} end={end_dt} "
        f"plId={args.plId} wcId={args.wcId} wsId={args.wsId} stNo={args.stNo} opTc={args.opTc}"
    )

    wide = rows_to_wide_df(rows)
    print(f"WIDE_OK shape={wide.shape} outputs={len(wide.columns)}")

    if wide.empty:
        print("TRAIN_SKIPPED: empty output frame.")
        return

    # If targets were not provided, auto-select from available data (no extra Cassandra reads)
    if not targets:
        targets = choose_targets_from_wide(
            wide=wide,
            resample_sec=int(resample_sec),
            top_k=int(args.top_k),
            min_cov=float(args.min_cov),
        )

    print(f"TARGETS selected_targets={targets}")

    if not targets:
        print("TRAIN_SKIPPED: no targets selected (too sparse / no coverage).")
        return

    os.makedirs(args.out_dir, exist_ok=True)

    # Train per target
    iterator = targets
    if tqdm is not None:
        iterator = tqdm(targets, desc="Training targets", unit="target")

    for tgt in iterator:
        if tgt not in wide.columns:
            print(f"SKIP target={tgt} reason=not_present_in_frame")
            continue

        X, y, horizon_steps, dbg = build_lagged_dataset_sparse_friendly(
            wide[tgt],
            resample_sec=int(resample_sec),
            horizon_sec=int(horizon_sec),
            n_lags=int(args.n_lags),
            ffill_limit=int(args.ffill_limit),
            max_gap_sec=int(args.max_gap_sec),
            max_ffill_cap=600,
            min_non_nan_lags=int(args.min_non_nan_lags),
        )

        if len(X) < int(args.min_rows):
            if len(X) == 0:
                print(
                    f"SKIP target={tgt} reason=insufficient_rows n=0 "
                    f"(raw_points={dbg.get('raw_points')}, median_gap_sec={dbg.get('median_gap_sec')}, "
                    f"ffill_limit={dbg.get('ffill_limit')}, max_gap_sec={dbg.get('max_gap_sec')}, "
                    f"resampled_non_nan={dbg.get('resampled_non_nan')})"
                )
            else:
                print(f"SKIP target={tgt} reason=insufficient_rows n={len(X)}")
            continue

        model, m = train_univariate_rf(X, y)

        key = (
            f"WSUID_{safe_token(ws_uid)}"
            f"_ST_{st_tag}"
            f"_OPTC_{op_tag}"
            f"__OUTONLY__TGT_{safe_token(tgt)}"
            f"__HSEC_{int(horizon_sec)}"
        )

        model_path = os.path.join(args.out_dir, f"{key}__ALG_RF.pkl")
        meta_path = os.path.join(args.out_dir, f"{key}__meta.json")

        joblib.dump(model, model_path)

        meta = {
            "mode": "OUT_ONLY",
            "ws_uid": ws_uid,
            "pl_id": int(args.plId),
            "wc_id": int(args.wcId),
            "ws_id": int(args.wsId),
            "stock_no": args.stNo,
            "op_tc": args.opTc,
            "target": str(tgt),
            "trained_at_utc": datetime.utcnow().isoformat(),
            "resample_sec": int(resample_sec),
            "horizon_sec": int(horizon_sec),
            "horizon_steps": int(horizon_steps),
            "trainer": "offline_outonly_trainer",
            "selection": {
                "targets": targets,
                "top_k": int(args.top_k),
                "min_cov": float(args.min_cov),
            },
            "data_window": {"start_utc": start_dt.isoformat(), "end_utc": end_dt.isoformat()},
            "fetch": {
                "rows": int(len(rows)),
                "limit": int(args.limit),
                "chunk_hours": int(args.chunk_hours),
                "stNo": args.stNo,
                "opTc": args.opTc,
            },
            "metrics": m,
            "model_path": model_path,
        }

        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        print(f"TRAIN_OK target={tgt} model={model_path}")
        print(f"TRAIN_OK target={tgt} meta ={meta_path}")


if __name__ == "__main__":
    main()
