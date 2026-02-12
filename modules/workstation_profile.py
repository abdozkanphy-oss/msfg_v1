# modules/workstation_profile.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from utils.config_reader import ConfigReader
from cassandra_utils.models.dw_single_data import dw_tbl_raw_data


@dataclass
class OutputSensorStat:
    equipment_name: str
    n_rows: int
    coverage: float  # fraction of resample buckets with a value


@dataclass
class StockProfile:
    stock_no: str
    n_rows: int
    n_unique_outputs: int
    top_outputs: List[OutputSensorStat]


@dataclass
class WorkstationProfile:
    pl_id: int
    wc_id: int
    ws_id: int
    start_dt: datetime
    end_dt: datetime
    total_rows: int
    stocks: List[StockProfile]
    resample_sec: int

    def best_stocks(self, k: int = 3) -> List[StockProfile]:
        return sorted(self.stocks, key=lambda s: s.n_rows, reverse=True)[:k]


def _fetch_rows(
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
):
    """
    Chunked fetch to reduce ALLOW FILTERING pressure and avoid list(queryset) length-hint (__len__) calls.
    """
    import time
    rows = []
    remaining = int(limit)

    t0 = start_dt
    while t0 < end_dt and remaining > 0:
        t1 = min(end_dt, t0 + timedelta(hours=int(chunk_hours)))

        for attempt in range(int(max_retries)):
            try:
                q = dw_tbl_raw_data.objects.allow_filtering()
                q = q.filter(
                    plant_id=int(pl_id),
                    work_center_id=int(wc_id),
                    work_station_id=int(ws_id),
                )
                # Use < end to avoid double counting boundaries
                q = q.filter(measurement_date__gte=t0, measurement_date__lt=t1)

                if st_no:
                    q = q.filter(produced_stock_no=str(st_no))
                if op_tc:
                    q = q.filter(operationtaskcode=str(op_tc))

                # Hard cap per-chunk to avoid huge responses
                per_chunk_cap = min(remaining, 50_000)
                q = q.limit(int(per_chunk_cap))

                part = []
                # DO NOT: list(q)  (can call __len__ as length hint)
                for r in q:
                    part.append(r)
                    if len(part) >= remaining:
                        break

                rows.extend(part)
                remaining = int(limit) - len(rows)
                break

            except Exception as e:
                if attempt >= int(max_retries) - 1:
                    raise
                time.sleep(float(sleep_base_sec) * (2 ** attempt))

        t0 = t1

    return rows



def _rows_to_df(rows) -> pd.DataFrame:
    # Output-only view: require counter_reading
    recs = []
    for r in rows:
        ts = getattr(r, "measurement_date", None)
        if ts is None:
            continue
        y = getattr(r, "counter_reading", None)
        if y is None:
            continue

        recs.append(
            {
                "ts": ts,
                "stock": getattr(r, "produced_stock_no", None) or "UNKNOWN_STOCK",
                "op_tc": getattr(r, "operationtaskcode", None) or "",
                "eq": getattr(r, "equipment_name", None) or "UNKNOWN_EQ",
                "y": float(y),
            }
        )

    if not recs:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(recs)
    df["ts"] = pd.to_datetime(df["ts"], utc=False)
    return df


def build_workstation_profile(
    pl_id: int,
    wc_id: int,
    ws_id: int,
    days: int = 7,
    limit: int = 200_000,
    st_no: Optional[str] = None,
    op_tc: Optional[str] = None,
    top_k_outputs: int = 6,
    min_cov: float = 0.10,
) -> WorkstationProfile:
    """
    Builds a data-driven profile for a workstation using RAW_TABLE output rows.
    Ignores equipment_type. A row is treated as output if counter_reading != null.
    """
    cfg = ConfigReader()
    resample_sec = int(getattr(cfg, "resample_seconds", 60) or 60)

    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(days=int(days))

    rows = _fetch_rows(pl_id, wc_id, ws_id, start_dt, end_dt, limit, st_no=st_no, op_tc=op_tc)
    df = _rows_to_df(rows)

    stocks: List[StockProfile] = []
    if df.empty:
        return WorkstationProfile(
            pl_id=pl_id,
            wc_id=wc_id,
            ws_id=ws_id,
            start_dt=start_dt,
            end_dt=end_dt,
            total_rows=0,
            stocks=[],
            resample_sec=resample_sec,
        )

    # coverage computed per output sensor after resample to a fixed grid
    freq = f"{int(resample_sec)}s"

    for stock, df_s in df.groupby("stock"):
        df_s = df_s.sort_values("ts")
        n_rows = int(len(df_s))
        unique_eq = df_s["eq"].nunique()

        # pivot: time x eq
        wide = df_s.pivot_table(index="ts", columns="eq", values="y", aggfunc="last")
        wide_r = wide.resample(freq).last()

        stats: List[OutputSensorStat] = []
        for eq in wide_r.columns:
            col = wide_r[eq]
            cov = float(col.notna().mean())
            cnt = int(col.notna().sum())
            stats.append(OutputSensorStat(equipment_name=str(eq), n_rows=cnt, coverage=cov))

        # filter by minimum coverage, sort by coverage then count
        stats = [s for s in stats if s.coverage >= float(min_cov)]
        stats = sorted(stats, key=lambda s: (s.coverage, s.n_rows), reverse=True)[: int(top_k_outputs)]

        stocks.append(
            StockProfile(
                stock_no=str(stock),
                n_rows=n_rows,
                n_unique_outputs=int(unique_eq),
                top_outputs=stats,
            )
        )

    stocks = sorted(stocks, key=lambda s: s.n_rows, reverse=True)

    return WorkstationProfile(
        pl_id=pl_id,
        wc_id=wc_id,
        ws_id=ws_id,
        start_dt=start_dt,
        end_dt=end_dt,
        total_rows=int(len(df)),
        stocks=stocks,
        resample_sec=resample_sec,
    )


def select_outonly_training_plan(
    profile: WorkstationProfile,
    top_stocks: int = 2,
    targets_per_stock: int = 2,
) -> Dict[str, Any]:
    """
    Returns a deterministic plan for OUT_ONLY training.
    """
    plan = {
        "mode": "OUT_ONLY",
        "pl_id": profile.pl_id,
        "wc_id": profile.wc_id,
        "ws_id": profile.ws_id,
        "resample_sec": profile.resample_sec,
        "window": {"start": profile.start_dt.isoformat(), "end": profile.end_dt.isoformat()},
        "stocks": [],
    }

    for sp in profile.best_stocks(k=int(top_stocks)):
        targets = [s.equipment_name for s in sp.top_outputs[: int(targets_per_stock)]]
        plan["stocks"].append(
            {
                "stock_no": sp.stock_no,
                "n_rows": sp.n_rows,
                "n_unique_outputs": sp.n_unique_outputs,
                "recommended_targets": targets,
                "candidates": [
                    {"eq": s.equipment_name, "coverage": s.coverage, "n_rows": s.n_rows}
                    for s in sp.top_outputs
                ],
            }
        )

    return plan
