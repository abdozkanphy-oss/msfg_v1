# modules/model_registry.py
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

_SAFE_TOKEN_RE = re.compile(r"[^A-Za-z0-9_.-]+")

def safe_token(x: str, default: str = "UNKNOWN") -> str:
    if x is None:
        return default
    s = str(x).strip()
    if not s or s.lower() == "none":
        return default
    return _SAFE_TOKEN_RE.sub("_", s)

# Filename produced by offline_outonly_trainer.py:
# WSUID_<wsuidtoken>_ST_<stock>_OPTC_<op>__OUTONLY__TGT_<tgt>__HSEC_<sec>__meta.json
_META_RE = re.compile(
    r"^WSUID_(?P<wsuid>.+?)_ST_(?P<st>.+?)_OPTC_(?P<op>.+?)__OUTONLY__TGT_(?P<tgt>.+?)__HSEC_(?P<hsec>\d+)__meta\.json$"
)

@dataclass(frozen=True)
class OutOnlyArtifact:
    wsuid_token: str
    stock_tag: str
    op_tag: str
    target_tag: str
    horizon_sec: int
    model_path: str
    meta_path: str
    mae: Optional[float]
    n_test: int
    n_train: int
    trained_at_utc: str
    feature_names: List[str]
    resample_sec: int


class ModelRegistry:
    def __init__(self, model_dir: str):
        self.model_dir = model_dir
        self._outonly: List[OutOnlyArtifact] = []
        self._indexed: bool = False

    def refresh(self) -> None:
        self._outonly = []
        if not os.path.isdir(self.model_dir):
            self._indexed = True
            return

        for fn in os.listdir(self.model_dir):
            m = _META_RE.match(fn)
            if not m:
                continue

            meta_path = os.path.join(self.model_dir, fn)
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f) or {}
            except Exception:
                continue

            # model path: prefer meta field; fallback derived from meta filename
            model_path = meta.get("model_path")
            if not model_path:
                # replace __meta.json with __ALG_RF.pkl (current trainer)
                model_path = os.path.join(self.model_dir, fn.replace("__meta.json", "__ALG_RF.pkl"))

            metrics = meta.get("metrics") or {}
            mae = metrics.get("mae")
            try:
                mae = float(mae) if mae is not None else None
            except Exception:
                mae = None

            n_test = int(metrics.get("n_test") or 0)
            n_train = int(metrics.get("n_train") or 0)
            trained_at = str(meta.get("trained_at_utc") or "")

            feature_names = meta.get("metrics", {}).get("feature_names") or meta.get("feature_names") or []
            if not isinstance(feature_names, list):
                feature_names = []

            resample_sec = int(meta.get("resample_sec") or 60)

            art = OutOnlyArtifact(
                wsuid_token=str(m.group("wsuid")),
                stock_tag=str(m.group("st")),
                op_tag=str(m.group("op")),
                target_tag=str(m.group("tgt")),
                horizon_sec=int(m.group("hsec")),
                resample_sec=int(resample_sec),
                model_path=str(model_path),
                meta_path=str(meta_path),
                mae=mae,
                n_test=n_test,
                n_train=n_train,
                trained_at_utc=trained_at,
                feature_names=[str(x) for x in feature_names],
            )

            self._outonly.append(art)

        self._indexed = True

    def list_outonly(self) -> List[OutOnlyArtifact]:
        if not self._indexed:
            self.refresh()
        return list(self._outonly)

    def find_best_outonly(
        self,
        wsuid_token: str,
        stock: str,
        op_tc: str,
        target: str,
        horizon_sec: int,
        min_test: int = 20,
    ) -> Optional[OutOnlyArtifact]:
        """
        Matching strategy (strict → fallback):
        - wsuid_token must match
        - horizon_sec must match
        - target token must match (safe_token)
        - try (stock, op_tc) exact, then op=ALL, then stock=ALL, then both ALL
        """
        if not self._indexed:
            self.refresh()

        wsuid_token = safe_token(wsuid_token)
        stock_tag = safe_token(stock, default="ALL")
        op_tag = safe_token(op_tc, default="ALL")
        tgt_tag = safe_token(target)

        # Candidate filters
        base = [
            a for a in self._outonly
            if a.wsuid_token == wsuid_token
            and a.horizon_sec == int(horizon_sec)
            and a.target_tag == tgt_tag
        ]
        if not base:
            return None

        # preference ladder
        ladders = [
            (stock_tag, op_tag),
            (stock_tag, "ALL"),
            ("ALL", op_tag),
            ("ALL", "ALL"),
        ]

        for st, op in ladders:
            cands = [a for a in base if a.stock_tag == st and a.op_tag == op]
            if not cands:
                continue

            # choose best: lowest mae if enough test; else largest n_test
            good = [a for a in cands if a.mae is not None and a.n_test >= int(min_test)]
            if good:
                return sorted(good, key=lambda a: a.mae)[0]

            # fallback: prefer any mae, else n_test
            any_mae = [a for a in cands if a.mae is not None]
            if any_mae:
                return sorted(any_mae, key=lambda a: a.mae)[0]

            return sorted(cands, key=lambda a: a.n_test, reverse=True)[0]

        return None


# singleton registry cache per dir
_REGISTRY_BY_DIR: Dict[str, ModelRegistry] = {}

def get_outonly_registry(model_dir: str) -> ModelRegistry:
    model_dir = model_dir or "./models/offline_outonly"
    reg = _REGISTRY_BY_DIR.get(model_dir)
    if reg is None:
        reg = ModelRegistry(model_dir)
        reg.refresh()
        _REGISTRY_BY_DIR[model_dir] = reg
    return reg
