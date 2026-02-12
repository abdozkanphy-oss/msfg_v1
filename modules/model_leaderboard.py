# modules/model_leaderboard.py
from __future__ import annotations

import argparse
from modules.model_registry import get_outonly_registry

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="./models/offline_outonly")
    ap.add_argument("--top", type=int, default=50)
    args = ap.parse_args()

    reg = get_outonly_registry(args.dir)
    arts = reg.list_outonly()

    # sort by mae (None last), then by n_test desc
    def key(a):
        mae = a.mae if a.mae is not None else 1e18
        return (mae, -a.n_test)

    arts = sorted(arts, key=key)[: int(args.top)]

    print(f"FOUND outonly_models={len(reg.list_outonly())}")
    print("TOP MODELS (lowest MAE):")
    for a in arts:
        print(
            f"mae={a.mae} n_test={a.n_test} "
            f"WSUID={a.wsuid_token} ST={a.stock_tag} OPTC={a.op_tag} "
            f"TGT={a.target_tag} HSEC={a.horizon_sec} "
            f"trained_at={a.trained_at_utc}"
        )

if __name__ == "__main__":
    main()
