import json
import os
from pathlib import Path
from copy import deepcopy

BASE_DIR = Path(__file__).resolve().parent


def _env(name: str, default=None):
    v = os.getenv(name)
    return v if v not in (None, "") else default


class ConfigReader:
    """
    Loads utils/config.json and applies environment variable overrides.

    Env vars (recommended):
      - MSF_CASSANDRA_HOST, MSF_CASSANDRA_USERNAME, MSF_CASSANDRA_PASSWORD, MSF_CASSANDRA_KEYSPACE
      - MSF_KAFKA_BOOTSTRAP_SERVERS, MSF_KAFKA_SASL_USERNAME, MSF_KAFKA_SASL_PASSWORD
    """

    def __init__(self):
        self.BASE_DIR = BASE_DIR
        with open(f"{BASE_DIR}/config.json", "r", encoding="utf-8") as f:
            cfg = json.load(f)

        # Normalize + deep copy
        self._cfg = deepcopy(cfg)

        # --- Cassandra overrides ---
        cass = self._cfg.get("cassandra_props") or self._cfg.get("cassandra") or {}
        cass["host"] = _env("MSF_CASSANDRA_HOST", cass.get("host"))
        cass["username"] = _env("MSF_CASSANDRA_USERNAME", cass.get("username"))
        cass["password"] = _env("MSF_CASSANDRA_PASSWORD", cass.get("password"))
        cass["keyspace"] = _env("MSF_CASSANDRA_KEYSPACE", cass.get("keyspace"))

        # Keep both keys consistent
        self._cfg["cassandra_props"] = cass
        self._cfg["cassandra"] = {
            "host": cass.get("host"),
            "username": cass.get("username"),
            "password": cass.get("password"),
            "keyspace": cass.get("keyspace"),
        }

        # --- Kafka overrides (consumer/producer props) ---
        for props_key in ("consumer_props", "producer_props"):
            props = self._cfg.get(props_key) or {}
            props["bootstrap.servers"] = _env("MSF_KAFKA_BOOTSTRAP_SERVERS", props.get("bootstrap.servers"))

            # SASL auth is optional (only override if env provided)
            sasl_user = _env("MSF_KAFKA_SASL_USERNAME", None)
            sasl_pass = _env("MSF_KAFKA_SASL_PASSWORD", None)
            if sasl_user is not None:
                props["sasl.username"] = sasl_user
            if sasl_pass is not None:
                props["sasl.password"] = sasl_pass

            self._cfg[props_key] = props

        # Expose keys as attributes for backward compatibility with code that does cfg["x"]
        for k, v in self._cfg.items():
            setattr(self, k, v)

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key, default=None):
        return getattr(self, key, default)
