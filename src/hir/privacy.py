import os

from pyspark.sql import functions as F


def get_salt(cfg, dbutils=None):
    s = cfg["security"]["salt"]
    val = os.environ.get(s.get("env_var") or "")
    if val:
        return val, "env"
    if dbutils is not None and s.get("secret_scope") and s.get("secret_key"):
        return dbutils.secrets.get(s["secret_scope"], s["secret_key"]), "secret_scope"
    if s.get("allow_dev_default"):
        return s["dev_default"], "dev_default"
    raise RuntimeError("No salt available: set the env var or a Databricks secret, or allow dev_default")


def strong_id_enabled(cfg, name):
    c = cfg["security"]["strong_identifiers"].get(name, {})
    return bool(c.get("enabled") and c.get("legal_basis_confirmed"))


def mask_expr(col, mode):
    c = F.col(col) if isinstance(col, str) else col
    s = c.cast("string")
    if mode == "full":
        return s
    if mode == "hidden":
        return F.when(s.isNotNull(), F.lit("***"))
    if mode == "last4":
        return F.when(s.isNotNull(), F.concat(F.lit("***"), F.substring(s, -4, 4)))
    if mode == "initial":
        return F.when(s.isNotNull(), F.concat(F.substring(s, 1, 1), F.lit("***")))
    if mode == "year":
        return F.when(s.isNotNull(), F.concat(F.substring(s, 1, 4), F.lit("-**-**")))
    if mode == "pincode":
        return F.when(s.isNotNull(), F.concat(F.substring(s, 1, 3), F.lit("***")))
    if mode == "hash_match_only":
        return F.when(s.isNotNull(), F.lit("(hidden)"))
    raise ValueError(f"unknown mask mode {mode}")


def role_modes(cfg, role):
    vis = cfg["security"]["role_visibility"]
    if role not in vis:
        raise PermissionError(f"role {role} has no visibility profile")
    return vis[role]
