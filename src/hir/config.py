import copy
import hashlib
import json
from pathlib import Path

SECTIONS = {
    "pipeline": "pipeline_config.json",
    "sources": "sources_config.json",
    "matching": "matching_config.json",
    "survivorship": "survivorship_config.json",
    "review": "review_config.json",
    "security": "security_config.json",
}


def deep_merge(base, over):
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def config_hash(cfg):
    body = {k: cfg[k] for k in sorted(SECTIONS)}
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def load_config(root, overrides=None, config_dir="configs"):
    cfg = {}
    for key, fname in SECTIONS.items():
        with open(Path(root) / config_dir / fname, "r", encoding="utf-8") as fh:
            cfg[key] = json.load(fh)
    for key, over in (overrides or {}).items():
        cfg[key] = deep_merge(cfg[key], over)
    cfg["hash"] = config_hash(cfg)
    return cfg


def fq(cfg, key):
    st = cfg["pipeline"]["storage"]
    name = cfg["pipeline"]["tables"][key]
    parts = [p for p in (st.get("catalog"), st.get("schema"), name) if p]
    return ".".join(parts)


def schema_fq(cfg):
    st = cfg["pipeline"]["storage"]
    return ".".join(p for p in (st.get("catalog"), st.get("schema")) if p)


def source_names(cfg):
    return list(cfg["sources"]["sources"].keys())


def validate_config(cfg):
    errors = []
    m = cfg["matching"]
    th = m["thresholds"]
    if not (0 < th["low"] < th["high"] <= 1):
        errors.append("thresholds must satisfy 0 < low < high <= 1")
    for field, levels in m["fields"].items():
        for lvl, p in levels.items():
            if not (0 < p["m"] < 1 and 0 < p["u"] < 1):
                errors.append(f"{field}.{lvl}: m and u must be between 0 and 1")
        if abs(sum(p["m"] for p in levels.values()) - 1) > 0.01:
            errors.append(f"{field}: m values must sum to 1")
        if abs(sum(p["u"] for p in levels.values()) - 1) > 0.01:
            errors.append(f"{field}: u values must sum to 1")
    required_fields = {"first_name", "last_name", "dob", "gender", "address", "phone"}
    if required_fields - set(m["fields"]):
        errors.append(f"matching.fields missing: {sorted(required_fields - set(m['fields']))}")
    if not m["blocking"]["rules"]:
        errors.append("at least one blocking rule is required")
    if m["safety_rules"]["strong_id_conflict_action"] not in ("cap_medium", "force_low"):
        errors.append("strong_id_conflict_action must be cap_medium or force_low")
    r = cfg["review"]
    for name, p in r["policies"].items():
        if p["approvals_needed"] > p["required_reviewers"]:
            errors.append(f"policy {name}: approvals_needed exceeds required_reviewers")
        if p["approvals_needed"] < 1:
            errors.append(f"policy {name}: approvals_needed must be at least 1")
    for rule in r["policy_selection"]:
        if rule["policy"] not in r["policies"]:
            errors.append(f"policy_selection references unknown policy {rule['policy']}")
    if not r["policy_selection"] or r["policy_selection"][-1].get("when"):
        errors.append("last policy_selection rule must have an empty 'when' as the default")
    roles = {x["role"] for x in r["reviewers"]}
    if not set(r["escalation"]["resolver_roles"]) & roles:
        errors.append("no reviewer holds an escalation resolver role")
    ids = [x["id"] for x in r["reviewers"]]
    if len(ids) != len(set(ids)):
        errors.append("reviewer ids must be unique")
    known_cols = {"first_name_norm", "middle_name_norm", "last_name_norm", "dob", "gender_norm", "address_line_norm", "city_norm", "state_norm", "pincode", "phone_norm", "aadhaar_hash", "aadhaar_last4"}
    for f in cfg["survivorship"]["fields"]:
        bad = set(f["columns"]) - known_cols
        if bad:
            errors.append(f"survivorship field {f['name']}: unknown columns {sorted(bad)}")
        for s in f["strategies"]:
            if s not in ("most_frequent", "most_recent", "source_priority", "most_complete", "longest"):
                errors.append(f"survivorship field {f['name']}: unknown strategy {s}")
    for sname, s in cfg["sources"]["sources"].items():
        if s["name"]["mode"] not in ("split", "first_last", "last_comma_first"):
            errors.append(f"{sname}: unknown name mode")
        if s["address"]["mode"] not in ("split", "single_line"):
            errors.append(f"{sname}: unknown address mode")
    if set(cfg["survivorship"]["source_priority"]) != set(cfg["sources"]["sources"]):
        errors.append("survivorship.source_priority must list every source system exactly once")
    return errors


def assert_valid(cfg):
    errors = validate_config(cfg)
    if errors:
        raise ValueError("Invalid configuration:\n- " + "\n- ".join(errors))
    return cfg
