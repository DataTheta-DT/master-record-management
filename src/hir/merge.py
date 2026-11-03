import hashlib
import uuid

from pyspark.sql import functions as F

from . import audit, decisions, mpi, store


def _members(spark, cfg, master_id):
    return [r["record_id"] for r in mpi.current_xref(spark, cfg).filter(F.col("master_id") == master_id).collect()]


def unmerge_records(spark, cfg, master_id, record_ids, actor, reason, run_id="manual", role="admin", block_remerge=True, target_master_id=None):
    members = _members(spark, cfg, master_id)
    if not members:
        raise ValueError(f"master {master_id} has no current members")
    record_ids = list(dict.fromkeys(record_ids))
    missing = [r for r in record_ids if r not in members]
    if missing:
        raise ValueError(f"records not in master {master_id}: {missing}")
    remaining = [m for m in members if m not in record_ids]
    if not remaining:
        raise ValueError("cannot split all records out of a master; at least one record must stay")
    if not reason:
        raise ValueError("a reason is required")
    seed = hashlib.sha256(("|".join(sorted(record_ids)) + uuid.uuid4().hex).encode()).hexdigest()
    new_id = target_master_id or ("EMPI-" + seed[:12].upper())
    rows = [(r, new_id, "UNMERGED", None) for r in record_ids]
    assign = spark.createDataFrame(rows, "record_id string, master_id string, link_type string, evidence_pair_id string")
    res = mpi.apply_membership(spark, cfg, assign, "UNMERGE", run_id, actor=actor, role=role, reason=reason)
    if block_remerge:
        decisions.add(spark, cfg, [{"record_id_a": a, "record_id_b": b, "label": "NON_MATCH", "source": "UNMERGE", "validated": True, "decided_by": actor, "comment": reason} for a in record_ids for b in remaining], run_id)
    res["new_master_id"] = new_id
    res["remaining_members"] = remaining
    return res


def manual_merge(spark, cfg, master_a, master_b, actor, reason, run_id="manual", role="admin", override_do_not_merge=False):
    if master_a == master_b:
        raise ValueError("masters are the same")
    cm = mpi.current_master(spark, cfg).filter(F.col("master_id").isin(master_a, master_b)).collect()
    if len(cm) != 2 or any(r["status"] != "ACTIVE" for r in cm):
        raise ValueError("both masters must exist and be ACTIVE")
    if not reason:
        raise ValueError("a reason is required")
    survivor = sorted(cm, key=lambda r: (r["created_at"], r["master_id"]))[0]["master_id"]
    other = master_b if survivor == master_a else master_a
    ma, mb = _members(spark, cfg, survivor), _members(spark, cfg, other)
    dnm = decisions.effective(spark, cfg).filter("label = 'NON_MATCH'").collect()
    blocked = [d for d in dnm if (d["record_id_a"] in ma and d["record_id_b"] in mb) or (d["record_id_b"] in ma and d["record_id_a"] in mb)]
    if blocked and not override_do_not_merge:
        raise ValueError(f"{len(blocked)} do-not-merge decision(s) exist between these masters; pass override_do_not_merge=True to supersede them")
    if blocked:
        decisions.add(spark, cfg, [{"record_id_a": d["record_id_a"], "record_id_b": d["record_id_b"], "label": "MATCH", "source": "MANUAL_MERGE", "validated": True, "decided_by": actor, "comment": reason} for d in blocked], run_id)
    rows = [(r, survivor, "MANUAL_MERGE", None) for r in mb]
    assign = spark.createDataFrame(rows, "record_id string, master_id string, link_type string, evidence_pair_id string")
    res = mpi.apply_membership(spark, cfg, assign, "MANUAL_MERGE", run_id, actor=actor, role=role, reason=reason)
    res["survivor"] = survivor
    res["merged_away"] = other
    return res


def reverse_event(spark, cfg, event_id, actor, reason, run_id="manual", role="admin", block_remerge=False):
    xref = mpi.xref_df(spark, cfg)
    opened = xref.filter((F.col("change_event_id") == event_id)).collect()
    if not opened:
        raise ValueError(f"no links opened by event {event_id}")
    still = [r for r in opened if r["is_current"]]
    if len(still) != len(opened):
        raise ValueError("some links from this event were changed by a later event; reverse the later event first")
    closed = {r["record_id"]: r["master_id"] for r in xref.filter(F.col("closed_event_id") == event_id).collect()}
    rows = [(r["record_id"], closed[r["record_id"]], "REVERSED", None) for r in still if r["record_id"] in closed]
    if not rows:
        raise ValueError("event created only new links; nothing to restore")
    assign = spark.createDataFrame(rows, "record_id string, master_id string, link_type string, evidence_pair_id string")
    res = mpi.apply_membership(spark, cfg, assign, "REVERSE_EVENT", run_id, actor=actor, role=role, reason=f"{reason} (reverses {event_id})")
    if block_remerge:
        by_master = {}
        for rid, m in closed.items():
            by_master.setdefault(m, []).append(rid)
        moved_to = {r["record_id"]: r["master_id"] for r in still}
        pairs = [(a, b) for a in closed for b in closed if a < b and closed[a] != closed[b]]
        decisions.add(spark, cfg, [{"record_id_a": a, "record_id_b": b, "label": "NON_MATCH", "source": "REVERSE_EVENT", "validated": True, "decided_by": actor, "comment": reason} for a, b in pairs], run_id)
    return res
