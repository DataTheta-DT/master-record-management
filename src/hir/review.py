import hashlib
import json
from datetime import datetime, timezone

from pyspark.sql import Window, functions as F

from . import audit, decisions, store
from .config import fq
from .privacy import mask_expr, role_modes

TASK_SCHEMA = "task_id string, pair_id string, task_type string, record_id_a string, record_id_b string, score double, explanation string, safety_flags array<string>, policy_name string, required_reviewers int, approvals_needed int, early_resolution boolean, escalate_on_split_vote boolean, priority double, created_at timestamp, run_id string"
DECISION_SCHEMA = "decision_id string, task_id string, reviewer_id string, reviewer_role string, decision string, comment string, stage string, decided_at timestamp"
ESC_SCHEMA = "task_id string, escalated_at timestamp, reason string, tally string"
RES_SCHEMA = "task_id string, pair_id string, outcome string, method string, tally string, resolved_by string, resolved_at timestamp"

POSITIVE = "APPROVE"
NEGATIVE = ("REJECT", "SPLIT")


def select_policy(rcfg, score, flags):
    flags = set(flags or [])
    for rule in rcfg["policy_selection"]:
        w = rule.get("when", {})
        if "flags_any" in w and not (flags & set(w["flags_any"])):
            continue
        if "min_score" in w and score < w["min_score"]:
            continue
        if "max_score" in w and score > w["max_score"]:
            continue
        return rule["policy"], rcfg["policies"][rule["policy"]]
    raise ValueError("no policy matched")


def evaluate_policy(votes, policy):
    pos = sum(1 for v in votes if v == POSITIVE)
    neg = sum(1 for v in votes if v in NEGATIVE)
    unsure = sum(1 for v in votes if v == "UNSURE")
    n = len(votes)
    k = policy["approvals_needed"]
    req = policy["required_reviewers"]
    tally = {"approve": pos, "negative": neg, "unsure": unsure, "votes": n}
    both = pos > 0 and neg > 0
    if policy.get("escalate_on_split_vote") and both:
        return "ESCALATE", tally
    complete = n >= req
    if policy.get("early_resolution") or complete:
        if pos >= k:
            return "APPROVED", tally
        if neg >= k:
            return "REJECTED", tally
    if complete:
        return "ESCALATE", tally
    return None, tally


def init(spark, cfg):
    for key, schema in (("review_tasks", TASK_SCHEMA), ("review_decisions", DECISION_SCHEMA), ("review_escalations", ESC_SCHEMA), ("review_resolutions", RES_SCHEMA)):
        if not store.has(spark, cfg, key):
            store.write(spark, cfg, spark.createDataFrame([], schema), key)


def create_tasks(spark, cfg, run_id):
    init(spark, cfg)
    rcfg = cfg["review"]
    th = cfg["matching"]["thresholds"]
    mid = (th["high"] + th["low"]) / 2
    pairs = store.read(spark, cfg, "pairs")
    xref = store.read(spark, cfg, "xref").filter("is_current")
    ma = xref.select(F.col("record_id").alias("ra"), F.col("master_id").alias("ma"))
    mb = xref.select(F.col("record_id").alias("rb"), F.col("master_id").alias("mb"))
    p = pairs.join(ma, pairs.record_id_a == ma.ra, "left").join(mb, pairs.record_id_b == mb.rb, "left")
    same_master = F.col("ma").isNotNull() & (F.col("ma") == F.col("mb"))
    existing = store.read(spark, cfg, "review_tasks").select("pair_id", "task_type").withColumn("_has", F.lit(True))
    review = p.filter((F.col("band") == "MEDIUM") & F.col("override_label").isNull() & ~same_master).withColumn("task_type", F.lit("PAIR_REVIEW"))
    rate = rcfg["audit_sampling"]["auto_merge_sample_rate"]
    bucket = F.abs(F.hash("pair_id")) % 10000
    audit_pairs = p.filter((F.col("band") == "HIGH") & F.col("override_label").isNull() & same_master & (bucket < int(rate * 10000))).withColumn("task_type", F.lit("MERGE_AUDIT"))
    dnm = decisions.effective(spark, cfg).filter("label = 'NON_MATCH'")
    da = xref.select(F.col("record_id").alias("ra"), F.col("master_id").alias("ma"))
    db = xref.select(F.col("record_id").alias("rb"), F.col("master_id").alias("mb"))
    conflict = dnm.join(da, dnm.record_id_a == da.ra).join(db, dnm.record_id_b == db.rb).filter(F.col("ma") == F.col("mb"))
    sc = pairs.select("pair_id", "score")
    conflict = conflict.join(sc, "pair_id", "left").select(
        "pair_id", "record_id_a", "record_id_b", F.coalesce("score", F.lit(0.0)).alias("score"),
        F.lit("conflict: a reviewer or admin marked this pair as different people, but both records share one master").alias("explanation"),
        F.array(F.lit("DO_NOT_MERGE_CONFLICT")).alias("safety_flags"), F.lit("MERGE_AUDIT").alias("task_type"))
    base_cols = ["pair_id", "record_id_a", "record_id_b", "score", "explanation", "safety_flags", "task_type"]
    cand = review.select(*base_cols).unionByName(audit_pairs.select(*base_cols)).unionByName(conflict.select(*base_cols))
    cand = cand.dropDuplicates(["pair_id", "task_type"]).join(existing, ["pair_id", "task_type"], "left").filter(F.col("_has").isNull())
    rows = cand.select("pair_id", "task_type", "record_id_a", "record_id_b", "score", "explanation", "safety_flags").collect()
    out = []
    now = datetime.now(timezone.utc)
    for r in rows:
        pname, pol = select_policy(rcfg, r["score"], r["safety_flags"])
        out.append((
            ("RT-" if r["task_type"] == "PAIR_REVIEW" else "RA-") + r["pair_id"][:12], r["pair_id"], r["task_type"], r["record_id_a"], r["record_id_b"], float(r["score"]), r["explanation"], list(r["safety_flags"] or []),
            pname, int(pol["required_reviewers"]), int(pol["approvals_needed"]), bool(pol.get("early_resolution", True)), bool(pol.get("escalate_on_split_vote", False)),
            float(abs(r["score"] - mid)), now, run_id,
        ))
    if out:
        df = spark.createDataFrame(out, TASK_SCHEMA)
        store.append(spark, cfg, df, "review_tasks")
        ev = df.select(F.col("task_id").alias("entity_id"), F.to_json(F.struct("task_type", "policy_name", "score")).alias("after_json"))
        audit.log_df(spark, cfg, ev, "REVIEW_TASK_CREATED", "review_task", run_id)
    return {"tasks_created": len(out), "pair_review": sum(1 for o in out if o[2] == "PAIR_REVIEW"), "merge_audit": sum(1 for o in out if o[2] == "MERGE_AUDIT")}


def reviewer_registry(cfg):
    return {r["id"]: r["role"] for r in cfg["review"]["reviewers"]}


def task_status(spark, cfg):
    init(spark, cfg)
    t = store.read(spark, cfg, "review_tasks")
    d = store.read(spark, cfg, "review_decisions").filter("stage = 'PRIMARY'").groupBy("task_id").agg(F.count("*").alias("n_votes"), F.collect_list("decision").alias("votes"))
    e = store.read(spark, cfg, "review_escalations").select("task_id").withColumn("escalated", F.lit(True))
    r = store.read(spark, cfg, "review_resolutions").select("task_id", "outcome", "method", "resolved_at")
    s = t.join(d, "task_id", "left").join(e, "task_id", "left").join(r, "task_id", "left")
    status = F.when(F.col("outcome").isNotNull(), F.col("outcome")).when(F.col("escalated").isNotNull(), "ESCALATED").when(F.col("n_votes").isNotNull(), "IN_REVIEW").otherwise("OPEN")
    return s.withColumn("status", status).withColumn("n_votes", F.coalesce("n_votes", F.lit(0)))


def _task_row(spark, cfg, task_id):
    rows = store.read(spark, cfg, "review_tasks").filter(F.col("task_id") == task_id).collect()
    if not rows:
        raise ValueError(f"unknown task {task_id}")
    return rows[0]


class ReviewSession:
    def __init__(self, cfg, tasks, decs, esc, res):
        self.cfg = cfg
        self.registry = reviewer_registry(cfg)
        self.tasks = tasks
        self.decs = decs
        self.esc = esc
        self.res = res
        self.new_decs = []
        self.new_esc = []
        self.new_res = []
        self.new_pair_decisions = []
        self.audit_events = []

    @classmethod
    def load(cls, spark, cfg, task_ids=None):
        init(spark, cfg)
        t = store.read(spark, cfg, "review_tasks")
        d = store.read(spark, cfg, "review_decisions")
        e = store.read(spark, cfg, "review_escalations")
        r = store.read(spark, cfg, "review_resolutions")
        if task_ids is not None:
            t, d, e, r = [x.filter(F.col("task_id").isin(*task_ids)) for x in (t, d, e, r)]
        tasks = {x["task_id"]: x.asDict() for x in t.collect()}
        decs = {}
        for x in d.orderBy("decided_at").collect():
            decs.setdefault(x["task_id"], []).append(x.asDict())
        return cls(cfg, tasks, decs, {x["task_id"] for x in e.collect()}, {x["task_id"] for x in r.collect()})

    def status(self, task_id):
        if task_id in self.res:
            return "RESOLVED"
        if task_id in self.esc:
            return "ESCALATED"
        return "OPEN"

    def vote(self, task_id, reviewer_id, decision, comment=None):
        rcfg = self.cfg["review"]
        if reviewer_id not in self.registry:
            raise PermissionError(f"unknown reviewer {reviewer_id}")
        role = self.registry[reviewer_id]
        task = self.tasks.get(task_id)
        if task is None:
            raise ValueError(f"unknown task {task_id}")
        state = self.status(task_id)
        if state == "RESOLVED":
            raise ValueError("task already resolved")
        decs = self.decs.setdefault(task_id, [])
        escalated = state == "ESCALATED"
        if escalated:
            if role not in rcfg["escalation"]["resolver_roles"]:
                raise PermissionError("only a senior reviewer or admin can decide an escalated task")
            drop = "SPLIT" if task["task_type"] == "PAIR_REVIEW" else "REJECT"
            allowed = [d for d in rcfg["escalation"]["allowed_decisions"] if d != drop]
            need_comment = rcfg["escalation"]["comment_required"]
        else:
            allowed = rcfg["task_types"][task["task_type"]]["allowed_decisions"]
            need_comment = decision in rcfg["comment_required_for"]
        if decision not in allowed:
            raise ValueError(f"decision {decision} not allowed for this task; allowed: {allowed}")
        if need_comment and not (comment and comment.strip()):
            raise ValueError(f"a comment is required for {decision}")
        stage = "ESCALATION" if escalated else "PRIMARY"
        if any(d["reviewer_id"] == reviewer_id and d["stage"] == stage for d in decs):
            raise ValueError("reviewer has already decided on this task")
        if not escalated and len([d for d in decs if d["stage"] == "PRIMARY"]) >= task["required_reviewers"]:
            raise ValueError("all required reviews are already submitted")
        now = datetime.now(timezone.utc)
        did = hashlib.sha256(f"{task_id}|{reviewer_id}|{stage}|{now.isoformat()}|{len(decs)}".encode()).hexdigest()[:20]
        row = {"decision_id": did, "task_id": task_id, "reviewer_id": reviewer_id, "reviewer_role": role, "decision": decision, "comment": comment, "stage": stage, "decided_at": now}
        decs.append(row)
        self.new_decs.append(row)
        self.audit_events.append({"action": "REVIEW_DECISION", "entity_type": "review_task", "entity_id": task_id, "after_json": json.dumps({"decision": decision, "stage": stage}), "reason": comment, "actor": reviewer_id, "role": role})
        if escalated:
            outcome = {"APPROVE": "APPROVED", "REJECT": "REJECTED", "SPLIT": "SPLIT"}[decision]
            self._resolve(task, outcome, "ESCALATION", {"final": decision}, reviewer_id, role)
            return {"status": outcome, "method": "ESCALATION"}
        votes = [d["decision"] for d in decs if d["stage"] == "PRIMARY"]
        policy = {k: task[k] for k in ("required_reviewers", "approvals_needed", "early_resolution", "escalate_on_split_vote")}
        result, tally = evaluate_policy(votes, policy)
        if result == "ESCALATE":
            self.esc.add(task_id)
            self.new_esc.append({"task_id": task_id, "escalated_at": now, "reason": "reviewers disagree or no decision reached", "tally": json.dumps(tally)})
            self.audit_events.append({"action": "REVIEW_ESCALATED", "entity_type": "review_task", "entity_id": task_id, "after_json": json.dumps(tally), "reason": "reviewers disagree or no decision reached"})
            return {"status": "ESCALATED", "tally": tally}
        if result is None:
            return {"status": "IN_REVIEW", "tally": tally}
        if result == "REJECTED" and task["task_type"] == "MERGE_AUDIT":
            result = "SPLIT"
        self._resolve(task, result, "CONSENSUS", tally, "consensus", "system")
        return {"status": result, "tally": tally, "method": "CONSENSUS"}

    def _resolve(self, task, outcome, method, tally, by, role):
        now = datetime.now(timezone.utc)
        self.res.add(task["task_id"])
        self.new_res.append({"task_id": task["task_id"], "pair_id": task["pair_id"], "outcome": outcome, "method": method, "tally": json.dumps(tally), "resolved_by": by, "resolved_at": now})
        self.audit_events.append({"action": "REVIEW_RESOLVED", "entity_type": "review_task", "entity_id": task["task_id"], "after_json": json.dumps({"outcome": outcome, "method": method, "tally": tally}), "actor": by, "role": role})
        label = "MATCH" if outcome in ("APPROVED",) else "NON_MATCH"
        self.new_pair_decisions.append({"record_id_a": task["record_id_a"], "record_id_b": task["record_id_b"], "label": label, "source": method, "task_id": task["task_id"], "validated": True, "decided_by": by})

    def commit(self, spark, run_id="review"):
        c = self.cfg
        if self.new_decs:
            rows = [tuple(d[k.split()[0]] for k in DECISION_SCHEMA.split(", ")) for d in self.new_decs]
            store.append(spark, c, spark.createDataFrame(rows, DECISION_SCHEMA), "review_decisions")
        if self.new_esc:
            rows = [tuple(d[k.split()[0]] for k in ESC_SCHEMA.split(", ")) for d in self.new_esc]
            store.append(spark, c, spark.createDataFrame(rows, ESC_SCHEMA), "review_escalations")
        if self.new_res:
            rows = [tuple(d[k.split()[0]] for k in RES_SCHEMA.split(", ")) for d in self.new_res]
            store.append(spark, c, spark.createDataFrame(rows, RES_SCHEMA), "review_resolutions")
        if self.new_pair_decisions:
            decisions.add(spark, c, self.new_pair_decisions, run_id)
        if self.audit_events:
            audit.log(spark, c, self.audit_events, run_id)
        n = (len(self.new_decs), len(self.new_res), len(self.new_esc))
        self.new_decs, self.new_esc, self.new_res, self.new_pair_decisions, self.audit_events = [], [], [], [], []
        return {"decisions": n[0], "resolutions": n[1], "escalations": n[2]}


def submit_decision(spark, cfg, task_id, reviewer_id, decision, comment=None, run_id="review"):
    session = ReviewSession.load(spark, cfg, [task_id])
    result = session.vote(task_id, reviewer_id, decision, comment)
    session.commit(spark, run_id)
    return result


def apply_outcomes(spark, cfg, run_id, actor="review_outcomes"):
    import uuid
    from . import mpi, pipeline
    res = store.read(spark, cfg, "review_resolutions").filter("outcome = 'SPLIT'").select("task_id")
    tasks = store.read(spark, cfg, "review_tasks").select("task_id", "record_id_a", "record_id_b")
    sp = res.join(tasks, "task_id").select("task_id", "record_id_a", "record_id_b")
    x = mpi.current_xref(spark, cfg).select("record_id", "master_id")
    xa = x.select(F.col("record_id").alias("ra"), F.col("master_id").alias("ma"))
    xb = x.select(F.col("record_id").alias("rb"), F.col("master_id").alias("mb"))
    same = sp.join(xa, sp.record_id_a == xa.ra).join(xb, sp.record_id_b == xb.rb).filter(F.col("ma") == F.col("mb"))
    todo = [(r["record_id_b"], r["ma"]) for r in same.select("record_id_b", "ma").distinct().collect()]
    moved = {}
    for rid, m in todo:
        moved.setdefault(m, []).append(rid)
    out = {"splits_applied": 0, "masters_touched": len(moved)}
    if moved:
        sizes = {r["master_id"]: r["count"] for r in x.filter(F.col("master_id").isin(*moved.keys())).groupBy("master_id").count().collect()}
        token = uuid.uuid4().hex[:8]
        rows = []
        for m, rids in moved.items():
            rids = sorted(set(rids))
            if len(rids) >= sizes[m]:
                rids = rids[:-1]
            for rid in rids:
                new_id = "EMPI-" + hashlib.sha256(f"{rid}|{token}".encode()).hexdigest()[:12].upper()
                rows.append((rid, new_id, "UNMERGED", None))
        if rows:
            assign = spark.createDataFrame(rows, "record_id string, master_id string, link_type string, evidence_pair_id string")
            r = mpi.apply_membership(spark, cfg, assign, "UNMERGE", run_id, actor=actor, role="system", reason="review outcome SPLIT")
            out["splits_applied"] = len(rows)
            out["event_id"] = r["event_id"]
    out["refresh"] = pipeline.refresh_after_review(spark, cfg, run_id, actor)
    return out


def pending_table(spark, cfg, role="reviewer"):
    modes = role_modes(cfg, role)
    st = task_status(spark, cfg).filter(F.col("status").isin("OPEN", "IN_REVIEW", "ESCALATED"))
    pairs = store.read(spark, cfg, "pairs").select("pair_id", F.array_join(F.transform("field_levels", lambda x: F.concat(x["field"], F.lit("="), x["level"])), "; ").alias("field_comparison"))
    std = store.read(spark, cfg, "silver")

    def side(p):
        name = F.concat_ws(" ", mask_expr("first_name_norm", modes["first_name"]), mask_expr("middle_name_norm", modes["first_name"]), mask_expr("last_name_norm", modes["last_name"]))
        addr = F.concat_ws(", ", mask_expr("address_line_norm", modes["address"]), F.col("city_norm"), mask_expr("pincode", modes["address"]))
        return std.select(
            F.col("record_id").alias(p + "_record_id"), F.col("source_system").alias(p + "_system"), name.alias(p + "_name"),
            mask_expr("dob", modes["dob"]).alias(p + "_dob"), mask_expr("gender_norm", modes["gender"]).alias(p + "_gender"),
            addr.alias(p + "_address"), mask_expr("phone_norm", modes["phone"]).alias(p + "_phone"), mask_expr("aadhaar_last4", modes["aadhaar"]).alias(p + "_aadhaar"),
        )

    out = st.join(pairs, "pair_id", "left").join(side("a"), st.record_id_a == F.col("a_record_id")).join(side("b"), st.record_id_b == F.col("b_record_id"))
    return out.select(
        "task_id", "task_type", "status", "policy_name", F.round("score", 3).alias("score"), F.array_join("safety_flags", ", ").alias("safety_flags"),
        F.concat(F.col("n_votes").cast("string"), F.lit(" of "), F.col("required_reviewers").cast("string")).alias("reviews_done"),
        "a_record_id", "a_system", "a_name", "a_dob", "a_gender", "a_address", "a_phone", "a_aadhaar",
        "b_record_id", "b_system", "b_name", "b_dob", "b_gender", "b_address", "b_phone", "b_aadhaar",
        "field_comparison",
    ).orderBy(F.when(F.col("status") == "ESCALATED", 0).otherwise(1), F.when(F.col("task_type") == "PAIR_REVIEW", 0).otherwise(1), F.col("score").desc())


def bulk_decide(spark, cfg, reviewer_id, decisions, run_id="review"):
    ids = sorted({d[0] for d in decisions})
    session = ReviewSession.load(spark, cfg, ids)
    results = []
    for d in decisions:
        task_id, decision = d[0], d[1]
        comment = d[2] if len(d) > 2 else None
        try:
            r = session.vote(task_id, reviewer_id, decision, comment)
            results.append((task_id, decision, r["status"], None))
        except (ValueError, PermissionError) as e:
            results.append((task_id, decision, "NOT_SAVED", str(e)))
    session.commit(spark, run_id)
    return results
