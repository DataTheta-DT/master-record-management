import functools
import math

from pyspark.sql import functions as F

from .blocking import candidate_pairs
from .privacy import strong_id_enabled

RECORD_COLS = ["record_id", "source_system", "first_name_norm", "first_canon", "first_initial", "first_soundex", "last_name_norm", "last_soundex", "dob", "dob_year", "dob_month", "dob_day", "gender_norm", "address_line_norm", "pincode", "phone_norm", "aadhaar_hash"]


def weights_table(mcfg):
    out = {}
    for field, levels in mcfg["fields"].items():
        out[field] = {lvl: math.log2(p["m"] / p["u"]) for lvl, p in levels.items()}
        out[field]["missing"] = 0.0
    return out


def weight_bounds(mcfg):
    w = weights_table(mcfg)
    wmax = sum(max(v.values()) for v in w.values())
    wmin = sum(min(v.values()) for v in w.values())
    return wmin, wmax


def _sim(a, b):
    return F.lit(1.0) - F.levenshtein(a, b) / F.greatest(F.length(a), F.length(b))


def _both(a, b):
    return a.isNotNull() & b.isNotNull()


def _first_level(a, b, fz):
    fa, fb = a("first_name_norm"), b("first_name_norm")
    la, lb = a("last_name_norm"), b("last_name_norm")
    swapped = _both(fa, lb) & _both(fb, la) & (fa == lb) & (fb == la) & (fa != fb)
    initial = (F.length(fa) == 1) | (F.length(fb) == 1)
    return (
        F.when(~_both(fa, fb), "missing")
        .when(fa == fb, "exact")
        .when(a("first_canon") == b("first_canon"), "nickname")
        .when(swapped, "swapped")
        .when(initial & (a("first_initial") == b("first_initial")), "initial")
        .when(initial, "disagree")
        .when((_sim(fa, fb) >= fz) | (a("first_soundex") == b("first_soundex")) & (_sim(fa, fb) >= 0.6), "fuzzy")
        .otherwise("disagree")
    )


def _last_level(a, b, fz):
    la, lb = a("last_name_norm"), b("last_name_norm")
    fa, fb = a("first_name_norm"), b("first_name_norm")
    swapped = _both(fa, lb) & _both(fb, la) & (fa == lb) & (fb == la) & (fa != fb)
    return (
        F.when(~_both(la, lb), "missing")
        .when(la == lb, "exact")
        .when(swapped, "swapped")
        .when((_sim(la, lb) >= fz) | (a("last_soundex") == b("last_soundex")) & (_sim(la, lb) >= 0.6), "fuzzy")
        .otherwise("disagree")
    )


def _dob_level(a, b):
    da, db = a("dob"), b("dob")
    ye = (a("dob_year") == b("dob_year")).cast("int")
    me = (a("dob_month") == b("dob_month")).cast("int")
    de = (a("dob_day") == b("dob_day")).cast("int")
    swapped = (a("dob_year") == b("dob_year")) & (a("dob_month") == b("dob_day")) & (a("dob_day") == b("dob_month"))
    return (
        F.when(~_both(da, db), "missing")
        .when(da == db, "exact")
        .when(swapped, "transposed")
        .when(ye + me + de == 2, "close")
        .otherwise("disagree")
    )


def _gender_level(a, b):
    ga, gb = a("gender_norm"), b("gender_norm")
    return F.when(~_both(ga, gb), "missing").when(ga == gb, "agree").otherwise("disagree")


def _address_level(a, b, ex, fz):
    la, lb = a("address_line_norm"), b("address_line_norm")
    pa, pb = a("pincode"), b("pincode")
    sim = _sim(la, lb)
    return (
        F.when(_both(la, lb) & (sim >= ex), "line_exact")
        .when(_both(la, lb) & (sim >= fz), "line_fuzzy")
        .when(_both(pa, pb) & (pa == pb), "area_match")
        .when(_both(la, lb) | _both(pa, pb), "disagree")
        .otherwise("missing")
    )


def _phone_level(a, b):
    pa, pb = a("phone_norm"), b("phone_norm")
    return F.when(~_both(pa, pb), "missing").when(pa == pb, "exact").otherwise("disagree")


def score_pairs(spark, pairs, std, cfg):
    mcfg = cfg["matching"]
    comp = mcfg["comparison"]
    w = weights_table(mcfg)
    wmin, wmax = weight_bounds(mcfg)
    th = mcfg["thresholds"]
    sr = mcfg["safety_rules"]
    det = mcfg["deterministic"]
    det_on = strong_id_enabled(cfg, "aadhaar")
    rec = std.select(*RECORD_COLS)
    ra = rec.select(*[F.col(c).alias("a_" + c) for c in RECORD_COLS])
    rb = rec.select(*[F.col(c).alias("b_" + c) for c in RECORD_COLS])
    p = pairs.join(ra, pairs.record_id_a == ra.a_record_id).join(rb, pairs.record_id_b == rb.b_record_id)
    A = lambda c: F.col("a_" + c)
    B = lambda c: F.col("b_" + c)
    fz = comp["fuzzy_similarity"]
    levels = {
        "first_name": _first_level(A, B, fz),
        "last_name": _last_level(A, B, fz),
        "dob": _dob_level(A, B),
        "gender": _gender_level(A, B),
        "address": _address_level(A, B, comp["address_exact_similarity"], comp["address_fuzzy_similarity"]),
        "phone": _phone_level(A, B),
    }
    for f, e in levels.items():
        p = p.withColumn("lvl_" + f, e)
    fields = list(levels.keys())
    for f in fields:
        wm = F.create_map(*[x for kv in w[f].items() for x in (F.lit(kv[0]), F.lit(float(kv[1])))])
        p = p.withColumn("w_" + f, F.element_at(wm, F.col("lvl_" + f)))
    p = p.withColumn("match_weight", functools.reduce(lambda x, y: x + y, [F.col("w_" + f) for f in fields]))
    p = p.withColumn("prob_score", F.greatest(F.lit(0.0), F.least(F.lit(1.0), (F.col("match_weight") - F.lit(wmin)) / F.lit(wmax - wmin))))
    both_id = _both(A("aadhaar_hash"), B("aadhaar_hash"))
    p = p.withColumn("det_match", F.lit(det_on) & both_id & (A("aadhaar_hash") == B("aadhaar_hash")))
    p = p.withColumn("strong_id_conflict", F.lit(det_on) & both_id & (A("aadhaar_hash") != B("aadhaar_hash")))
    p = p.withColumn("score", F.when(F.col("det_match"), F.greatest(F.col("prob_score"), F.lit(det["score_floor"]))).otherwise(F.col("prob_score")))
    p = p.withColumn("rule_hit", F.when(F.col("det_match"), "DET_AADHAAR").otherwise("PROBABILISTIC"))
    evidence = functools.reduce(lambda x, y: x + y, [(F.col("w_" + f) > 0).cast("int") for f in ("first_name", "last_name", "dob", "address", "phone")])
    p = p.withColumn("evidence_fields", evidence)
    fn_lvl, ln_lvl, dob_lvl = F.col("lvl_first_name"), F.col("lvl_last_name"), F.col("lvl_dob")
    flags = [
        ("DOB_CONFLICT", dob_lvl == "disagree"),
        ("DOB_NOT_EXACT", dob_lvl.isin("transposed", "close")),
        ("DOB_MISSING", (dob_lvl == "missing") & F.lit(bool(sr["require_dob_for_auto"]))),
        ("GENDER_CONFLICT", (F.col("lvl_gender") == "disagree") & F.lit(bool(sr["block_on_gender_conflict"]))),
        ("STRONG_ID_CONFLICT", F.col("strong_id_conflict") & F.lit(bool(sr["block_on_strong_id_conflict"]))),
        ("TWIN_GUARD", F.lit(bool(sr["twin_guard"])) & (dob_lvl == "exact") & ln_lvl.isin("exact", "fuzzy") & (fn_lvl.isin("disagree") | ((fn_lvl == "fuzzy") & (F.lit(bool(sr.get("twin_guard_fuzzy_first_name", False))) | (A("first_soundex") != B("first_soundex")))))),
        ("LOW_EVIDENCE", F.col("evidence_fields") < F.lit(int(sr["min_evidence_fields"]))),
    ]
    if not sr["require_exact_dob_for_auto"]:
        flags = [f for f in flags if f[0] != "DOB_NOT_EXACT"]
    raw_flags = F.array(*[F.when(cond, F.lit(name)) for name, cond in flags])
    raw_flags = F.filter(raw_flags, lambda x: x.isNotNull())
    waivable = det["waivable_flags"]
    waived = F.filter(raw_flags, lambda x: ~x.isin(*waivable)) if waivable else raw_flags
    p = p.withColumn("safety_flags", F.when(F.col("det_match"), waived).otherwise(raw_flags))
    p = p.withColumn("safety_passed", F.size("safety_flags") == 0)
    high, low = th["high"], th["low"]
    base = F.when(F.col("score") >= high, "HIGH").when(F.col("score") >= low, "MEDIUM").otherwise("LOW")
    banded = F.when((base == "HIGH") & ~F.col("safety_passed"), "MEDIUM").otherwise(base)
    if sr["strong_id_conflict_action"] == "force_low":
        banded = F.when(F.col("strong_id_conflict"), "LOW").otherwise(banded)
    band_reason = (
        F.when((base == "HIGH") & ~F.col("safety_passed"), F.concat(F.lit("score>=high but safety failed: "), F.array_join("safety_flags", ",")))
        .when(F.col("strong_id_conflict") & (F.lit(sr["strong_id_conflict_action"]) == "force_low"), "strong id conflict forced LOW")
        .when(base == "HIGH", "score>=high and safety passed")
        .when(base == "MEDIUM", "score between low and high")
        .otherwise("score<low")
    )
    p = p.withColumn("band", banded).withColumn("band_reason", band_reason)
    p = p.withColumn("pair_id", F.sha2(F.concat_ws("||", "record_id_a", "record_id_b"), 256))
    parts = F.array(*[F.struct(F.lit(f).alias("field"), F.col("lvl_" + f).alias("level"), F.round(F.col("w_" + f), 2).alias("weight")) for f in fields])
    p = p.withColumn("field_levels", parts)
    expl = F.array_join(F.transform("field_levels", lambda x: F.concat(x["field"], F.lit("="), x["level"], F.lit(" ("), F.format_number(x["weight"], 1), F.lit(")"))), "; ")
    p = p.withColumn("explanation", F.concat(F.col("rule_hit"), F.lit(" | score="), F.format_number("score", 3), F.lit(" | "), expl, F.lit(" | safety="), F.when(F.col("safety_passed"), "passed").otherwise(F.concat(F.lit("failed:"), F.array_join("safety_flags", ",")))))
    return p.select(
        "pair_id", "record_id_a", "record_id_b", F.col("a_source_system").alias("source_a"), F.col("b_source_system").alias("source_b"),
        "block_rules", "match_weight", "score", "rule_hit", "det_match", "strong_id_conflict", "safety_flags", "safety_passed",
        F.col("band").alias("model_band"), F.col("band_reason").alias("model_band_reason"), "field_levels", "explanation",
    )


def apply_overrides(pairs, overrides):
    p = pairs
    for c in ("override_label", "band", "band_reason"):
        if c in p.columns:
            p = p.drop(c)
    if overrides is None:
        p = p.withColumn("override_label", F.lit(None).cast("string"))
    else:
        p = p.join(overrides.select("pair_id", F.col("label").alias("override_label")), "pair_id", "left")
    p = p.withColumn("band", F.when(F.col("override_label") == "MATCH", "HIGH").when(F.col("override_label") == "NON_MATCH", "LOW").otherwise(F.col("model_band")))
    p = p.withColumn("band_reason", F.when(F.col("override_label") == "MATCH", "validated reviewer decision: MATCH").when(F.col("override_label") == "NON_MATCH", "validated reviewer decision: NON_MATCH").otherwise(F.col("model_band_reason")))
    return p


def run_matching(spark, cfg, std, run_id, overrides=None):
    pairs, stats = candidate_pairs(std, cfg["matching"])
    scored = apply_overrides(score_pairs(spark, pairs, std, cfg), overrides)
    scored = scored.withColumn("run_id", F.lit(run_id)).withColumn("config_hash", F.lit(cfg["hash"])).withColumn("scored_at", F.current_timestamp())
    return scored, stats
