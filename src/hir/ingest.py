import functools

from pyspark.sql import functions as F

from . import store
from .config import source_names
from .privacy import strong_id_enabled


def _nz(c):
    c = F.trim(c.cast("string"))
    return F.when(c == "", None).otherwise(c)


def _strip_titles(col, titles):
    pat = r"\b(" + "|".join(titles) + r")\b\.?"
    return F.trim(F.regexp_replace(F.regexp_replace(F.upper(col), pat, ""), r"\s+", " "))


def _name_cols(df, ncfg, titles):
    mode = ncfg["mode"]
    if mode == "split":
        first = _nz(F.col(ncfg["first"]))
        middle = _nz(F.col(ncfg["middle"])) if ncfg.get("middle") else F.lit(None).cast("string")
        last = _nz(F.col(ncfg["last"]))
        return first, middle, last
    full = _strip_titles(_nz(F.col(ncfg["full"])), titles)
    if mode == "last_comma_first":
        parts = F.split(full, ",")
        last_c = F.when(F.size(parts) > 1, F.trim(parts[0]))
        rest = F.when(F.size(parts) > 1, F.trim(parts[1]))
        rt = F.split(rest, " ")
        first_c = F.when(rest.isNotNull(), rt[0])
        middle_c = F.when(F.size(rt) > 1, F.array_join(F.slice(rt, 2, F.size(rt) - 1), " "))
        toks = F.split(full, " ")
        fb_first = F.when(F.size(parts) <= 1, toks[0])
        fb_last = F.when((F.size(parts) <= 1) & (F.size(toks) > 1), toks[F.size(toks) - 1])
        return _nz(F.coalesce(first_c, fb_first)), _nz(middle_c), _nz(F.coalesce(last_c, fb_last))
    toks = F.split(full, " ")
    n = F.size(toks)
    first = F.when(n >= 1, toks[0])
    last = F.when(n > 1, toks[n - 1])
    middle = F.when(n > 2, F.array_join(F.slice(toks, 2, n - 2), " "))
    return _nz(first), _nz(middle), _nz(last)


def _address_cols(acfg):
    if acfg["mode"] == "split":
        g = lambda k: _nz(F.col(acfg[k])) if acfg.get(k) else F.lit(None).cast("string")
        return g("line"), g("city"), g("state"), g("pincode")
    raw = _nz(F.col(acfg["line"]))
    pin = F.regexp_extract(raw, r"(\d{6})", 1)
    pin = F.when(pin != "", pin)
    stripped = F.regexp_replace(raw, r"\d{6}", "")
    arr = F.transform(F.split(stripped, ","), lambda x: F.trim(x))
    arr = F.filter(arr, lambda x: x != "")
    trailing = [p for p in acfg.get("parts", ["line", "city"])[1:]]
    n = F.size(arr)
    out = {}
    for j, name in enumerate(trailing):
        idx = n - len(trailing) + j
        out[name] = F.when(idx >= 1, F.element_at(arr, idx + 1))
    line = F.when(n - len(trailing) >= 1, F.array_join(F.slice(arr, 1, n - len(trailing)), ", "))
    return _nz(line), _nz(out.get("city", F.lit(None))), _nz(out.get("state", F.lit(None))), pin


def canonical(spark, cfg, system, salt):
    s = cfg["sources"]["sources"][system]
    raw = spark.table(store.source_table_name(cfg, system))
    titles = cfg["sources"]["standardization"]["name_titles"]
    first, middle, last = _name_cols(raw, s["name"], titles)
    line, city, state, pin = _address_cols(s["address"])
    gmap = s["gender"]["map"]
    graw = F.upper(_nz(F.col(s["gender"]["column"])))
    gender = functools.reduce(lambda acc, kv: acc.when(graw == kv[0], F.lit(kv[1])), gmap.items(), F.when(F.lit(False), None))
    dob_expr = f"try_to_timestamp({s['dob']['column']}, '{s['dob']['format']}')"
    upd_expr = f"try_to_timestamp({s['updated_at']['column']}, '{s['updated_at']['format']}')"
    aad_col = s.get("strong_ids", {}).get("aadhaar")
    if aad_col and strong_id_enabled(cfg, "aadhaar"):
        digits = F.regexp_replace(F.col(aad_col).cast("string"), r"\D", "")
        valid = (F.length(digits) == 12) & digits.rlike(r"^[2-9]")
        a_hash = F.when(valid, F.sha2(F.concat(F.lit(salt), digits), 256))
        a_last4 = F.when(valid, F.substring(digits, -4, 4))
    else:
        a_hash = F.lit(None).cast("string")
        a_last4 = F.lit(None).cast("string")
    idc = F.col(s["id_column"]).cast("string")
    df = raw.select(
        F.lit(system).alias("source_system"),
        idc.alias("source_patient_id"),
        F.concat(F.lit(system), F.lit(":"), idc).alias("record_id"),
        first.alias("first_raw"), middle.alias("middle_raw"), last.alias("last_raw"),
        F.expr(dob_expr).cast("date").alias("dob"),
        _nz(F.col(s["dob"]["column"])).alias("dob_raw"),
        gender.alias("gender_raw"),
        line.alias("address_line_raw"), city.alias("city_raw"), state.alias("state_raw"), pin.alias("pincode_raw"),
        _nz(F.col(s["phone"])).alias("phone_raw") if s.get("phone") else F.lit(None).cast("string").alias("phone_raw"),
        a_hash.alias("aadhaar_hash"), a_last4.alias("aadhaar_last4"),
        F.expr(upd_expr).alias("updated_at"),
        F.lit(s["table"]).alias("source_table"),
    )
    return df.withColumn("source_row_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns if c not in ("source_table",)]), 256))


def run(spark, cfg, run_id, salt, batch_id=None):
    parts = [canonical(spark, cfg, s, salt) for s in source_names(cfg)]
    df = functools.reduce(lambda a, b: a.unionByName(b), parts)
    df = df.withColumn("ingest_run_id", F.lit(run_id)).withColumn("ingested_at", F.current_timestamp())
    store.write(spark, cfg, df, "bronze")
    return store.read(spark, cfg, "bronze")
