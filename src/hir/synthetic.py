import random
from datetime import date, datetime, timedelta

from .config import source_names
from .store import fmt, source_table_name

MALE = ["RAJESH", "VENKATESH", "SURESH", "RAMESH", "KRISHNAN", "SUBRAMANIAN", "MOHAMMED", "DEEPAK", "ANAND", "KARTHIK", "SATHISH", "MANIKANDAN", "ARJUN", "ARUN", "VIJAY", "AJAY", "SANJAY", "KIRAN", "NAVEEN", "PRAKASH", "GANESH", "HARISH", "MAHESH", "ASHOK", "DINESH", "RAHUL", "ROHIT", "VIKRAM", "SUNIL", "GOPAL", "BALAJI", "SRINIVASAN", "MURUGAN", "SENTHIL", "RAVI", "SIVA", "BHARATH", "PRANAV", "IMRAN", "FAISAL"]
FEMALE = ["LAKSHMI", "PRIYA", "ANITHA", "KAVITHA", "MEENA", "DIVYA", "SANGEETHA", "REVATHI", "NITHYA", "DEEPA", "SHANTHI", "RADHA", "SUNITHA", "POOJA", "SWATHI", "ANJALI", "FATIMA", "AYESHA", "JANANI", "MYTHILI", "SARANYA", "VIDYA", "GEETHA", "UMA", "LATHA", "BHAVANI"]
LAST = ["KUMAR", "SHARMA", "IYER", "IYENGAR", "NAIR", "REDDY", "NAIDU", "PILLAI", "MENON", "RAO", "GUPTA", "SINGH", "KHAN", "AHMED", "DAS", "BANERJEE", "CHATTERJEE", "PATEL", "SHAH", "MEHTA", "JOSHI", "KULKARNI", "DESAI", "MURTHY", "SUBRAMANIAM", "CHANDRAN", "RAMAN", "VENKATESAN", "SELVAM", "RAJAN", "NATARAJAN", "BALASUBRAMANIAN", "KRISHNAMURTHY", "GOPALAN", "RAMASWAMY", "SUNDARAM", "THOMAS", "FERNANDES", "VARMA", "BHAT"]
MIDDLE = ["K", "S", "R", "M", "KUMAR", "RAJ", "LAL", "DEVI", "P", "V"]
CITIES = [("CHENNAI", "TAMIL NADU", 600001, 99), ("TAMBARAM", "TAMIL NADU", 600045, 5), ("BENGALURU", "KARNATAKA", 560001, 99), ("HYDERABAD", "TELANGANA", 500001, 99), ("COIMBATORE", "TAMIL NADU", 641001, 60), ("MUMBAI", "MAHARASHTRA", 400001, 99), ("PUNE", "MAHARASHTRA", 411001, 60), ("KOCHI", "KERALA", 682001, 40), ("DELHI", "DELHI", 110001, 99), ("KOLKATA", "WEST BENGAL", 700001, 80)]
STREETS = ["GANDHI", "NEHRU", "ANNA", "TEMPLE", "LAKE VIEW", "MARKET", "STATION", "GREEN PARK", "RAJAJI", "BHARATHI", "KAMARAJ", "SARDAR PATEL", "TAGORE", "NETAJI", "MG"]
SUFFIX = ["ROAD", "STREET", "MAIN ROAD", "CROSS STREET", "NAGAR", "COLONY"]
AREAS = ["ANNA NAGAR", "ADYAR", "VELACHERY", "T NAGAR", "KOTHRUD", "WHITEFIELD", "BANJARA HILLS", "SALT LAKE", "DWARKA", "ANDHERI", "RS PURAM", "EDAPPALLY"]
GEN_NICK = {"RAJESH": ["RAJ", "RAJU"], "VENKATESH": ["VENKY", "VENKAT"], "KRISHNAN": ["KRISH"], "MOHAMMED": ["MOHD", "MD"], "KARTHIK": ["KARTHI"], "SUBRAMANIAN": ["SUBBU"], "LAKSHMI": ["LAXMI"], "DEEPAK": ["DEEPU"], "SATHISH": ["SATISH"], "SURESH": ["SURI"]}
ABBREV = [("ROAD", "RD"), ("STREET", "ST"), ("NAGAR", "NGR"), ("COLONY", "COL"), ("CROSS", "CRS")]


class Gen:
    def __init__(self, scfg):
        self.c = scfg
        self.r = random.Random(scfg["seed"])
        self.counters = {}
        self.used_aadhaar = set()

    def p(self, rate):
        return self.r.random() < rate

    def aadhaar(self):
        while True:
            n = str(self.r.randint(2, 9)) + "".join(str(self.r.randint(0, 9)) for _ in range(11))
            if n not in self.used_aadhaar:
                self.used_aadhaar.add(n)
                return n

    def phone(self):
        return str(self.r.randint(6, 9)) + "".join(str(self.r.randint(0, 9)) for _ in range(9))

    def address(self, city=None):
        c = city or self.r.choice(CITIES)
        pin = c[2] + self.r.randint(0, c[3])
        line = f"{self.r.randint(1, 240)} {self.r.choice(STREETS)} {self.r.choice(SUFFIX)} {self.r.choice(AREAS)}"
        return {"address_line": line, "city": c[0], "state": c[1], "pincode": str(pin)}

    def person(self, pid):
        gender = self.r.choice(["M", "F"])
        first = self.r.choice(MALE if gender == "M" else FEMALE)
        dob = date(1935, 1, 1) + timedelta(days=self.r.randint(0, 365 * 88))
        d = {"pid": pid, "first": first, "middle": self.r.choice(MIDDLE) if self.p(0.4) else None, "last": self.r.choice(LAST), "gender": gender, "dob": dob, "phone": self.phone(), "aadhaar": self.aadhaar()}
        d.update(self.address())
        return d

    def build_persons(self):
        n = self.c["n_persons"]
        persons = []
        for i in range(n):
            persons.append(self.person(f"P{i:06d}"))
        extra = []
        k = n
        for base in persons:
            if self.p(self.c["twin_rate"]):
                t = dict(base)
                t["pid"] = f"P{k:06d}"
                k += 1
                t["aadhaar"] = self.aadhaar()
                if self.p(0.5):
                    t["first"] = self.similar_name(base["first"])
                else:
                    t["first"] = self.r.choice(MALE if base["gender"] == "M" else FEMALE)
                    if t["first"] == base["first"]:
                        t["first"] = self.similar_name(base["first"])
                t["middle"] = base["middle"]
                extra.append(t)
            elif self.p(self.c["family_shared_address_rate"]):
                t = self.person(f"P{k:06d}")
                k += 1
                for f in ("last", "address_line", "city", "state", "pincode", "phone"):
                    t[f] = base[f]
                extra.append(t)
            elif self.p(self.c["same_name_different_person_rate"]):
                t = self.person(f"P{k:06d}")
                k += 1
                t["first"], t["last"], t["gender"] = base["first"], base["last"], base["gender"]
                extra.append(t)
        return persons + extra

    def similar_name(self, name):
        pool = MALE + FEMALE
        best = [x for x in pool if x != name and x[0] == name[0] and abs(len(x) - len(name)) <= 1]
        if len(name) > 3 and (not best or self.p(0.5)):
            i = self.r.randint(1, len(name) - 2)
            return name[:i] + name[i + 1:]
        return self.r.choice(best) if best else name + "A"

    def typo(self, s):
        if not s or len(s) < 4:
            return s
        i = self.r.randint(1, len(s) - 2)
        op = self.r.choice(["swap", "drop", "sub", "dup"])
        if op == "swap":
            return s[:i] + s[i + 1] + s[i] + s[i + 2:]
        if op == "drop":
            return s[:i] + s[i + 1:]
        if op == "sub":
            return s[:i] + self.r.choice("AEIOUNRSTL") + s[i + 1:]
        return s[:i] + s[i] + s[i:]

    def vary_address(self, line):
        out = line
        if self.p(0.5):
            for a, b in ABBREV:
                out = out.replace(a, b)
        if self.p(0.3):
            parts = out.split(" ")
            out = " ".join(parts[:-2]) if len(parts) > 4 else out
        if self.p(0.3):
            out = self.typo(out)
        if self.p(0.3):
            out = "NO " + out
        return out

    def phone_variant(self, ph):
        return self.r.choice([ph, ph, "+91" + ph, "+91 " + ph[:5] + " " + ph[5:], "0" + ph, ph[:5] + "-" + ph[5:]])

    def case(self, s):
        if s is None:
            return None
        return self.r.choice([s, s, s.title(), s.lower()])

    def record_view(self, person, system, others):
        c = self.c
        first, last, middle = person["first"], person["last"], person["middle"]
        if self.p(c["name_typo"]):
            if self.p(0.5):
                first = self.typo(first)
            else:
                last = self.typo(last)
        if self.p(c["nickname"]):
            first = self.r.choice(GEN_NICK[first]) if first in GEN_NICK else first[:4]
        if self.p(c["initial_only"]):
            first = first[0]
        if self.p(c["name_swap"]):
            first, last = last, first
        dob = person["dob"]
        if self.p(c["dob_transpose"]) and dob.day <= 12 and dob.day != dob.month:
            dob = date(dob.year, dob.day, dob.month)
        elif self.p(c["dob_typo"]):
            dob = dob + timedelta(days=self.r.choice([-1, 1, 365, -365]))
        if self.p(c["dob_missing"].get(system, 0)):
            dob = None
        gender = person["gender"]
        if self.p(c["gender_error"]):
            gender = "F" if gender == "M" else "M"
        if self.p(c["gender_missing"]):
            gender = None
        ph = person["phone"] if not self.p(0.08) else self.phone()
        phone = None if self.p(c["phone_missing"]) else self.phone_variant(ph)
        addr = {k: person[k] for k in ("address_line", "city", "state", "pincode")}
        if self.p(c["address_variation"]):
            addr["address_line"] = self.vary_address(addr["address_line"])
        miss = c["address_missing"] * (5 if system == "LIS" else 1)
        if self.p(miss):
            addr["address_line"] = None
        if self.p(0.04):
            addr["pincode"] = None
        aad = None
        if self.p(c["aadhaar_presence"].get(system, 0)):
            aad = person["aadhaar"]
            if self.p(c["aadhaar_typo_rate"]):
                i = self.r.randint(0, 11)
                aad = aad[:i] + str((int(aad[i]) + self.r.randint(1, 9)) % 10) + aad[i + 1:]
            elif others and self.p(c["aadhaar_shared_error_rate"] * 10):
                aad = self.r.choice(others)["aadhaar"]
        ts = datetime(2023, 1, 1) + timedelta(seconds=self.r.randint(0, 3 * 365 * 86400))
        return {"first": first, "middle": middle, "last": last, "dob": dob, "gender": gender, "phone": phone, "aadhaar": aad, "updated": ts, **addr}

    def fmt_aadhaar(self, a):
        if a is None:
            return None
        return self.r.choice([a, a[:4] + " " + a[4:8] + " " + a[8:], a[:4] + "-" + a[4:8] + "-" + a[8:]])

    def next_id(self, system, prefix):
        self.counters[system] = self.counters.get(system, 0) + 1
        return f"{prefix}{self.counters[system]:06d}"

    def to_source(self, system, v):
        ts = v["updated"].strftime("%Y-%m-%d %H:%M:%S")
        cs = self.case
        name_parts = [v["first"], v["middle"], v["last"]]
        full_fl = " ".join(x for x in name_parts if x)
        if system == "EHR":
            return {"patient_id": self.next_id(system, "EHR"), "first_name": cs(v["first"]), "middle_name": v["middle"], "last_name": cs(v["last"]), "dob": v["dob"].strftime("%Y-%m-%d") if v["dob"] else None, "sex": v["gender"], "address_line": v["address_line"], "city": v["city"], "state": v["state"], "pincode": v["pincode"], "mobile": v["phone"], "aadhaar_no": self.fmt_aadhaar(v["aadhaar"]), "last_updated": ts}
        if system == "LIS":
            nm = f"{v['last']}, {v['first']}"
            return {"lab_patient_no": self.next_id(system, "LAB"), "patient_name": cs(nm), "date_of_birth": v["dob"].strftime("%d-%m-%Y") if v["dob"] else None, "gender": {"M": "Male", "F": "Female"}.get(v["gender"]), "street": v["address_line"], "city": v["city"], "state": v["state"], "pin": v["pincode"], "contact_no": v["phone"], "registered_on": ts}
        if system == "RIS":
            line = None
            if v["address_line"]:
                line = f"{v['address_line']}, {v['city']} {v['pincode'] or ''}".strip()
            return {"ris_pid": self.next_id(system, "RIS"), "given_name": cs(v["first"]), "family_name": cs(v["last"]), "birthdate": v["dob"].strftime("%d/%m/%Y") if v["dob"] else None, "sex_code": {"M": "1", "F": "2"}.get(v["gender"]), "address_full": line, "phone": v["phone"], "study_date": ts}
        if system == "PHARMACY":
            return {"rx_customer_id": self.next_id(system, "RX"), "customer_name": cs(full_fl), "dob": v["dob"].strftime("%Y-%m-%d") if v["dob"] else None, "gender": v["gender"], "address": v["address_line"], "city": v["city"], "zip": v["pincode"], "phone_number": v["phone"], "aadhaar": self.fmt_aadhaar(v["aadhaar"]), "last_dispensed": ts}
        line = None
        if v["address_line"]:
            line = f"{v['address_line']}, {v['city']}, {v['state']} {v['pincode'] or ''}".strip()
        return {"account_no": self.next_id(system, "ACC"), "payer_name": cs(full_fl).upper() if self.p(0.5) else cs(full_fl), "dob": v["dob"].strftime("%d.%m.%Y") if v["dob"] else None, "gender": v["gender"], "billing_address": line, "phone": v["phone"], "aadhaar_number": self.fmt_aadhaar(v["aadhaar"]), "last_invoice_ts": ts}


ID_COLUMN = {"EHR": "patient_id", "LIS": "lab_patient_no", "RIS": "ris_pid", "PHARMACY": "rx_customer_id", "BILLING": "account_no"}


def generate(cfg):
    scfg = cfg["pipeline"]["synthetic"]
    g = Gen(scfg)
    persons = g.build_persons()
    systems = [s for s in source_names(cfg) if s in ID_COLUMN]
    data = {s: [] for s in systems}
    for person in persons:
        for system in systems:
            if not g.p(scfg["system_coverage"].get(system, 0.5)):
                continue
            n = 2 if g.p(scfg["duplicate_within_system_rate"]) else 1
            for _ in range(n):
                pool = g.r.sample(persons, 3) if g.p(0.05) else None
                v = g.record_view(person, system, pool)
                row = g.to_source(system, v)
                data[system].append(row)
    return data, persons


def to_spark(spark, cfg, data):
    f = fmt(cfg)
    out = {}
    for system, rows in data.items():
        cols = list(rows[0].keys())
        df = spark.createDataFrame([tuple(r.get(c) for c in cols) for r in rows], schema=", ".join(f"{c} string" for c in cols))
        w = df.write.format(f).mode("overwrite")
        if f == "delta":
            w = w.option("overwriteSchema", "true")
        w.saveAsTable(source_table_name(cfg, system))
        out[system] = df.count()
    return out
