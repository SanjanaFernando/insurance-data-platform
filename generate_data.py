"""Generate a synthetic insurance dataset with deliberate data-quality problems.
Run:  python generate_data.py
Creates CSV files in ./data (customers, policies, claims_part1, claims_part2, customers_changes)."""
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

random.seed(42)
Faker.seed(42)
fake = Faker()
OUT = Path("data")
OUT.mkdir(exist_ok=True)

CITIES = ["Colombo", "Kandy", "Galle", "Jaffna", "Negombo", "Matara"]
POLICY_TYPES = ["Motor", "Health", "Property", "Travel"]
STATUSES = ["OPEN", "APPROVED", "REJECTED", "PAID"]

# ---- customers (500) ----
customers = []
for cid in range(1, 501):
    customers.append({
        "customer_id": cid,
        "full_name": fake.name(),
        "email": fake.email() if random.random() > 0.03 else None,   # 3% missing emails
        "city": random.choice(CITIES),
        "birth_date": fake.date_of_birth(minimum_age=20, maximum_age=75).isoformat(),
        "signup_date": fake.date_between(start_date=date(2023, 1, 1), end_date=date(2025, 6, 30)).isoformat(),
    })
cust = pd.DataFrame(customers)
dupes = cust.sample(10, random_state=1)            # 10 exact duplicate rows
cust = pd.concat([cust, dupes], ignore_index=True).sample(frac=1, random_state=2)
cust.to_csv(OUT / "customers.csv", index=False)

# ---- policies (800) ----
policies = []
for pid in range(1, 801):
    start = fake.date_between(start_date=date(2024, 1, 1), end_date=date(2026, 6, 30))
    policies.append({
        "policy_id": pid,
        "customer_id": random.randint(1, 500),
        "policy_type": random.choice(POLICY_TYPES),
        "start_date": start.isoformat(),
        "end_date": (start + timedelta(days=365)).isoformat(),
        "annual_premium": round(random.uniform(150, 2500), 2),
        "broker_id": random.randint(1, 20),
    })
pd.DataFrame(policies).to_csv(OUT / "policies.csv", index=False)

# ---- claims (3000), two files so we can demo incremental loads ----
claims = []
for cid in range(1, 3001):
    pol = random.choice(policies)
    start = date.fromisoformat(pol["start_date"])
    claim_date = start + timedelta(days=random.randint(1, 300))
    claim_date = min(claim_date, date(2026, 10, 31))
    amount = round(random.lognormvariate(7.0, 0.8), 2)
    status = random.choices(STATUSES, weights=[2, 4, 2, 4])[0]
    claims.append({
        "claim_id": cid, "policy_id": pol["policy_id"], "claim_date": claim_date.isoformat(),
        "claim_amount": amount, "status": status,
    })
cl = pd.DataFrame(claims)
# dirty data
cl.loc[cl.sample(30, random_state=3).index, "claim_amount"] = -1 * cl["claim_amount"]      # negative amounts
cl.loc[cl.sample(25, random_state=4).index, "status"] = None                               # missing status
cl.loc[cl.sample(40, random_state=5).index, "status"] = cl["status"].str.lower()            # inconsistent case
cl = cl.sort_values("claim_date").reset_index(drop=True)
cut = int(len(cl) * 0.7)
part1, part2 = cl.iloc[:cut].copy(), cl.iloc[cut:].copy()
part2 = pd.concat([part2, part1.sample(20, random_state=6)], ignore_index=True)           # 20 re-sent rows (overlap)
part1.to_csv(OUT / "claims_part1.csv", index=False)
part2.to_csv(OUT / "claims_part2.csv", index=False)

# ---- customer changes (for SCD Type 2): 40 customers moved city ----
changed = cust.drop_duplicates("customer_id").sample(40, random_state=7).copy()
changed["city"] = changed["city"].apply(lambda c: random.choice([x for x in CITIES if x != c]))
changed.to_csv(OUT / "customers_changes.csv", index=False)

print({f.name: len(pd.read_csv(f)) for f in sorted(OUT.glob("*.csv"))})
