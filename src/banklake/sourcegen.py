"""Synthetic content for the source databases: extra columns on the six core tables and the source-only tables.
Everything is fictional and deterministic for a given seed. Flag columns get a little whitespace / lower-case noise,
like real upstream extracts, so the Bronze TRIM + UPPER step has something to do."""
import numpy as np
import pandas as pd

from .schemas import CALENDAR, SOURCE_EXTRA_COLUMNS, SOURCE_ONLY, SOURCES

STATES = ["Maharashtra", "Karnataka", "Tamil Nadu", "Delhi", "Gujarat", "West Bengal", "Telangana", "Rajasthan", "Kerala", "Punjab"]
CITY_OF = {"Maharashtra": "Mumbai", "Karnataka": "Bengaluru", "Tamil Nadu": "Chennai", "Delhi": "New Delhi", "Gujarat": "Ahmedabad",
           "West Bengal": "Kolkata", "Telangana": "Hyderabad", "Rajasthan": "Jaipur", "Kerala": "Kochi", "Punjab": "Ludhiana"}
OCC = ["Salaried", "Self-employed", "Business owner", "Professional", "Retired", "Student", "Homemaker"]


def _pick(rng, values, n, p=None):
    return rng.choice(np.array(values, dtype=object), n, p=p)


def _noise(rng, s, rate=0.04):
    """A few values arrive padded or in lower case, as in real extracts."""
    s = pd.Series(s, dtype=object).copy()
    hit = rng.random(len(s)) < rate
    kind = rng.random(len(s))
    for i in np.flatnonzero(hit):
        v = s.iat[i]
        if isinstance(v, str):
            s.iat[i] = f" {v.lower()} " if kind[i] < 0.5 else f"{v.lower()}  "
    return s


def _dates(rng, n, start, end):
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    return (s + pd.to_timedelta(rng.integers(0, (e - s).days + 1, n), unit="D")).strftime("%Y-%m-%d")


def _ids(prefix, n, width):
    return [f"{prefix}{i:0{width}d}" for i in range(1, n + 1)]


def _digits(rng, n, k):
    return pd.Series(rng.integers(10 ** (k - 1), 10 ** k, n).astype(str))


def extend_core(base: dict, rng) -> None:
    """Add the extra source columns to the six core tables (in place)."""
    c = base["customer"]
    n = len(c)
    c["date_of_birth"] = _dates(rng, n, "1950-01-01", "2005-12-31")
    c["gender"] = _noise(rng, _pick(rng, ["Male", "Female", "Other"], n, [.49, .49, .02]))
    c["marital_status"] = _noise(rng, _pick(rng, ["Single", "Married", "Divorced", "Widowed"], n, [.3, .6, .06, .04]))
    c["occupation"] = _pick(rng, OCC, n)
    c["annual_income"] = (rng.lognormal(13.4, .55, n).round(-3)).astype(int).astype(str)
    c["risk_rating"] = _noise(rng, _pick(rng, ["Low", "Medium", "High"], n, [.7, .22, .08]))
    c["kyc_status"] = _noise(rng, _pick(rng, ["Verified", "Pending", "Expired"], n, [.86, .09, .05]))
    c["preferred_channel"] = _noise(rng, _pick(rng, ["Mobile", "Branch", "Internet", "Call centre"], n, [.5, .2, .25, .05]))
    c["customer_since"] = _dates(rng, n, "2005-01-01", "2024-12-31")
    for col in ("segment",):
        c[col] = _noise(rng, c[col], .03)

    a = base["account"]
    n = len(a)
    a["currency"] = _noise(rng, _pick(rng, ["INR", "USD", "EUR"], n, [.96, .025, .015]))
    a["account_status"] = _noise(rng, _pick(rng, ["Active", "Dormant", "Frozen", "Closed"], n, [.9, .06, .01, .03]))
    a["product_code"] = _noise(rng, _pick(rng, ["SB-REG", "SB-PLUS", "CA-BIZ", "SAL-PRO", "SB-KIDS"], n, [.45, .2, .15, .15, .05]))
    a["interest_rate"] = _pick(rng, ["2.70", "3.00", "3.50", "0.00", "4.00"], n)
    a["overdraft_limit"] = _pick(rng, ["0", "0", "0", "25000", "100000", "500000"], n)
    a["last_txn_date"] = _dates(rng, n, "2026-01-01", "2026-07-31")
    a["nominee_flag"] = _pick(rng, ["Y", "N"], n, [.8, .2])

    t = base["transaction"]
    n = len(t)
    t["channel"] = _noise(rng, _pick(rng, ["UPI", "ATM", "NEFT", "IMPS", "POS", "Branch", "NetBanking"], n, [.34, .12, .1, .1, .2, .04, .1]))
    t["currency"] = _noise(rng, _pick(rng, ["INR"], n))
    t["merchant_category"] = _pick(rng, ["Groceries", "Fuel", "Dining", "Travel", "Utilities", "Shopping", "Healthcare", "Transfer", "Salary"], n)
    t["balance_after"] = (rng.gamma(2.0, 45000, n)).round(2).astype(str)
    t["reference_no"] = "REF" + _digits(rng, n, 10)
    t["txn_status"] = _noise(rng, _pick(rng, ["Posted", "Pending", "Reversed", "Failed"], n, [.93, .03, .02, .02]))

    b = base["branch"]
    n = len(b)
    st = _pick(rng, STATES, n)
    b["state"] = _noise(rng, st)
    b["city"] = [CITY_OF[x] for x in st]
    b["ifsc_code"] = ["BKLK0" + f"{i:06d}" for i in range(1, n + 1)]
    b["opened_date"] = _dates(rng, n, "1995-01-01", "2023-12-31")
    b["branch_type"] = _noise(rng, _pick(rng, ["Metro", "Urban", "Semi-urban", "Rural"], n, [.25, .35, .25, .15]))
    b["employee_count"] = rng.integers(6, 80, n).astype(str)

    d = base["deposit"]
    n = len(d)
    d["deposit_type"] = _noise(rng, _pick(rng, ["Fixed", "Recurring", "Tax-saver", "Flexi"], n, [.55, .2, .15, .1]))
    d["interest_rate"] = _pick(rng, ["6.50", "6.75", "7.00", "7.25", "7.50"], n)
    d["tenure_months"] = _pick(rng, ["6", "12", "24", "36", "60"], n)
    d["deposit_status"] = _noise(rng, _pick(rng, ["Active", "Matured", "Closed"], n, [.7, .22, .08]))
    d["opened_date"] = _dates(rng, n, "2022-01-01", "2026-06-30")
    d["auto_renew"] = _pick(rng, ["Y", "N"], n, [.4, .6])

    ln = base["loan"]
    n = len(ln)
    ln["loan_type"] = _noise(rng, _pick(rng, ["Home", "Personal", "Auto", "Education", "Business"], n, [.3, .3, .15, .1, .15]))
    ln["tenure_months"] = _pick(rng, ["12", "24", "36", "60", "120", "240"], n)
    amt = pd.to_numeric(ln["loan_amount"], errors="coerce").fillna(0)
    ln["emi_amount"] = (amt / pd.to_numeric(ln["tenure_months"]) * 1.12).round(2).astype(str)
    ln["disbursed_date"] = _dates(rng, n, "2020-01-01", "2026-06-30")
    ln["loan_status"] = _noise(rng, _pick(rng, ["Current", "Overdue", "Closed", "Written-off"], n, [.82, .1, .06, .02]))
    ln["outstanding_amount"] = (amt * rng.uniform(.1, 1.0, n)).round(2).astype(str)
    ln["collateral_type"] = _noise(rng, _pick(rng, ["Property", "Vehicle", "Gold", "None"], n, [.3, .15, .1, .45]))
    for e, cols in SOURCE_EXTRA_COLUMNS.items():
        missing = [x for x in cols if x not in base[e].columns]
        assert not missing, (e, missing)


def source_only_tables(base: dict, rng) -> dict:
    """Banking tables that only exist in the source systems (cards, KYC, addresses, CRM interactions, repayments, beneficiaries)."""
    cust = base["customer"]["customer_id"].dropna().astype(str).to_numpy()
    cust_names = base["customer"]["customer_name"].dropna().astype(str).to_numpy()
    acct = base["account"]["account_id"].dropna().astype(str).to_numpy()
    loan = base["loan"]["loan_id"].dropna().astype(str).to_numpy()
    out = {}
    stamp = "2026-07-01 00:00:00"

    n = int(len(acct) * 1.2)
    out["card"] = pd.DataFrame({
        "card_id": _ids("CRD", n, 8), "account_id": rng.choice(acct, n), "card_number": "4" + _digits(rng, n, 15),
        "card_type": _noise(rng, _pick(rng, ["Debit", "Credit", "Prepaid"], n, [.7, .25, .05])),
        "network": _noise(rng, _pick(rng, ["Visa", "Mastercard", "RuPay"], n, [.35, .25, .4])),
        "card_status": _noise(rng, _pick(rng, ["Active", "Blocked", "Expired", "Hotlisted"], n, [.86, .05, .07, .02])),
        "issued_date": _dates(rng, n, "2021-01-01", "2026-06-30"), "expiry_date": _dates(rng, n, "2026-08-01", "2031-12-31"),
        "daily_limit": _pick(rng, ["25000", "50000", "100000", "200000"], n), "contactless_enabled": _pick(rng, ["Y", "N"], n, [.75, .25]),
    })
    n = len(cust)
    out["customer_kyc"] = pd.DataFrame({
        "kyc_id": _ids("KYC", n, 8), "customer_id": cust,
        "id_type": _noise(rng, _pick(rng, ["PAN", "Passport", "Voter ID", "Driving licence"], n, [.55, .15, .15, .15])),
        "id_number": _pick(rng, list("ABCDEFGHJK"), n) + _digits(rng, n, 9),
        "verification_status": _noise(rng, _pick(rng, ["Verified", "Pending", "Rejected", "Expired"], n, [.85, .08, .02, .05])),
        "verified_on": _dates(rng, n, "2023-01-01", "2026-07-31"), "risk_score": rng.integers(1, 100, n).astype(str),
        "pep_flag": _noise(rng, _pick(rng, ["N", "Y"], n, [.985, .015])), "document_expiry": _dates(rng, n, "2026-09-01", "2034-12-31"),
    })
    n = int(len(cust) * 1.3)
    stt = _pick(rng, STATES, n)
    out["customer_address"] = pd.DataFrame({
        "address_id": _ids("ADR", n, 8), "customer_id": rng.choice(cust, n),
        "address_type": _noise(rng, _pick(rng, ["Residential", "Office", "Permanent"], n, [.6, .2, .2])),
        "address_line": [f"{h}, {s} Nagar, Sector {k}" for h, s, k in zip(rng.integers(1, 400, n), _pick(rng, ["Shanti", "Gandhi", "Nehru", "Lake View", "Green Park"], n), rng.integers(1, 40, n))],
        "city": [CITY_OF[x] for x in stt], "state": _noise(rng, stt), "pincode": _digits(rng, n, 6), "is_primary": _pick(rng, ["Y", "N"], n, [.7, .3]),
    })
    n = len(cust) * 3
    out["customer_interaction"] = pd.DataFrame({
        "interaction_id": _ids("INT", n, 9), "customer_id": rng.choice(cust, n),
        "channel": _noise(rng, _pick(rng, ["Call", "Email", "Chat", "Branch visit", "App"], n, [.3, .15, .2, .15, .2])),
        "topic": _pick(rng, ["Card issue", "Loan enquiry", "Statement request", "Complaint", "KYC update", "Fund transfer", "Account opening"], n),
        "outcome": _noise(rng, _pick(rng, ["Resolved", "Escalated", "Follow-up", "Closed"], n, [.65, .08, .17, .1])),
        "interaction_date": _dates(rng, n, "2026-04-01", "2026-07-31"), "duration_sec": rng.integers(30, 1800, n).astype(str),
        "agent_id": "AG" + _digits(rng, n, 4),
    })
    n = len(loan) * 6
    due = pd.to_datetime(_dates(rng, n, "2026-01-01", "2026-07-31"))
    emi = rng.uniform(4000, 90000, n).round(2)
    paid_ok = rng.random(n) < .88
    out["loan_repayment"] = pd.DataFrame({
        "repayment_id": _ids("RPY", n, 9), "loan_id": rng.choice(loan, n), "due_date": due.strftime("%Y-%m-%d"),
        "paid_date": np.where(paid_ok, (due + pd.to_timedelta(rng.integers(0, 6, n), unit="D")).strftime("%Y-%m-%d"), None),
        "amount_due": emi.astype(str), "amount_paid": np.where(paid_ok, emi, 0).astype(str),
        "repayment_status": _noise(rng, np.where(paid_ok, "Paid", _pick(rng, ["Overdue", "Partial"], n))),
        "payment_mode": _noise(rng, _pick(rng, ["Auto-debit", "UPI", "Cheque", "NetBanking"], n, [.6, .2, .05, .15])),
        "penalty_amount": np.where(paid_ok, 0, rng.integers(100, 2500, n)).astype(str),
    })
    n = int(len(cust) * 1.5)
    out["payment_beneficiary"] = pd.DataFrame({
        "beneficiary_id": _ids("BEN", n, 8), "customer_id": rng.choice(cust, n), "beneficiary_name": rng.choice(cust_names, n),
        "beneficiary_account": _digits(rng, n, 12), "bank_ifsc": "BKLK0" + _digits(rng, n, 6).str[:6],
        "beneficiary_status": _noise(rng, _pick(rng, ["Active", "Inactive", "Blocked"], n, [.9, .08, .02])),
        "added_on": _dates(rng, n, "2023-01-01", "2026-07-31"), "daily_limit": _pick(rng, ["50000", "100000", "200000"], n),
    })
    tx = base["transaction"]["transaction_id"].dropna().astype(str).to_numpy()
    n = len(acct) // 2
    out["standing_instruction"] = pd.DataFrame({
        "si_id": _ids("SI", n, 8), "account_id": rng.choice(acct, n), "beneficiary_account": _digits(rng, n, 12),
        "amount": rng.integers(500, 80000, n).astype(str), "frequency": _noise(rng, _pick(rng, ["Monthly", "Quarterly", "Weekly"], n, [.8, .15, .05])),
        "start_date": _dates(rng, n, "2023-01-01", "2026-06-30"), "end_date": _dates(rng, n, "2026-12-01", "2031-12-31"),
        "si_status": _noise(rng, _pick(rng, ["Active", "Paused", "Cancelled"], n, [.85, .08, .07])), "next_run_date": _dates(rng, n, "2026-08-01", "2026-12-31"),
    })
    n = len(acct) * 2
    out["cheque_register"] = pd.DataFrame({
        "cheque_id": _ids("CHQ", n, 9), "account_id": rng.choice(acct, n), "cheque_number": _digits(rng, n, 6),
        "amount": rng.integers(1000, 500000, n).astype(str), "payee_name": rng.choice(cust_names, n),
        "issue_date": _dates(rng, n, "2024-10-01", "2026-12-31"), "clearing_date": _dates(rng, n, "2024-10-01", "2026-12-31"),
        "cheque_status": _noise(rng, _pick(rng, ["Cleared", "Pending", "Bounced", "Stopped"], n, [.82, .1, .06, .02])),
        "bank_micr": "4000" + _digits(rng, n, 5),
    })
    n = 120000
    out["general_ledger_entry"] = pd.DataFrame({
        "gl_entry_id": _ids("GL", n, 9), "gl_account": _pick(rng, ["1001", "1100", "2001", "2100", "3001", "4001", "5001", "5100"], n),
        "branch_id": rng.choice(base["branch"]["branch_id"].dropna().astype(str).to_numpy(), n),
        "entry_type": _noise(rng, _pick(rng, ["Debit", "Credit"], n)), "amount": rng.gamma(2.0, 60000, n).round(2).astype(str),
        "currency": _noise(rng, _pick(rng, ["INR"], n)), "posting_date": _dates(rng, n, "2024-10-01", "2026-12-31"),
        "narration_code": _pick(rng, ["INT", "FEE", "CHG", "ADJ", "XFR", "SAL"], n), "batch_ref": "GLB" + _digits(rng, n, 7),
    })
    days = pd.date_range(*CALENDAR)
    pairs = ["USD/INR", "EUR/INR", "GBP/INR", "JPY/INR", "AED/INR", "SGD/INR", "AUD/INR", "CAD/INR"]
    base_rate = {"USD/INR": 83.2, "EUR/INR": 90.1, "GBP/INR": 105.3, "JPY/INR": .55, "AED/INR": 22.6, "SGD/INR": 61.8, "AUD/INR": 54.9, "CAD/INR": 61.2}
    n = len(days) * len(pairs)
    d = np.repeat(days.strftime("%Y-%m-%d").to_numpy(), len(pairs))
    pr = np.tile(np.array(pairs, dtype=object), len(days))
    mid = np.array([base_rate[x] for x in pr]) * (1 + rng.normal(0, .004, n))
    out["fx_rate"] = pd.DataFrame({
        "rate_id": _ids("FX", n, 8), "currency_pair": _noise(rng, pr), "buy_rate": (mid * .997).round(4).astype(str),
        "sell_rate": (mid * 1.003).round(4).astype(str), "mid_rate": mid.round(4).astype(str),
        "rate_source": _noise(rng, _pick(rng, ["Treasury", "RBI-Ref"], n, [.7, .3])), "rate_date": d,
    })
    n = max(int(len(cust) * .4), 1000)
    opened = pd.to_datetime(_dates(rng, n, "2024-10-01", "2026-12-31"))
    out["customer_complaint"] = pd.DataFrame({
        "complaint_id": _ids("CMP", n, 8), "customer_id": rng.choice(cust, n),
        "category": _noise(rng, _pick(rng, ["Card", "Loan", "Account", "Digital", "Charges", "Branch service"], n)),
        "severity": _noise(rng, _pick(rng, ["Low", "Medium", "High", "Critical"], n, [.5, .3, .15, .05])),
        "complaint_status": _noise(rng, _pick(rng, ["Open", "In progress", "Resolved", "Closed"], n, [.15, .15, .5, .2])),
        "channel": _noise(rng, _pick(rng, ["Call", "Email", "Branch", "App"], n)), "opened_date": opened.strftime("%Y-%m-%d"),
        "resolved_date": (opened + pd.to_timedelta(rng.integers(1, 30, n), unit="D")).strftime("%Y-%m-%d"),
        "resolution_days": rng.integers(1, 30, n).astype(str), "assigned_to": "AG" + _digits(rng, n, 4),
    })
    n = len(cust) * 2
    out["customer_consent"] = pd.DataFrame({
        "consent_id": _ids("CON", n, 8), "customer_id": rng.choice(cust, n),
        "consent_type": _noise(rng, _pick(rng, ["Marketing", "Data sharing", "Bureau pull", "SMS alerts"], n)),
        "consent_status": _noise(rng, _pick(rng, ["Granted", "Withdrawn", "Expired"], n, [.8, .12, .08])),
        "granted_on": _dates(rng, n, "2023-01-01", "2026-12-31"), "expires_on": _dates(rng, n, "2026-12-01", "2030-12-31"),
        "channel": _noise(rng, _pick(rng, ["App", "Branch", "Web"], n)),
    })
    n = len(cust) * 3
    out["campaign_response"] = pd.DataFrame({
        "response_id": _ids("RSP", n, 9), "customer_id": rng.choice(cust, n), "campaign_code": _pick(rng, ["CMP-HL25", "CMP-PL25", "CMP-FD25", "CMP-CC25", "CMP-SIP25"], n),
        "offer_type": _pick(rng, ["Home loan", "Personal loan", "Fixed deposit", "Credit card", "Mutual fund"], n),
        "response": _noise(rng, _pick(rng, ["Accepted", "Declined", "No response", "Enquired"], n, [.12, .18, .6, .1])),
        "response_date": _dates(rng, n, "2024-10-01", "2026-12-31"), "channel": _noise(rng, _pick(rng, ["SMS", "Email", "App", "Call"], n)),
        "conversion_flag": _pick(rng, ["Y", "N"], n, [.1, .9]),
    })
    n = int(len(loan) * 2.5)
    applied = pd.to_datetime(_dates(rng, n, "2024-10-01", "2026-12-31"))
    out["loan_application"] = pd.DataFrame({
        "application_id": _ids("APP", n, 9), "customer_id": rng.choice(cust, n),
        "loan_type": _noise(rng, _pick(rng, ["Home", "Personal", "Auto", "Education", "Business"], n, [.3, .3, .15, .1, .15])),
        "requested_amount": (rng.lognormal(13.2, .7, n).round(-3)).astype(int).astype(str), "tenure_months": _pick(rng, ["12", "24", "36", "60", "120", "240"], n),
        "application_status": _noise(rng, _pick(rng, ["Approved", "Rejected", "Under review", "Withdrawn"], n, [.55, .2, .2, .05])),
        "applied_date": applied.strftime("%Y-%m-%d"), "decision_date": (applied + pd.to_timedelta(rng.integers(1, 15, n), unit="D")).strftime("%Y-%m-%d"),
        "pan_number": _pick(rng, list("ABCDEFGHJK"), n) + _digits(rng, n, 9), "credit_score": rng.integers(300, 900, n).astype(str),
    })
    n = int(len(loan) * .7)
    out["collateral"] = pd.DataFrame({
        "collateral_id": _ids("COL", n, 8), "loan_id": rng.choice(loan, n),
        "collateral_type": _noise(rng, _pick(rng, ["Property", "Vehicle", "Gold", "Securities"], n, [.5, .25, .15, .1])),
        "market_value": (rng.lognormal(14.3, .6, n).round(-3)).astype(int).astype(str), "valuation_date": _dates(rng, n, "2024-01-01", "2026-12-31"),
        "ltv_ratio": rng.uniform(.4, .85, n).round(2).astype(str), "collateral_status": _noise(rng, _pick(rng, ["Pledged", "Released", "Under valuation"], n, [.8, .12, .08])),
        "insurance_flag": _pick(rng, ["Y", "N"], n, [.85, .15]),
    })
    n = len(cust) * 2
    sc = rng.integers(300, 900, n)
    out["credit_bureau_score"] = pd.DataFrame({
        "score_id": _ids("CBS", n, 8), "customer_id": rng.choice(cust, n), "bureau": _noise(rng, _pick(rng, ["CIBIL", "Experian", "Equifax", "CRIF"], n)),
        "score": sc.astype(str), "score_band": _noise(rng, np.select([sc < 580, sc < 670, sc < 740, sc < 800], ["Poor", "Fair", "Good", "Very good"], "Excellent")),
        "pulled_on": _dates(rng, n, "2024-10-01", "2026-12-31"), "enquiries_6m": rng.integers(0, 9, n).astype(str), "active_loans": rng.integers(0, 6, n).astype(str),
    })
    n = max(int(len(tx) * .01), 1000)
    out["fraud_alert"] = pd.DataFrame({
        "alert_id": _ids("FRD", n, 8), "transaction_id": rng.choice(tx, n), "account_id": rng.choice(acct, n),
        "rule_code": _pick(rng, ["VEL-01", "GEO-02", "AMT-03", "NEW-BEN-04", "NIGHT-05"], n), "risk_score": rng.integers(40, 100, n).astype(str),
        "alert_status": _noise(rng, _pick(rng, ["Open", "Investigating", "Closed", "Escalated"], n, [.15, .2, .55, .1])),
        "raised_on": _dates(rng, n, "2024-10-01", "2026-12-31"), "analyst_id": "AN" + _digits(rng, n, 3),
        "outcome": _noise(rng, _pick(rng, ["Genuine", "Fraud", "False positive", "Pending"], n, [.1, .08, .6, .22])),
    })
    owner = {t: sd["system"] for sd in SOURCES.values() for t in sd["tables"]}
    for name, df in out.items():
        df["source_system"] = owner[name]
        df["updated_at"] = stamp
        assert list(df.columns) == SOURCE_ONLY[name], (name, list(df.columns))
    return out
