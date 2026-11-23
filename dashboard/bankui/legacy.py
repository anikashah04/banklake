"""Detail pages that were built before the enterprise shell. Each still reads real data and runs real actions.

render(name, role) draws one page. Names are the original page names (Ingestion, Bronze, Silver Merge, ...).
"""
from .helpers import *  # noqa: F401,F403
from .helpers import current_role  # noqa: F401

MODE = "Technical"
page = ""
role = "analyst"


def page_title(text):
    """The shell header already shows the title; here only the plain-language help box."""
    friendly, what, how = guide.PAGE_HELP.get(page, (text, "", ""))
    if what:
        with st.expander("What am I looking at?", expanded=False):
            st.markdown(f"{what}\n\n**How to use it:** {how}")


def render(page_name: str, role_name: str) -> None:
    global page, role
    page, role = page_name, role_name
    if page == "__none__":
        pass
    elif page == "BI dashboards":
        page_title("BI dashboards")
        st.caption("Built **only from Gold tables**, the way a bank's analytics users would consume them. Data is synthetic, so the figures "
                   "and thresholds are illustrative; the questions and workflow are realistic.")
        missing = [g for g in GOLD_TABLES if not has("gold", g)]
        if missing:
            st.info(f"Run the Gold pipelines first (missing: {', '.join(missing)}).")
            st.stop()
        G = {g: pq("gold", g) for g in GOLD_TABLES}
        t_exec, t_br, t_cu, t_dep, t_loan, t_txn = st.tabs(["Executive summary", "Branches", "Customers & cross-sell", "Deposits maturity", "Loans", "Transactions"])

        with t_exec:
            st.caption("Decision supported: where is the bank strong, how much is lent against what it holds, and where is money moving?")
            k = Q(f"""SELECT (SELECT count(*) FROM {G['customer_360']}) AS customers,
                             (SELECT sum(n_accounts) FROM {G['branch_performance']}) AS accounts,
                             (SELECT sum(total_balance)::DOUBLE FROM {G['branch_performance']}) AS balances,
                             (SELECT sum(total_deposits)::DOUBLE FROM {G['deposit_summary']}) AS fds,
                             (SELECT sum(total_loan_amount)::DOUBLE FROM {G['loan_summary']}) AS loans""").iloc[0]
            base = k.balances + k.fds
            c = st.columns(5)
            c[0].metric("Customers", f"{int(k.customers):,}")
            c[1].metric("Accounts", f"{int(k.accounts):,}")
            c[2].metric("Deposit base", inr(base), help="Account balances + fixed deposits")
            c[3].metric("Loan book", inr(k.loans))
            c[4].metric("Loan-to-deposit", f"{k.loans / base:.0%}" if base else "n/a", help="Loan book / (account balances + fixed deposits)")
            a, b = st.columns(2)
            a.markdown("**Account balances by region**")
            a.bar_chart(Q(f"SELECT region, sum(total_balance)::DOUBLE AS balance FROM {G['branch_performance']} GROUP BY 1 ORDER BY 2 DESC").set_index("region"))
            b.markdown("**Customers by segment**")
            b.bar_chart(Q(f"SELECT segment, count(*) AS customers FROM {G['customer_360']} GROUP BY 1 ORDER BY 2 DESC").set_index("segment"))
            st.markdown("**Monthly net flow (credits minus debits)**")
            tx = Q(f"SELECT txn_month, txn_type, sum(total_amount)::DOUBLE AS amt FROM {G['transaction_summary']} GROUP BY 1,2").pivot_table(index="txn_month", columns="txn_type", values="amt").fillna(0)
            if {"CREDIT", "DEBIT"} <= set(tx.columns):
                st.line_chart((tx["CREDIT"] - tx["DEBIT"]).rename("net flow"))

        with t_br:
            st.caption("Decision supported: which branches carry the business, and which are candidates for review or a local campaign?")
            br = Q(f"SELECT * FROM {G['branch_performance']}")
            regs = sorted(br.region.dropna().unique())
            sel = st.multiselect("Region", regs, default=regs)
            br = br[br.region.isin(sel)].copy()
            br["txns_per_account"] = (br.n_transactions / br.n_accounts.where(br.n_accounts > 0)).round(1)
            c = st.columns(3)
            c[0].metric("Branches", len(br))
            c[1].metric("Total balance", inr(br.total_balance.astype(float).sum()))
            c[2].metric("Avg balance / branch", inr(br.total_balance.astype(float).mean() if len(br) else 0))
            a, b = st.columns(2)
            a.markdown("**Top 10 branches by balance**")
            a.bar_chart(br.assign(total_balance=br.total_balance.astype(float)).nlargest(10, "total_balance").set_index("branch_name")["total_balance"])
            b.markdown("**Accounts vs balance (each dot = a branch)**")
            b.scatter_chart(br.assign(total_balance=br.total_balance.astype(float)), x="n_accounts", y="total_balance", color="region")
            pct = st.slider("Flag branches in the bottom X% by balance", 5, 50, 20)
            live = br[br.n_accounts > 0].assign(total_balance=lambda d: d.total_balance.astype(float))
            if len(live):
                cut = live.total_balance.quantile(pct / 100)
                flagged = live[live.total_balance <= cut].sort_values("total_balance")
                st.markdown(f"**Branches to review ({len(flagged)})** - balance at or below {inr(cut)}")
                st.dataframe(flagged[["branch_id", "branch_name", "region", "n_accounts", "total_balance", "n_transactions", "txns_per_account"]], hide_index=True, width="stretch")
            st.markdown("**All branches**")
            st.dataframe(br.sort_values("total_balance", ascending=False), hide_index=True, width="stretch")

        with t_cu:
            st.caption("Decision supported: who should relationship managers call first? Gold holds unmasked customer details, so the lists show real (synthetic) names and phones.")
            cu = Q(f"SELECT * FROM {G['customer_360']}")
            for col in ["total_balance", "total_deposits", "total_loans", "total_txn_amount"]:
                cu[col] = cu[col].astype(float)
            seg = cu.groupby("segment").agg(customers=("customer_id", "count"), avg_balance=("total_balance", "mean"),
                                            pct_with_fd=("total_deposits", lambda x: (x > 0).mean()),
                                            pct_with_loan=("total_loans", lambda x: (x > 0).mean())).round(2).reset_index()
            st.markdown("**Segment profile**")
            st.dataframe(seg, hide_index=True, width="stretch",
                         column_config={"pct_with_fd": st.column_config.NumberColumn("% with FD", format="percent"),
                                        "pct_with_loan": st.column_config.NumberColumn("% with loan", format="percent")})
            st.markdown("**Action list 1: fixed-deposit cross-sell** (healthy balance, no FD, no loan)")
            minbal = st.slider("Minimum account balance (₹)", 0, 500_000, 100_000, step=10_000)
            l1 = cu[(cu.total_balance >= minbal) & (cu.total_deposits == 0) & (cu.total_loans == 0)].sort_values("total_balance", ascending=False)
            st.metric("Candidates", f"{len(l1):,}", help="Customers meeting the rule above")
            view = l1.head(100)[["customer_id", "customer_name", "phone", "segment", "city", "n_accounts", "total_balance"]]
            st.dataframe(view, hide_index=True, width="stretch")
            st.download_button("Download this list (CSV, contains unmasked synthetic PII)", view.to_csv(index=False), "fd_crosssell.csv")
            st.markdown("**Action list 2: credit review** (loans large relative to balances held)")
            lev = st.slider("Loans at least X times the balance held", 1, 100, 25)
            l2 = cu[(cu.total_loans > 0) & (cu.total_loans >= lev * cu.total_balance.clip(lower=1))].sort_values("total_loans", ascending=False)
            st.metric("Customers to review", f"{len(l2):,}")
            v2 = l2.head(100)[["customer_id", "customer_name", "segment", "total_balance", "total_loans"]]
            st.dataframe(v2, hide_index=True, width="stretch")

        with t_dep:
            st.caption("Decision supported: how much fixed-deposit money matures soon, so relationship teams can plan retention offers?")
            dd = Q(f"SELECT segment, maturity_month, n_deposits, total_deposits::DOUBLE AS total_deposits FROM {G['deposit_summary']}")
            c = st.columns(2)
            asof = c[0].date_input("As-of date", value=pd.Timestamp(2026, 7, 1).date())
            horizon = c[1].slider("Maturing within (months)", 1, 36, 12)
            start = pd.Period(asof, "M")
            win = dd[(dd.maturity_month >= str(start)) & (dd.maturity_month <= str(start + horizon))]
            m = st.columns(2)
            m[0].metric(f"FDs maturing in next {horizon} months", inr(win.total_deposits.sum()))
            m[1].metric("Number of deposits", f"{int(win.n_deposits.sum()):,}")
            st.markdown("**Maturity ladder by month and segment**")
            if len(win):
                st.bar_chart(win.pivot_table(index="maturity_month", columns="segment", values="total_deposits", aggfunc="sum").fillna(0))
            st.markdown("**By segment (whole book)**")
            st.dataframe(dd.groupby("segment").agg(deposits=("n_deposits", "sum"), amount=("total_deposits", "sum")).reset_index(), hide_index=True, width="stretch")

        with t_loan:
            st.caption("Decision supported: where is the loan book concentrated and what does it earn?")
            ln = Q(f"SELECT segment, n_loans, total_loan_amount::DOUBLE AS exposure, avg_interest_rate::DOUBLE AS avg_rate FROM {G['loan_summary']}")
            ln["share_of_book"] = (ln.exposure / ln.exposure.sum()).round(3)
            top = ln.sort_values("exposure", ascending=False).iloc[0]
            c = st.columns(3)
            c[0].metric("Loan book", inr(ln.exposure.sum()))
            c[1].metric("Largest segment", f"{top.segment} ({top.share_of_book:.0%})")
            c[2].metric("Simple avg rate", f"{ln.avg_rate.mean():.2f}%", help="Unweighted mean of segment averages")
            a, b = st.columns(2)
            a.markdown("**Exposure by segment**")
            a.bar_chart(ln.set_index("segment")["exposure"])
            b.markdown("**Average interest rate by segment (%)**")
            b.bar_chart(ln.set_index("segment")["avg_rate"])
            st.dataframe(ln, hide_index=True, width="stretch", column_config={"share_of_book": st.column_config.NumberColumn("share of book", format="percent")})

        with t_txn:
            st.caption("Decision supported: is activity growing, and are customers putting money in faster than they take it out?")
            tx = Q(f"SELECT txn_month, txn_type, n_transactions, total_amount::DOUBLE AS amount FROM {G['transaction_summary']} ORDER BY 1")
            piv = tx.pivot_table(index="txn_month", columns="txn_type", values="amount", aggfunc="sum").fillna(0)
            cnt = tx.groupby("txn_month").n_transactions.sum()
            if len(cnt) >= 2:
                c = st.columns(3)
                c[0].metric("Latest month", cnt.index[-1])
                c[1].metric("Transactions", f"{int(cnt.iloc[-1]):,}", f"{(cnt.iloc[-1] / cnt.iloc[-2] - 1):+.0%} vs prior month")
                c[2].metric("Value", inr(piv.iloc[-1].sum()))
            st.markdown("**Value by type**")
            st.line_chart(piv)
            st.markdown("**Number of transactions per month**")
            st.bar_chart(cnt)

    elif page == "Table journey":
        page_title("Table journey (ETL / ELT, step by step)")
        st.caption("Raw files → Bronze → Silver → Gold. Boxes are the real pipelines from the metadata table; colours show the latest run (green ok, red failed, orange blocked, grey skipped).")
        if not meta.query("SELECT 1 FROM pipeline_execution_log LIMIT 1"):
            st.info("Nothing has run yet. Use **Ingestion** to generate data and **Metadata & orchestration** to run the pipelines.")
        st.graphviz_chart(flow_dot(), width="stretch")
        c = st.columns(3)
        for col, layer in zip(c, ["bronze", "silver", "gold"]):
            col.markdown(f"**{layer.upper()}**")
            col.caption(LAYER_DOCS[layer])

        st.subheader("Follow one table through every stage")
        ent = st.selectbox("Source table", ORDER, index=ORDER.index("customer"))
        raw = raw_df(ent, raw_stamp(ent)) if (RAW / ent).exists() else pd.DataFrame()
        n_raw, n_b, n_s = len(raw), count("bronze", ent), count("silver", ent)
        n_bq, n_sq = count("quarantine", f"bronze_{ent}"), count("quarantine", f"silver_{ent}")
        removed = max(0, n_b - n_s - n_sq)
        funnel = pd.DataFrame([
            {"step": "1. Raw rows in source files", "rows": n_raw},
            {"step": "2. Loaded to Bronze", "rows": n_b},
            {"step": "   (unreadable lines quarantined at Bronze)", "rows": n_bq},
            {"step": "3. Rejected to quarantine at Silver", "rows": n_sq},
            {"step": "   (duplicates / older versions merged away - derived)", "rows": removed},
            {"step": "4. Rows in Silver (clean, unique)", "rows": n_s},
        ])
        a, b = st.columns([1, 1])
        a.dataframe(funnel, hide_index=True, width="stretch")
        if has("quarantine", f"silver_{ent}"):
            why = Q(f"SELECT reject_reason AS reason, count(*) AS rows FROM {pq('quarantine', 'silver_' + ent)} GROUP BY 1 ORDER BY 2 DESC")
            b.markdown("**Why rows were rejected**")
            b.bar_chart(why.set_index("reason"))
        else:
            b.caption("No rows were quarantined for this table.")

        st.markdown("**The same table at each stage (first rows)**")
        tabs = st.tabs(["1 Raw", "2 Bronze", "3 Silver", "4 Quarantine", "5 Gold"])
        with tabs[0]:
            st.caption("Exactly as it arrived: everything is text, mistakes included.")
            st.dataframe(mask_clear(raw.head(8), ent), hide_index=True, width="stretch")
        with tabs[1]:
            st.caption("Still raw text, plus _source_file, _batch_id, _ingest_ts, _load_type. Bronze is stored unmasked on this machine only; it is masked here for display.")
            st.dataframe(mask_clear(Q(f"SELECT * FROM {pq('bronze', ent)} LIMIT 8"), ent) if has("bronze", ent) else pd.DataFrame(), hide_index=True, width="stretch")
        with tabs[2]:
            st.caption("Typed, trimmed, de-duplicated and key-checked.")
            st.dataframe(Q(f"SELECT * FROM {pq('silver', ent)} LIMIT 8") if has("silver", ent) else pd.DataFrame(), hide_index=True, width="stretch")
        with tabs[3]:
            st.caption("Rows Silver refused, with the reason.")
            st.dataframe(Q(f"SELECT * FROM {pq('quarantine', 'silver_' + ent)} LIMIT 8") if has("quarantine", f"silver_{ent}") else pd.DataFrame(), hide_index=True, width="stretch")
        with tabs[4]:
            g = GOLD_FOR[ent]
            st.caption(f"gold_{g} - the report table this data feeds (PII masked).")
            st.dataframe(Q(f"SELECT * FROM {pq('gold', g)} LIMIT 8") if has("gold", g) else pd.DataFrame(), hide_index=True, width="stretch")

        st.subheader("Execution steps")
        runs = df("SELECT DISTINCT run_id FROM pipeline_execution_log ORDER BY run_id DESC LIMIT 20")
        if not runs.empty:
            rid = st.selectbox("Run", runs.run_id.tolist())
            steps = df("SELECT execution_id, pipeline_id, load_strategy, rows_in, rows_out, rows_rejected, status, duration_s, retry_count, error_message FROM pipeline_execution_log l LEFT JOIN (SELECT pipeline_id AS pid, load_strategy FROM pipeline_metadata) m ON m.pid = l.pipeline_id WHERE run_id=? ORDER BY execution_id", (rid,))
            steps.insert(0, "step", range(1, len(steps) + 1))
            st.dataframe(steps.drop(columns="execution_id"), hide_index=True, width="stretch")

    elif page == "Data explorer":
        page_title("Data explorer")
        st.caption("Browse every real table in the lake. All values are synthetic.")
        options = [("raw", e) for e in ORDER if (RAW / e).exists()]
        for layer in ["bronze", "silver", "quarantine", "gold"]:
            d = LAKE / layer
            options += [(layer, p.name) for p in sorted(d.glob("*")) if any(p.glob("*.parquet"))] if d.exists() else []
        if not options:
            st.info("No data yet. Generate data and run the pipelines first.")
            st.stop()
        c = st.columns([1, 2])
        layer = c[0].selectbox("Layer", sorted({o[0] for o in options}, key=["raw", "bronze", "silver", "quarantine", "gold"].index))
        name = c[1].selectbox("Table", [n for l, n in options if l == layer])
        if layer == "raw":
            full = raw_df(name, raw_stamp(name))
            total, cols = len(full), list(full.columns)
            types = {c_: "TEXT (raw)" for c_ in cols}
        else:
            total = Q(f"SELECT count(*) AS n FROM {pq(layer, name)}").n[0]
            d_ = Q(f"DESCRIBE SELECT * FROM {pq(layer, name)}")
            cols, types = d_.column_name.tolist(), dict(zip(d_.column_name, d_.column_type))
        ent = re.sub(r"^(bronze|silver_merge|silver)_", "", name)
        st.metric("Rows", f"{total:,}")
        f1, f2 = st.columns(2)
        fcol = f1.selectbox("Filter column", ["(none)"] + cols)
        ftxt = f2.text_input("contains (case-insensitive)")
        limit = st.slider("Rows to show", 10, 1000, 100)
        if layer == "raw":
            view = full
            if fcol != "(none)" and ftxt:
                view = view[view[fcol].astype(str).str.contains(ftxt, case=False, na=False, regex=False)]
            matched, view = len(view), view.head(limit)
        else:
            where, params = "", []
            if fcol != "(none)" and ftxt:
                where, params = f'WHERE CAST("{fcol}" AS VARCHAR) ILIKE ?', [f"%{ftxt}%"]
            matched = Q(f"SELECT count(*) AS n FROM {pq(layer, name)} {where}", params).n[0]
            view = Q(f"SELECT * FROM {pq(layer, name)} {where} LIMIT {limit}", params)
        if layer in ("raw", "bronze", "silver_merge") or name.startswith(("bronze_", "silver_merge_")):
            view = mask_clear(view, ent)  # clear-text layers are masked for display
        st.caption(f"{matched:,} matching rows (showing {len(view):,})")
        st.dataframe(view, hide_index=True, width="stretch")
        st.subheader("Columns")
        docs = COLUMN_DOCS.get(ent, {})
        ex = {c_: (view[c_].dropna().iloc[0] if c_ in view and view[c_].notna().any() else "") for c_ in cols}
        st.dataframe(pd.DataFrame([{"column": c_, "type": types[c_], "example": str(ex[c_]), "meaning": docs.get(c_, "")} for c_ in cols]),
                     hide_index=True, width="stretch")

    elif page == "Customers":
        page_title("Customers (synthetic)")
        st.caption(f"Viewing as **{role}**. PII is stored masked everywhere in the lake; clear values come from the encrypted vault only if your role allows it.")
        if not has("silver", "customer"):
            st.info("Run the pipelines first (Silver customer table not found).")
            st.stop()
        t1, t2 = st.tabs(["Browse & insights", "One customer: full journey"])
        gold_ok = has("gold", "customer_360")
        with t1:
            segs = Q(f"SELECT DISTINCT segment FROM {pq('silver', 'customer')} ORDER BY 1").segment.tolist()
            f1, f2 = st.columns(2)
            sel = f1.multiselect("Segment", segs, default=segs)
            city = f2.text_input("City contains")
            agg = (f"LEFT JOIN (SELECT customer_id, n_accounts, total_balance, total_deposits, total_loans FROM {pq('gold', 'customer_360')}) g USING (customer_id)" if gold_ok else "")
            extra = ", g.n_accounts, g.total_balance, g.total_deposits, g.total_loans" if gold_ok else ""
            base = f"FROM {pq('silver', 'customer')} c {agg} WHERE list_contains(?, c.segment) AND c.city ILIKE ?"
            prm = [sel, f"%{city}%"]
            k = Q(f"SELECT count(*) AS n {base}", prm).n[0]
            st.metric("Customers matching", f"{k:,}")
            table = Q(f"SELECT c.customer_id, c.customer_name, c.email, c.phone, c.customer_name_token, c.email_token, c.phone_token, c.city, c.segment{extra} {base} ORDER BY c.customer_id LIMIT 500", prm)
            st.dataframe(show_pii(table, "customer"), hide_index=True, width="stretch")
            a, b = st.columns(2)
            a.markdown("**Customers by segment**")
            a.bar_chart(Q(f"SELECT c.segment, count(*) AS customers {base} GROUP BY 1", prm).set_index("segment"))
            b.markdown("**Top 10 cities**")
            b.bar_chart(Q(f"SELECT c.city, count(*) AS customers {base} GROUP BY 1 ORDER BY 2 DESC LIMIT 10", prm).set_index("city"))
        with t2:
            ids = Q(f"SELECT customer_id FROM {pq('silver', 'customer')} ORDER BY 1").customer_id.tolist()
            if "cust" not in st.session_state:
                st.session_state.cust = ids[0]
            if st.button("Pick a random customer"):
                import random
                st.session_state.cust = random.choice(ids)
            cid = st.text_input("Customer ID", key="cust").strip()
            st.caption("Try an ID from the Browse tab. Customers rejected by Silver (e.g. missing name) are explained below.")
            st.subheader("1. Bronze: every version that arrived (stored unmasked locally; masked for display)")
            if has("bronze", "customer"):
                st.dataframe(mask_clear(Q(f"SELECT customer_id, customer_name, email, city, segment, updated_at, _source_file, _ingest_ts, _load_type FROM {pq('bronze', 'customer')} WHERE trim(customer_id)=? ORDER BY updated_at", [cid]), "customer"), hide_index=True, width="stretch")
                st.caption("More than one row means the customer was duplicated or updated in a later file.")
            st.subheader("2. Silver: the clean record (what we keep)")
            sc = Q(f"SELECT * FROM {pq('silver', 'customer')} WHERE customer_id=?", [cid])
            st.dataframe(show_pii(sc, "customer") if not sc.empty else sc, hide_index=True, width="stretch")
            if sc.empty:
                if has("quarantine", "silver_customer"):
                    qq = Q(f"SELECT * FROM {pq('quarantine', 'silver_customer')} WHERE customer_id=?", [cid])
                    if not qq.empty:
                        st.error(f"Rejected by Silver: {', '.join(qq.reject_reason.unique())}")
                        st.dataframe(qq, hide_index=True, width="stretch")
                    else:
                        st.warning("Customer ID not found.")
                st.stop()
            st.subheader("3. Gold: what reporting users see")
            if gold_ok:
                gc = Q(f"SELECT * FROM {pq('gold', 'customer_360')} WHERE customer_id=?", [cid])
                st.dataframe(show_pii(gc, "customer", note=False), hide_index=True, width="stretch")
            st.subheader("Accounts")
            accts = Q(f"SELECT a.account_id, a.account_type, a.account_number, a.account_number_token, a.balance, a.opened_date, b.branch_name, b.region FROM {pq('silver', 'account')} a LEFT JOIN {pq('silver', 'branch')} b USING (branch_id) WHERE a.customer_id=? ORDER BY a.account_id", [cid])
            st.dataframe(show_pii(accts, "account") if not accts.empty else accts, hide_index=True, width="stretch")
            st.subheader("Transactions")
            tx = Q(f"SELECT t.transaction_id, t.account_id, t.transaction_date, t.txn_type, t.amount FROM {pq('silver', 'transaction')} t JOIN {pq('silver', 'account')} a USING (account_id) WHERE a.customer_id=? ORDER BY t.transaction_date DESC", [cid])
            if tx.empty:
                st.caption("No transactions for this customer (not an error - the customer may simply have none).")
            else:
                m = tx.assign(month=tx.transaction_date.astype(str).str[:7]).pivot_table(index="month", columns="txn_type", values="amount", aggfunc="sum").astype(float)
                st.bar_chart(m)
                st.dataframe(tx.head(100), hide_index=True, width="stretch")
            a, b = st.columns(2)
            a.subheader("Deposits")
            a.dataframe(Q(f"SELECT deposit_id, deposit_amount, maturity_date FROM {pq('silver', 'deposit')} WHERE customer_id=?", [cid]) if has("silver", "deposit") else pd.DataFrame(), hide_index=True)
            b.subheader("Loans")
            b.dataframe(Q(f"SELECT loan_id, loan_amount, interest_rate FROM {pq('silver', 'loan')} WHERE customer_id=?", [cid]) if has("silver", "loan") else pd.DataFrame(), hide_index=True)

    elif page == "Ingestion":
        page_title("Ingestion")
        st.caption("Source files arrive in data/raw. Bronze reads them as text, records every file (hash, row counts, status) and never loads the same content twice.")
        a, c = st.columns(2)
        with a:
            st.subheader("Source data")
            scale = st.selectbox("Scale", ["small", "full"], help="full = 10k customers, 100k transactions")
            if st.button("Generate synthetic data (resets lake)"):
                cli_seq(("reset",), ("generate", "--scale", scale))
            if st.button("Add incremental batch"):
                cli("generate", "--incremental")
            if st.button("Add incremental batch with deletes (CDC)"):
                cli("generate", "--incremental", "--deletes")
        with c:
            st.subheader("Manifest of injected anomalies")
            mf = ROOT / "data" / "raw" / "_anomaly_manifest.json"
            st.json(json.loads(mf.read_text()), expanded=False) if mf.exists() else st.caption("generate data first")
        st.subheader("Source files on disk")
        files = [{"entity": e, "file": f.name, "size_kb": round(f.stat().st_size / 1024, 1)} for e in ORDER if (RAW / e).exists() for f in sorted((RAW / e).glob("*"))]
        st.dataframe(pd.DataFrame(files), hide_index=True, width="stretch")
        st.subheader("Ingestion audit (one row per file load attempt)")
        st.dataframe(df("SELECT ts, batch_id, entity, source_file, source_system, load_type, records_read, records_loaded, records_rejected, status, message FROM ingestion_audit_log ORDER BY id DESC LIMIT 200"), hide_index=True, width="stretch")

    elif page == "Bronze":
        page_title("Bronze")
        st.caption(LAYER_DOCS["bronze"] + " Bronze and Silver Merge hold clear synthetic PII on this machine only; it is masked here for display and never uploaded.")
        st.dataframe(df("SELECT entity, count(*) AS files_attempted, sum(status='SUCCESS') AS loaded, sum(status='SKIPPED') AS skipped_duplicate_content, "
                        "sum(status='FAILED') AS rejected_files, sum(CASE WHEN status='SUCCESS' THEN records_loaded ELSE 0 END) AS rows_loaded, "
                        "sum(records_rejected) AS rows_rejected FROM ingestion_audit_log GROUP BY entity ORDER BY entity"), hide_index=True, width="stretch")
        ent = st.selectbox("Table", ORDER, key="bronze_ent")
        if has("bronze", ent):
            st.metric("Rows in Bronze", f"{count('bronze', ent):,}")
            st.dataframe(mask_clear(Q(f"SELECT * FROM {pq('bronze', ent)} LIMIT 50"), ent), hide_index=True, width="stretch")
            st.caption("_source_file, _batch_id, _ingest_ts and _load_type are added by Bronze; everything else is the file's text, unchanged.")
        else:
            st.info("Run the Bronze pipelines first.")
        rej = LAKE / "quarantine" / "rejected_files"
        if rej.exists() and any(rej.rglob("*.*")):
            st.subheader("Rejected files (schema check failed)")
            st.dataframe(pd.DataFrame([{"entity": f.parent.name, "file": f.name} for f in sorted(rej.rglob("*.*"))]), hide_index=True, width="stretch")

    elif page == "Silver Merge":
        page_title("Silver Merge (CDC)")
        st.caption("Applies inserts, updates and deletes from Bronze to the current-state table. Deletes are soft (flagged, not removed). "
                   "Out-of-order and duplicate changes are resolved by updated_at, then batch. Still local and unmasked.")
        cdc = df("SELECT ts, batch_id, entity, load_type, rows_in, inserts, updates, deletes, ignored, duplicates_removed, rejected, rows_out FROM transformation_audit_log WHERE layer='silver_merge' ORDER BY id DESC LIMIT 200")
        if cdc.empty:
            st.info("Run the Silver Merge pipelines first.")
        else:
            t = cdc[["inserts", "updates", "deletes", "ignored", "duplicates_removed", "rejected"]].sum()
            c = st.columns(6)
            for col, k in zip(c, t.index):
                col.metric(k.replace("_", " "), int(t[k]))
            st.dataframe(cdc, hide_index=True, width="stretch")
            st.caption("ignored = stale changes older than what is already stored; rejected = null key or invalid op_type (quarantined).")
        ent = st.selectbox("Table", ORDER, key="merge_ent")
        if has("silver_merge", ent):
            st.dataframe(Q(f"SELECT _is_deleted, count(*) AS rows FROM {pq('silver_merge', ent)} GROUP BY 1 ORDER BY 1"), hide_index=True)
            st.dataframe(mask_clear(Q(f"SELECT * FROM {pq('silver_merge', ent)} ORDER BY _is_deleted DESC LIMIT 50"), ent), hide_index=True, width="stretch")

    elif page == "Silver Transform":
        page_title("Silver Transform")
        st.caption(LAYER_DOCS["silver"] + " This is where PII is masked and tokenised; real values go to the encrypted vault.")
        tr = df("SELECT ts, batch_id, entity, load_type, rows_in, rows_out, deletes AS keys_removed, rejected FROM transformation_audit_log WHERE layer='silver_transform' ORDER BY id DESC LIMIT 200")
        st.dataframe(tr, hide_index=True, width="stretch")
        ent = st.selectbox("Table", ORDER, key="tf_ent")
        a, b = st.columns(2)
        if has("quarantine", f"silver_{ent}"):
            a.markdown("**Why rows were rejected**")
            a.dataframe(Q(f"SELECT reject_reason AS reason, count(*) AS rows FROM {pq('quarantine', 'silver_' + ent)} GROUP BY 1 ORDER BY 2 DESC"), hide_index=True)
        else:
            a.caption("Nothing rejected for this table.")
        b.markdown("**Masking applied to this table**")
        b.dataframe(df("SELECT column_name, rule, is_active FROM masking_metadata WHERE entity=?", (ent,)), hide_index=True)
        if has("silver", ent):
            st.markdown("**Silver rows (masked, tokens hidden)**")
            shown = show_pii(Q(f"SELECT * FROM {pq('silver', ent)} LIMIT 50"), ent, note=True)
            st.dataframe(shown, hide_index=True, width="stretch")

    elif page == "Metadata & orchestration":
        page_title("Metadata & orchestration")
        st.caption("The pipeline_metadata and pipeline_dependencies tables decide what runs, in what order, how often it retries and what blocks what.")
        b, _ = st.columns(2)
        with b:
            st.subheader("Run pipelines")
            layers = st.multiselect("Layers", ["bronze", "silver_merge", "silver", "gold"], default=["bronze", "silver_merge", "silver", "gold"])
            refresh = st.checkbox("Full refresh (ignore watermarks)")
            if st.button("Run selected layers", type="primary"):
                cli("run", "--layers", *layers, *(["--full-refresh"] if refresh else []))
            pick = st.selectbox("Reload from pipeline (rebuilds it and everything downstream)",
                                [r["pipeline_id"] for r in meta.query("SELECT pipeline_id FROM pipeline_metadata ORDER BY execution_order")])
            if st.button("Reload"):
                cli("reload", "--pipelines", pick)
        st.subheader("Pipeline graph (latest status)")
        st.graphviz_chart(flow_dot(), width="stretch")
        st.subheader("Pipeline metadata (edit `is_active` to enable/disable a pipeline)")
        md = df("SELECT execution_order, pipeline_id, source_database, source_table, target_database, target_table, source_layer, target_layer, load_strategy, primary_key, watermark_column, max_retries, is_active, handler FROM pipeline_metadata ORDER BY execution_order")
        edited = st.data_editor(md, hide_index=True, width="stretch", disabled=[c for c in md.columns if c != "is_active"])
        if st.button("Save metadata changes"):
            for _, r in edited.iterrows():
                meta.execute("UPDATE pipeline_metadata SET is_active=? WHERE pipeline_id=?", (r["is_active"], r["pipeline_id"]))
            st.success("saved")
        st.subheader("Watermarks (last batch each pipeline processed)")
        st.dataframe(df("SELECT * FROM pipeline_watermark ORDER BY pipeline_id"), hide_index=True, width="stretch")
        st.subheader("Dependencies")
        st.dataframe(df("SELECT * FROM pipeline_dependencies ORDER BY pipeline_id"), hide_index=True, width="stretch")

    elif page == "Data quality & reconciliation":
        page_title("Data quality & reconciliation")
        a, b = st.columns(2)
        with a:
            if st.button("Run the checks", type="primary"):
                cli("reconcile")
        with b:
            with st.expander("Try it: break something on purpose, then watch it get detected"):
                ent = st.selectbox("Silver table", ["account", "customer", "transaction", "deposit", "loan"])
                mode = st.selectbox("Discrepancy", ["delete", "duplicate", "alter"])
                n = st.number_input("Rows affected", 1, 500, 25)
                if st.button("Inject discrepancy"):
                    cli("inject", "--entity", ent, "--mode", mode, "--n", str(n))
                if st.button("Repair: full refresh of Silver from Bronze"):
                    cli("run", "--pipelines", f"silver_{ent}", "--full-refresh")
        latest = df("SELECT run_id FROM reconciliation_results ORDER BY id DESC LIMIT 1")
        if latest.empty:
            st.info("The checks have not been run yet. Press **Run the checks**.")
        else:
            rid = latest.iloc[0]["run_id"]
            res = df("SELECT entity, stage, check_name, source_value, target_value, status, detail FROM reconciliation_results WHERE run_id=?", (rid,))
            checks = res[~res.check_name.isin(["rejection_breakdown", "join_funnel"]) & ~res.check_name.str.startswith("info_")]
            fails = checks[checks.status == "FAIL"]
            st.markdown(f"**{len(checks) - len(fails)} of {len(checks)} checks passed.**" + (" Every record is accounted for." if not len(fails) else ""))
            st.caption(f"Check run {rid}")
            c = st.columns(3)
            c[0].metric("Checks", len(checks))
            c[1].metric("Failed", len(fails))
            if len(fails):
                first = fails.assign(o=fails.stage.map(reconcile.STAGE_ORDER)).sort_values("o").iloc[0]
                c[2].metric("First failing stage", first.stage)
                st.error(f"{first.entity} / {first.check_name}: {first.detail}")
            else:
                c[2].metric("First failing stage", "none")
                st.success("Source and target reconcile.")
            with st.expander("Details: every check, stage by stage", expanded=(MODE == "Technical")):
                by_stage = checks.groupby(["stage", "status"]).size().unstack(fill_value=0).reindex(reconcile.STAGES).fillna(0).astype(int)
                st.caption("Checks per stage: BRONZE = raw to Bronze, SILVER_MERGE = Bronze to merged table, SILVER_TRANSFORM = merged table to Silver, GOLD = Silver to reports")
                st.dataframe(by_stage, width="stretch")
                st.dataframe(checks.assign(o=checks.stage.map(reconcile.STAGE_ORDER)).sort_values("o", kind="stable").drop(columns="o")
                             .style.map(lambda v: "background-color:#f8d7da" if v == "FAIL" else "", subset=["status"]),
                             hide_index=True, width="stretch")
                fun = res[res.check_name == "join_funnel"]
                if not fun.empty:
                    st.subheader("Join funnel (where do records drop out?)")
                    st.dataframe(pd.DataFrame(json.loads(fun.iloc[0].detail)["funnel"]), hide_index=True)
                br = res[res.check_name == "rejection_breakdown"]
                if not br.empty:
                    st.subheader("Why records were rejected")
                    rows = [{"entity": r.entity, "reason": k, "records": v} for r in br.itertuples() for k, v in json.loads(r.detail).items()]
                    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
        with st.expander("Details: find records that went missing and why", expanded=(MODE == "Technical")):
            st.subheader("Trace missing records: which layer lost them, and why")
            st.caption("Compares the keys in the raw source files with Silver, then follows every missing key through Bronze, Silver Merge and Silver Transform.")
            tent = st.selectbox("Entity to trace", ORDER, index=ORDER.index("account"), key="trace_entity")
            if (RAW / tent).exists() and has("silver", tent):
                t = trace.trace(tent, limit=200)
                lc = t["layer_counts"]
                m = st.columns(5)
                m[0].metric("Source keys", f"{lc['raw_distinct_keys']:,}")
                m[1].metric("Bronze", f"{lc['bronze_distinct_keys']:,}")
                m[2].metric("Silver Merge", f"{lc['silver_merge_keys']:,}", f"-{lc['silver_merge_soft_deleted']} deleted" if lc["silver_merge_soft_deleted"] else None, delta_color="off")
                m[3].metric("Silver", f"{lc['silver_transform_keys']:,}")
                m[4].metric("Unexplained", t["unexplained_total"])
                if t["by_reason"]:
                    st.dataframe(pd.DataFrame([{"reason": k, "records": v} for k, v in sorted(t["by_reason"].items(), key=lambda x: -x[1])]),
                                 hide_index=True, width="stretch")
                if t["unexplained"]:
                    st.error(f"{t['unexplained_total']} source records are missing from Silver without a recorded reason. First layer where each vanished:")
                    st.dataframe(pd.DataFrame(t["unexplained"]).drop(columns="explained"), hide_index=True, width="stretch")
                else:
                    st.success("Every source record that is missing from Silver has a recorded reason.")
                if t["extra_in_silver"]:
                    st.warning(f"Keys in Silver that are not in the source: {t['extra_in_silver'][:10]}")
                if t["explained"]:
                    with st.expander("Records explained by the pipeline (rejected or deleted)"):
                        st.dataframe(pd.DataFrame(t["explained"]).drop(columns="explained"), hide_index=True, width="stretch")
            else:
                st.info("Generate data and run the pipelines first.")
            st.subheader("Data-quality results")
            st.dataframe(df("SELECT entity, check_name, status, value, ts FROM data_quality_results ORDER BY id DESC LIMIT 200"), hide_index=True, width="stretch")

    elif page == "SQL migration":
        page_title("SQL migration: legacy (Teradata-style) → Athena")
        st.caption("Tokenizer-based converter (functions, data types, schema names), Athena syntax check, then execution compared with PySpark. Unsupported syntax is reported, never silently guessed.")
        if st.button("Convert & validate all examples", type="primary"):
            cli("sqlmigrate")
        log = df("SELECT * FROM sql_migration_log ORDER BY id DESC LIMIT 8")
        for r in log.itertuples():
            with st.expander(f"{r.query_name} — {r.status}" + ("  ✅ result matches PySpark" if r.match == 1 else ""), expanded=r.Index == 0):
                a, b = st.columns(2)
                a.markdown("**Legacy**"), a.code(r.legacy_sql, language="sql")
                b.markdown("**Athena**"), b.code(r.converted_sql, language="sql")
                st.write("Rules applied:", json.loads(r.applied))
                if json.loads(r.unsupported):
                    st.warning(f"Unsupported: {json.loads(r.unsupported)}")
                st.caption(r.detail)
        st.subheader("Try your own")
        q = st.text_area("Legacy SQL", "SEL ZEROIFNULL(SUM(balance)) FROM legacy_db.account WHERE opened_date >= ADD_MONTHS(DATE '2026-06-30', -12);")
        conv = convert(q)
        st.code(conv.converted, language="sql")
        st.write(conv.status, conv.applied, conv.unsupported)

    elif page == "Security & masking":
        page_title("Security & PII masking")
        st.markdown("**Where is clear PII?**")
        st.dataframe(pd.DataFrame([
            {"stage": "Raw source files", "PII state": "Clear (it is the source system's data)"},
            {"stage": "Bronze and Silver Merge (local only)", "PII state": "Clear synthetic values as received; never uploaded to S3."},
            {"stage": "Silver Transform", "PII state": "Masked + token added. Real values go to the vault."},
            {"stage": "Silver / Gold tables", "PII state": "Masked value + token only. No clear PII."},
            {"stage": "PII vault (separate file)", "PII state": "Encrypted with AES-256-GCM (random nonce per value, bound to its row). Readable only through the role check."},
            {"stage": "Dashboard", "PII state": "Clear only if your role may unmask that column. Every reveal is audited."},
        ]), hide_index=True, width="stretch")
        st.warning("Role selection in this dashboard is a **simulation** (a dropdown), not authentication. It demonstrates role-based "
                   "unmasking and its audit trail; a real deployment would take the role from a login.")
        st.markdown("**Masking rules**")
        st.dataframe(df("SELECT * FROM masking_metadata"), hide_index=True)
        if role == "admin":
            from banklake import masking as _mk
            with st.expander("Change a masking rule or permission (admin, audited)"):
                c1, c2, c3 = st.columns(3)
                m_ent, m_col = c1.text_input("Entity", "customer"), c2.text_input("Column", "city")
                m_rule = c3.selectbox("Rule", _mk.RULES, index=_mk.RULES.index("REDACT"))
                if st.button("Save masking rule"):
                    _mk.set_rule(m_ent, m_col, m_rule, actor=role)
                    st.success("Saved. Applies the next time Silver Transform runs (reload silver_" + m_ent + ").")
                g1, g2 = st.columns(2)
                p_role = g1.selectbox("Role", meta.ROLES, key="perm_role")
                p_target = g2.text_input("Entity.column", "customer.email")
                b1, b2 = st.columns(2)
                if b1.button("Grant unmask") and "." in p_target:
                    _mk.grant_unmask(p_role, *p_target.split(".", 1), actor=role)
                    st.success("Granted.")
                if b2.button("Revoke unmask") and "." in p_target:
                    _mk.revoke_unmask(p_role, *p_target.split(".", 1), actor=role)
                    st.success("Revoked.")
        st.markdown("**Who may unmask what**")
        perm = pd.DataFrame(meta.query("SELECT role, entity, column_name FROM role_permissions"))
        grid = pd.DataFrame({"role": meta.ROLES})
        grid["can unmask"] = grid.role.map(lambda r: ", ".join(f"{x.entity}.{x.column_name}" for x in perm[perm.role == r].itertuples()).replace("*.*", "everything") or "nothing")
        st.dataframe(grid, hide_index=True, width="stretch")

        st.subheader(f"Try it as '{role}'")
        if has("silver", "customer"):
            cid = st.text_input("Customer ID to look up", value=Q(f"SELECT min(customer_id) AS c FROM {pq('silver', 'customer')}").c[0])
            row = Q(f"SELECT customer_id, customer_name, email, phone, customer_name_token, email_token, phone_token FROM {pq('silver', 'customer')} WHERE customer_id=?", [cid])
            if not row.empty:
                st.markdown("Stored in the lake:")
                st.dataframe(row[["customer_id", "customer_name", "email", "phone"]], hide_index=True)
                if st.button("Request unmasked values"):
                    for col in ["customer_name", "email", "phone"]:
                        try:
                            val = vault.reveal(role, "customer", col, [row[f"{col}_token"].iloc[0]])
                            st.success(f"{col}: {next(iter(val.values()), '(no vault entry)')}")
                        except PermissionError as e:
                            st.error(f"Denied - {e}")
                    st.caption("Both outcomes are written to the security audit log below.")
        st.subheader("PII leak scan")
        st.caption("Searches Bronze, Silver and Gold for clear values taken from the raw files. Expected: 0 matches everywhere.")
        if st.button("Run leak scan"):
            for ent, col in [("customer", "email"), ("customer", "phone"), ("customer", "customer_name"), ("account", "account_number")]:
                st.write(f"{ent}.{col}:", vault.leak_scan(ent, col))
        st.subheader("Access summary")
        st.dataframe(pd.DataFrame(vault.access_summary()), hide_index=True, width="stretch")
        st.subheader("Security audit log")
        st.dataframe(df("SELECT * FROM security_audit_log ORDER BY id DESC LIMIT 100"), hide_index=True, width="stretch")

    elif page == "Audit & monitoring":
        from banklake import monitor
        page_title("Audit & monitoring")
        h = monitor.health_summary()
        c = st.columns(4)
        c[0].metric("Pipelines tracked", h["pipelines"])
        c[1].metric("Latest SUCCESS", h["status_counts"].get("SUCCESS", 0))
        c[2].metric("Latest FAILED", h["status_counts"].get("FAILED", 0))
        c[3].metric("Latest BLOCKED", h["status_counts"].get("BLOCKED", 0))
        st.subheader("Alerts")
        al = monitor.alerts()
        if al:
            st.dataframe(pd.DataFrame(al), hide_index=True, width="stretch")
        else:
            st.success("No open alerts: every pipeline's latest run succeeded, reconciliation has no failed checks, no rejected files.")
        a, b = st.columns(2)
        a.markdown("**Freshness: last successful step per layer**")
        a.dataframe(pd.DataFrame(h["freshness"]), hide_index=True, width="stretch")
        b.markdown("**Rows rejected in each layer (latest run of each pipeline)**")
        b.dataframe(pd.DataFrame([{"layer": k, "rows_rejected": v} for k, v in h["rejected_by_layer"].items()]), hide_index=True, width="stretch")
        t1, t2, t3, t4 = st.tabs(["Pipeline runs", "Ingestion audit", "Transformation audit", "Errors"])
        with t1:
            log = df("SELECT run_id, run_type, layer, entity, pipeline_id, batch_id, start_ts, duration_s, rows_in, rows_out, rows_rejected, status, failure_reason, retry_count, error_message FROM pipeline_execution_log ORDER BY execution_id DESC LIMIT 500")
            if log.empty:
                st.caption("No pipeline runs recorded yet.")
                log = pd.DataFrame(columns=["layer", "entity", "status"])
            f1, f2, f3 = st.columns(3)
            fs = f1.multiselect("Status", ["SUCCESS", "FAILED", "BLOCKED", "SKIPPED", "RUNNING"])
            fl = f2.multiselect("Layer", sorted(log.layer.dropna().unique()))
            fe = f3.multiselect("Entity", sorted(log.entity.dropna().unique()))
            if fs:
                log = log[log.status.isin(fs)]
            if fl:
                log = log[log.layer.isin(fl)]
            if fe:
                log = log[log.entity.isin(fe)]
            st.dataframe(log, hide_index=True, width="stretch")
            st.caption("batch_id = the last source batch this pipeline had processed (its watermark) when the step ended.")
        with t2:
            st.dataframe(df("SELECT ts, run_id, batch_id, entity, source_file, source_system, load_type, records_read, records_loaded, records_rejected, status, message FROM ingestion_audit_log ORDER BY id DESC LIMIT 300"), hide_index=True, width="stretch")
        with t3:
            st.dataframe(df("SELECT ts, run_id, layer, entity, batch_id, load_type, rows_in, inserts, updates, deletes, ignored, duplicates_removed, rejected, rows_out FROM transformation_audit_log ORDER BY id DESC LIMIT 300"), hide_index=True, width="stretch")
        with t4:
            st.dataframe(df("SELECT run_id, pipeline_id, ts, substr(error,1,300) AS error FROM pipeline_error_log ORDER BY id DESC LIMIT 100"), hide_index=True, width="stretch")
        st.caption("Security events (unmask requests, denials, rule changes) are on the Security & masking page.")

    elif page == "Gold":
        page_title("Gold")
        st.caption(LAYER_DOCS["gold"] + " Only Silver and Gold leave this machine (S3).")
        tabs = st.tabs(["Branch performance", "Customer 360 (masked)", "Deposits", "Loans", "Transactions", "Lineage"])
        names = ["branch_performance", "customer_360", "deposit_summary", "loan_summary", "transaction_summary"]
        from banklake import lineage as _lin
        with tabs[5]:
            st.markdown("Which Silver table feeds which Gold table (live from the pipeline metadata), then column by column.")
            st.dataframe(pd.DataFrame([{"gold table": k, "built from Silver": ", ".join(v)} for k, v in _lin.table_lineage().items()]), hide_index=True, width="stretch")
            lt = st.selectbox("Gold table", list(_lin.GOLD_LINEAGE), key="lin_tbl")
            st.caption("Grain: " + _lin.GOLD_LINEAGE[lt][0])
            st.dataframe(pd.DataFrame(_lin.column_rows(lt)), hide_index=True, width="stretch")
            st.caption("Also written to docs/GOLD_LINEAGE.md (python main.py lineage); a test checks it against the real tables.")
        for tab, name in zip(tabs[:5], names):
            with tab:
                if not has("gold", name):
                    st.info("Run the Gold pipelines first.")
                    continue
                d = sample("gold", name, 1000)
                if name == "branch_performance":
                    st.bar_chart(d.groupby("region")["total_balance"].sum().astype(float))
                elif name == "deposit_summary":
                    st.bar_chart(d.pivot_table(index="maturity_year", columns="segment", values="total_deposits", aggfunc="sum").astype(float))
                elif name == "loan_summary":
                    st.bar_chart(d.set_index("segment")["total_loan_amount"].astype(float))
                elif name == "transaction_summary":
                    st.line_chart(d.pivot_table(index="txn_month", columns="txn_type", values="total_amount", aggfunc="sum").astype(float))
                st.dataframe(d, hide_index=True, width="stretch")

