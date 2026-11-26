"""What each report shows. Four kinds, four layouts:
  Dashboard   fixed monitoring view: key numbers and several charts, nothing to configure
  Report      detailed fixed layout: the full table with totals, one supporting chart, export
  Regulatory  fixed-format return for compliance: figures must reconcile to Gold, share of each status, export
  BI          self-service: the person picks chart type, measure, breakdown
Column names are the real output columns of the Gold procedures; tests check every one against the loaded tables."""

TYPE_NOTE = {
    "Dashboard": "Fixed monitoring view: key numbers and several charts. Nothing to configure.",
    "Report": "Detailed report with a fixed layout: the full table with totals and one supporting chart.",
    "Regulatory": "Fixed-format return for compliance: the figures must reconcile to the Gold table.",
    "BI": "Self-service analysis: choose the chart type, measure and breakdown yourself.",
}

CO, DS, LS = "gold_customer_overview", "gold_deposit_summary", "gold_loan_summary"
FRAUD, FX, REPAY, SCORE = "gold_fraud_alert_summary", "gold_fx_rate_summary", "gold_loan_repayment_summary", "gold_credit_score_summary"

SPECS = {
    # ---------------------------------------------------------------- dashboards
    "Customer Overview Dashboard": dict(
        kpis=[("Customers", CO, "total_customers", "sum", "int"), ("Accounts", CO, "total_accounts", "sum", "int"), ("Total balance", CO, "total_balance", "sum", "inr")],
        panels=[dict(table=CO, chart="Donut", title="Customers by type", x="customer_type", y="total_customers"),
                dict(table=CO, chart="Column", title="Balance by customer type", x="customer_type", y="total_balance"),
                dict(table=CO, chart="Bar", title="Accounts by customer type", x="customer_type", y="total_accounts"),
                dict(table=CO, chart="Bubble", title="Customers vs accounts (size = balance)", x="total_customers", y="total_accounts", size="total_balance", color="customer_type")]),
    "Executive Summary": dict(
        kpis=[("Customers", CO, "total_customers", "sum", "int"), ("Account balances", CO, "total_balance", "sum", "inr"),
              ("Fixed deposits", DS, "total_deposits", "sum", "inr"), ("Loan book", LS, "total_loan_amount", "sum", "inr")],
        panels=[dict(table=CO, chart="Column", title="Account balances by customer type", x="customer_type", y="total_balance"),
                dict(table=DS, chart="Stacked column", title="Deposits maturing by year and segment", x="maturity_year", y="total_deposits", color="segment"),
                dict(table=LS, chart="Donut", title="Loan book by segment", x="segment", y="total_loan_amount"),
                dict(table=DS, chart="Area", title="Deposits maturing by month", x="maturity_month", y="total_deposits")]),
    "Loan Portfolio Dashboard": dict(
        kpis=[("Loans", LS, "n_loans", "sum", "int"), ("Loan book", LS, "total_loan_amount", "sum", "inr"), ("Average interest rate", LS, "avg_interest_rate", "mean", "pct")],
        panels=[dict(table=LS, chart="Column", title="Loan amount by segment", x="segment", y="total_loan_amount"),
                dict(table=LS, chart="Donut", title="Number of loans by segment", x="segment", y="n_loans"),
                dict(table=LS, chart="Bubble", title="Loans vs interest rate (size = amount)", x="n_loans", y="avg_interest_rate", size="total_loan_amount", color="segment"),
                dict(table=LS, chart="Funnel", title="Segments ranked by loan amount", x="segment", y="total_loan_amount")]),
    "Fraud Monitoring Dashboard": dict(
        kpis=[("Alerts", FRAUD, "n_alerts", "sum", "int"), ("Average risk score", FRAUD, "avg_risk_score", "mean", "dec"), ("Rules triggered", FRAUD, "rule_code", "nunique", "int")],
        panels=[dict(table=FRAUD, chart="Bar", title="Alerts by rule", x="rule_code", y="n_alerts"),
                dict(table=FRAUD, chart="Donut", title="Alerts by status", x="alert_status", y="n_alerts"),
                dict(table=FRAUD, chart="Stacked column", title="Alerts by rule and status", x="rule_code", y="n_alerts", color="alert_status"),
                dict(table=FRAUD, chart="Heatmap", title="Rule × status", x="alert_status", y="rule_code", v="n_alerts")]),
    "Treasury FX Dashboard": dict(
        kpis=[("Currency pairs", FX, "currency_pair", "nunique", "int"), ("Rates captured", FX, "n_rates", "sum", "int"), ("Highest sell rate", FX, "max_sell_rate", "max", "dec")],
        panels=[dict(table=FX, chart="Range", title="Buy-to-sell range by pair (log scale)", x="currency_pair", y="min_buy_rate", y2="max_sell_rate", log=True),
                dict(table=FX, chart="Column", title="Average mid rate by pair (log scale)", x="currency_pair", y="avg_mid_rate", log=True),
                dict(table=FX, chart="Bar", title="Rates captured by pair", x="currency_pair", y="n_rates"),
                dict(table=FX, chart="Scatter", title="Rates captured vs average mid rate", x="n_rates", y="avg_mid_rate", color="currency_pair", log=True)]),
    "Credit Risk Dashboard": dict(
        kpis=[("Amount due", REPAY, "total_due", "sum", "inr"), ("Amount paid", REPAY, "total_paid", "sum", "inr"), ("Penalties", REPAY, "total_penalty", "sum", "inr"),
              ("Average credit score", SCORE, "avg_score", "mean", "dec")],
        panels=[dict(table=REPAY, chart="Grouped column", title="Due, paid and penalty by repayment status", x="repayment_status", y=["total_due", "total_paid", "total_penalty"]),
                dict(table=REPAY, chart="Donut", title="Repayments by status", x="repayment_status", y="n_repayments"),
                dict(table=SCORE, chart="Heatmap", title="Scores by bureau and band", x="score_band", y="bureau", v="n_scores"),
                dict(table=SCORE, chart="Stacked column", title="Scores by band and bureau", x="score_band", y="n_scores", color="bureau")]),
    # ---------------------------------------------------------------- reports
    "Relationship Manager Report": dict(
        table=dict(table="gold_customer_360", cols=["customer_id", "customer_name", "city", "segment", "n_accounts", "total_balance", "total_deposits", "total_loans", "n_transactions"],
                   sort="total_balance", top=50),
        kpis=[("Customers", "gold_customer_360", "customer_id", "nunique", "int"), ("Total balance", "gold_customer_360", "total_balance", "sum", "inr"),
              ("Total deposits", "gold_customer_360", "total_deposits", "sum", "inr"), ("Total loans", "gold_customer_360", "total_loans", "sum", "inr")],
        panels=[dict(table="gold_customer_360", chart="Bar", title="Top 10 customers by balance", x="customer_id", y="total_balance", top=10)]),
    "Branch Performance Report": dict(
        table=dict(table="gold_branch_performance", cols=["branch_name", "region", "n_accounts", "n_active_customers", "total_balance", "avg_balance", "n_transactions", "txn_amount"], sort="total_balance"),
        kpis=[("Branches", "gold_branch_performance", "branch_id", "nunique", "int"), ("Accounts", "gold_branch_performance", "n_accounts", "sum", "int"),
              ("Total balance", "gold_branch_performance", "total_balance", "sum", "inr")],
        panels=[dict(table="gold_branch_performance", chart="Bar", title="Top 12 branches by balance", x="branch_name", y="total_balance", top=12)]),
    "Deposit Maturity Report": dict(
        table=dict(table=DS, cols=["segment", "maturity_year", "maturity_month", "n_deposits", "total_deposits", "avg_deposit"], sort="maturity_year", ascending=True),
        kpis=[("Deposits", DS, "n_deposits", "sum", "int"), ("Total deposits", DS, "total_deposits", "sum", "inr")],
        panels=[dict(table=DS, chart="Stacked column", title="Deposits maturing by year and segment", x="maturity_year", y="total_deposits", color="segment")]),
    "Card Portfolio Report": dict(
        table=dict(table="gold_card_portfolio", cols=["card_type", "network", "card_status", "n_cards", "avg_daily_limit"], sort="n_cards"),
        kpis=[("Cards", "gold_card_portfolio", "n_cards", "sum", "int"), ("Average daily limit", "gold_card_portfolio", "avg_daily_limit", "mean", "inr")],
        panels=[dict(table="gold_card_portfolio", chart="Stacked column", title="Cards by type and status", x="card_type", y="n_cards", color="card_status")]),
    "Complaints Report": dict(
        table=dict(table="gold_complaint_summary", cols=["category", "severity", "complaint_status", "n_complaints", "avg_resolution_days"], sort="n_complaints"),
        kpis=[("Complaints", "gold_complaint_summary", "n_complaints", "sum", "int"), ("Average days to resolve", "gold_complaint_summary", "avg_resolution_days", "mean", "dec")],
        panels=[dict(table="gold_complaint_summary", chart="Stacked column", title="Complaints by category and severity", x="category", y="n_complaints", color="severity")]),
    "Loan Collections Report": dict(
        table=dict(table=REPAY, cols=["repayment_status", "n_repayments", "total_due", "total_paid", "total_penalty"], sort="total_due"),
        kpis=[("Repayments", REPAY, "n_repayments", "sum", "int"), ("Amount due", REPAY, "total_due", "sum", "inr"), ("Amount paid", REPAY, "total_paid", "sum", "inr")],
        panels=[dict(table=REPAY, chart="Grouped column", title="Due, paid and penalty by status", x="repayment_status", y=["total_due", "total_paid", "total_penalty"])]),
    # ---------------------------------------------------------------- regulatory returns
    "Regulatory Complaints Return": dict(
        table=dict(table="gold_complaint_summary", cols=["category", "severity", "complaint_status", "n_complaints", "avg_resolution_days"], sort="category", ascending=True),
        share=dict(table="gold_complaint_summary", by="complaint_status", measure="n_complaints"),
        panels=[dict(table="gold_complaint_summary", chart="100% stacked", title="Complaint status mix by category", x="category", y="n_complaints", color="complaint_status")]),
    "KYC Compliance Report": dict(
        table=dict(table="gold_kyc_status_summary", cols=["verification_status", "segment", "n_customers"], sort="segment", ascending=True),
        share=dict(table="gold_kyc_status_summary", by="verification_status", measure="n_customers"),
        panels=[dict(table="gold_kyc_status_summary", chart="100% stacked", title="Verification status mix by segment", x="segment", y="n_customers", color="verification_status")]),
    # ---------------------------------------------------------------- BI (self-service); preset = what it opens with
    "Customer 360 View": dict(tables=["gold_customer_360"], preset=dict(chart="Column", x="segment", y="total_balance")),
    "Regional Branch Scorecard": dict(tables=["gold_branch_performance", "gold_transaction_summary"], preset=dict(chart="Bar", x="region", y="total_balance")),
    "Monthly Transaction Trends": dict(tables=["gold_transaction_summary"], preset=dict(chart="Line", x="txn_month", y="total_amount", color="txn_type")),
    "Marketing Campaign Report": dict(tables=["gold_campaign_response_summary"], preset=dict(chart="Stacked column", x="campaign_code", y="n_responses", color="response")),
}
