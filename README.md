E-Commerce KPI Analysis (SQL)
Cleaning legacy orders data and delivering 10 trusted business KPIs.

1) Business context
A retail team inherited a legacy e-commerce orders extract from an acquisition.
The data is usable, but not trustworthy: mixed date formats, inconsistent categories, duplicates, and missing values.
The goal is to build one clean “source of truth” table and compute 10 business KPIs for reporting.

2) What this repo contains
sql/data_cleaning_pipeline.sql → raw → typed/parsing → rules → dedup → clean_table
sql/kpi_calculations.sql → kpi_1 … kpi_10 → kpi_results
assets/ → screenshots (row counts, KPI output, before/after checks)

3) KPIs delivered
KPI 1: Average Order Value (AOV) — average revenue per valid order.
KPI 2: Gross Margin % — profit share after cost.
KPI 3: Return Rate — share of orders returned.
KPI 4: Orders by Segment — how volume splits by customer tier.
KPI 5: Segment GMV Share — how revenue splits by tier.
KPI 6: High-Value Segment GMV Share — revenue share from premium tiers.
KPI 7: Peak Hour — which hour sees the most orders.
KPI 8: Top Month by GMV — best month by sales.
KPI 9: Latest MoM GMV Growth — most recent month vs the prior month.
KPI 10: Max MoM Payment Share Shift — biggest month-to-month change in payment mix.

4) Data issues and how they were handled
Mixed date formats → multi-format parsing into a single typed date.
Category/segment typos → normalised mapping into a small, consistent set of values.
Invalid values (e.g. negative amounts, impossible hours) → rule-based filtering with counts reported.
Duplicates → deduplicated on a strict business key (documented in the SQL).

5) Results and validation
Raw rows: 10,000
Clean rows after rules + dedup: 9720
Validation checks included: 4
row-count checkpoints at each stage
null-rate checks on key fields
duplicate collision counts on the business key
KPI outputs standardised into kpi_results
Screenshots: see assets/ for “before vs after” counts and KPI output.

6) Tech stack
SQL (CTEs, CASE, window functions, data-quality checks)
Execution environment: Verulam Blue Mint SQL Environment
Version control: Git

7) Key learnings
Profiling first makes cleaning decisions measurable and defensible.
Data-quality rules are business decisions, not just technical ones.
A clean, shared denominator (clean_table) prevents silent KPI drift.

