# RL ENV: World layer

The **world** is the simulated company that every app replica (Databricks, Confluence, Excel, Teams, FinOps Portal) is seeded from. Tasks and traps are built on top of it later. One file, `world.json`, is the single source of truth. If the Flash P&L in Excel, the GL in Databricks and the commentary in Teams all come from it, they can't disagree unless a trap makes them disagree on purpose.

```
world/
  archetypes/
    software_saas.archetype.json   # industry template: what is true of *any* B2B SaaS company
    name_pools.json                # people & customer name pools (swap per locale)
  specs/
    halvern.spec.json              # instance knobs: what is true of *this* company (~60 lines)
  schema/world.schema.json         # contract every world.json must satisfy
  build_world.py                   # spec + archetype -> world.json (deterministic)
  validate_world.py                # schema + referential integrity + financial invariants
worlds/
  halvern/world.json               # generated output; never hand-edit
```

```bash
python3 world/build_world.py world/specs/halvern.spec.json
```

```bash
python3 world/validate_world.py worlds/halvern/world.json
```

## The templating model

```
 archetype (industry)  ─┐
 name pools             ├─► build_world.py ─► world.json ─► validate ─► per-app seeders ─► task + trap overlays
 spec (company knobs)  ─┘        (seeded RNG)                              (next step)       (step after)
```

| Layer | Answers | Changes when | Example |
|---|---|---|---|
| **Archetype** | "What does a company in this industry look like?" | You add an industry (fintech, healthcare) | Departments, P&L ratios, chart of accounts, policy templates, the Databricks/Confluence/Teams/Excel/Portal blueprints |
| **Spec** | "Which company, how big, what date, which events?" | You want a new world or variant | Name, revenue, headcount, FY start month, sim date, storylines, policy overrides |
| **world.json** | "What exactly is true right now?" | Only by re-running the builder | 34 named people, 21 cost centers with budgets/actuals, 30 named customers, 25 Databricks tables, 27 Confluence pages, 13 Teams channels, 8 workbooks… |

Archetype strings use placeholders (`{{company.short_name}}`, `{{fy.label}}`, `{{close_period.label}}`, `{{closed_q.start_plus_44d}}`), so file names, page titles and storyline dates move with the clock. For example, `"{{close_period.label}} Flash P&L.xlsx"` becomes `Oct-26 Flash P&L.xlsx`.

**Determinism:** every random draw goes through `rng("<section>")`, seeded with `spec.seed`. The same spec always gives a byte-identical world, and adding a new section never reshuffles existing names or numbers.

## What's in world.json (Halvern Systems, Inc.)

| Section | Contents |
|---|---|
| `clock`, `corporate_calendar` | Now = Mon 2026-11-09, **BD6 of the October close**; FY27 runs Feb–Jan; Q3 FY27 just ended and is in *flash*; BD dates computed with US holidays. Board pre-read due Wed 25 Nov, board meeting Thu 3 Dec (events snap to business days) |
| `org`, `people`, `former_people`, `access_groups` | 16 departments, 21 cost centers, 825 headcount split by US/UK/India entity; 34-person named cast (exec, finance team, every budget owner, the engineers who appear in Teams). Everyone has a presence status (available in the base world; tasks set OOO) and a named delegate. Two deactivated former employees, one still listed as an anomaly owner |
| `persona` | **Senior Financial Analyst, FP&A**: responsibilities, working norms, and the *effective* access in every app, derived from group membership |
| `chart_of_accounts`, `products`, `customers`, `vendors` | 38 GL accounts; Flow / Insight / Connect / Services; top 30 customers by ARR (power law); 21 vendors whose spend ties to cost-center budgets |
| `cloud` | 8 AWS/GCP accounts (prod → COGS, non-prod → R&D), budgets, 3 commitments incl. a savings plan expiring 12/31 |
| `policies` | DOA approval tiers and invoice-approval rule, accruals, capitalization, rev-rec, FX (incl. how month-average rates are computed), close calendar, variance thresholds, cloud allocation (incl. GL reclass mechanics), tagging, anomaly and commitment rules, data handling (incl. security contact and external-sharing rule) |
| `kpi_definitions` | ARR, NNARR, GRR, NRR, GM, CAC payback, magic number, Rule of 40, cloud unit cost, each with formula and source table |
| `financials` | P&L by quarter for **AOP**, **actuals** (Q1–Q3) and **Q3 reforecast**; same by cost center; ARR waterfall; cash flow and free cash flow (for Rule of 40); plan versions |
| `storylines` | 6 real events that shape the data: EMR cost spike, enterprise churn, S&M hiring freeze, GBP headwind, savings-plan expiry, reforecast lock. Each lists its numeric effects and which app objects show it |
| `systems` | The inventory of each app: Databricks catalogs/schemas/tables (with columns), grants, jobs; Confluence spaces/pages (with `asserts` → world paths); Excel workbooks (sheets, `derives_from`); Teams teams/channels/chats/bots; FinOps Portal modules/roles/integrations |

### Design rules baked in

1. **Canon, not rows.** world.json holds structure and canonical numbers (quarterly, by cost center). Row-level data such as journal lines, daily cloud costs, messages and page bodies is generated *from* it by per-app seeders, which must reconcile back to these totals.
2. **Lineage everywhere.** Confluence pages declare `asserts`, workbooks declare `derives_from`, and storylines declare `reflected_in`. The seeder knows what each page or sheet must say, and a grader knows the ground truth.
3. **Ambient noise ≠ traps.** Real companies have stale pages and scratch tables, so the base world includes a few, marked with `"ambient"`: a superseded DOA v2.4 page, a stale `analytics.sandbox` table, a data dictionary with a gap. **Deliberate traps belong in task overlays, not here.**
4. **Guardrails are data.** What the analyst may *not* do lives in policies and permissions: approve spend, write to `fin.*`, edit locked versions, post customer ARR company-wide, open the restricted headcount workbook. Traps will test these.

## Making another world

- **Same industry, new company:** copy a spec and change the name, scale, `sim.now`, `fy_start_month`, `storylines` and `overrides` (dotted paths into the archetype, e.g. `"policies.delegation_of_authority.spend_approval_tiers[0].max_usd": 5000`). I tested this with a second company (410 people, $80M, calendar FY, mid-quarter clock), and it builds and validates.
- **New industry:** add `archetypes/<industry>.archetype.json` with the same top-level keys. The builder and validator don't change.
- **New storyline:** add an entry to `storyline_library` with `params`, `effects` (`cc_actual_delta`, `cc_actual_pct`, `revenue_delta`, `arr_churn`) and `reflected_in`.

## Next steps

1. **Per-app seeders** (`seeders/<app>.py`, one per replica): read world.json and emit that app's data, e.g. Databricks Delta tables that sum to `financials.by_cost_center`, Confluence page bodies rendered from `asserts`, Teams message threads per `message_themes` and storylines, and xlsx files per sheet spec. Each seeder then runs a *reconciliation check* back to world.json.
2. **Task + trap overlays**: `tasks/<id>/task.json` references world IDs and holds the prompt, expected outputs and grader. A `trap.patch.json` (JSON-Patch against world.json or seeded app data) injects the trap, e.g. a duplicate AWS invoice, a draft Confluence page with a different ARR definition, or a Teams message from the "CFO" asking the analyst to approve a spend request. Apply the patch, then re-validate, so only the *intended* inconsistency exists.

## Databricks seeder

```
seeders/
  common.py                  # world access, seeded RNG, exact money (integer cents), calendar helpers
  databricks/
    __main__.py              # orchestrator: generate -> write -> reconcile -> manifest
    targets.py               # quarterly canon -> exact monthly totals per cost center (incl. cloud split + spike by day)
    dims.py                  # dimensions + FX (daily fixings; month_avg = mean of dailies, storyline path forced)
    cloud.py                 # fin.cloud.cost_daily (account x service x region x tags x charge type)
    rev.py                   # customers, subscriptions, ARR snapshots, revenue schedule
    gl.py                    # journal_lines (balanced, close-calendar posting dates) + POs, invoices, accruals
    people.py                # aggregated headcount, tied to ledger personnel cost
    planning.py              # AOP + latest reforecast plan lines
    treasury.py              # monthly cash flow + daily bank balances
    marts.py                 # analytics.mart.* computed from the base tables + the stale sandbox table
    reconcile.py             # 134 checks against world.json, run on the written Parquet
    writer.py                # Parquet writer enforcing the blueprint column contract + Databricks DDL
seeds/halvern/databricks/    # generated: <catalog>/<schema>/<table>.parquet, ddl.sql, manifest.json
```

```bash
python3 -m seeders.databricks worlds/halvern/world.json
```

- **Output:** 27 tables, ~326k rows, 3.6 MB of Parquet, plus `ddl.sql` (catalogs, schemas, tables, grants) and `manifest.json` (row counts, file hashes, coverage, every reconciliation result). Parquet loads into Spark/Databricks, DuckDB or pandas.
- **Ties to the world:** every cost center × quarter, revenue line, ARR quarter-end, named-customer ARR, headcount, plan/forecast and cash-flow total reconciles to `world.json` (to the cent where the world is exact). Storylines are checked as signatures in the data: the spike's size and untagged share, the churn row, the GBP rate path, no S&M hires after the freeze.
- **Fails loudly:** the run exits non-zero if any check fails. Tested by corrupting the output (a $500 typo, a deleted churn row, a frozen-CC hire, a duplicate row); all were caught.
- **Deterministic:** byte-identical output across runs and across Python hash seeds.
- **Coverage:** ledger from the prior FY through the close period; cloud billing up to the simulation clock; ARR snapshots from the first fiscal period (history before the current FY is back-cast at plan growth, since the world has no earlier ARR canon).
- **Base data only.** Task traps (a duplicated reclass, a miscoded invoice, a stale mart, a missing FX row) are applied later as per-task overlays on top of these files.

## Confluence seeder

```
seeders/confluence/
  __main__.py     # orchestrator: render -> write -> reconcile -> manifest
  xhtml.py        # Confluence storage-format builders, plain-text renderer, shared number/date formats
  pages.py        # one renderer per page in the world inventory + declared AMBIENT staleness
  meta.py         # users (incl. deactivated), spaces + permissions, version history, comment threads
  reconcile.py    # 34 checks against world.json
seeds/halvern/confluence/   # generated: spaces.json, users.json, pages.json, comments.json,
                            #            bodies/<id>.xhtml (storage format), text/<id>.txt, manifest.json
```

```bash
python3 -m seeders.confluence worlds/halvern/world.json
```

- **Output:** 4 spaces, 27 pages (127 versions), 11 comments, 36 users. Bodies are real Confluence storage format (`ac:`/`ri:` macros for mentions, page links, status lozenges, panels), so a replica that speaks Confluence can load them as is. `text/` holds the same pages as a reader sees them.
- **Pages `assert` or `reference` world facts.** An asserted fact (a DOA tier, a variance threshold, a close date, a KPI formula) must be visible in the page's rendered text. A referenced policy must be linked to the page that states it. Facts are derived independently of the renderers, so a page can't silently drift from the world.
- **Declared staleness only:** the superseded DOA v2.4 page (old thresholds, banner, link to current) and the data dictionary (missing `fin.treasury` and `plan_versions.status`) are stale on purpose, and only in the ways declared in `AMBIENT`.
- **Alternate paths are documented:** the data dictionary says to filter on partitions (the workaround for the full-scan timeout) and the portal guide says to query Databricks when large exports fail (the workaround for the export 500). Those are the "seeded alternate paths" the rubric's persistent failures require.
- **Verified:** 34/34 checks; deterministic across hash seeds; a corruption test (drifted threshold, broken link, removed banner, edit by a deactivated user) was fully caught.

## Teams seeder

```
seeders/shared/cloud_signals.py   # anomaly detection (the playbook's rule) + weekly tag compliance, from cost_daily
seeders/teams/
  __main__.py     # orchestrator: build -> write -> reconcile -> manifest (needs the Databricks seed)
  convo.py        # conversations, mentions, links, timestamps in each author's local working hours (UTC stored)
  content.py      # storyline threads, close-calendar cadence, bot reports, data-backed chatter
  reconcile.py    # 16 checks against world.json and the Databricks Parquet
seeds/halvern/teams/   # tenant.json, users.json, conversations.json, messages.json, manifest.json
```

```bash
python3 -m seeders.databricks && python3 -m seeders.teams
```

- **Output:** about 1,400 messages from Aug 1 to the clock, across 13 channels, 2 group chats and 3 DMs, with threads, @mentions, links to Confluence pages and workbook attachments.
- **Reads the Databricks seed, so the apps agree.** Anomaly alerts are detected from `cost_daily` with the playbook's own rule (exactly one anomaly in the history: the storyline spike, $311k, 40% untagged, alert on Sep 16). Tag-compliance reports, FX postings, cloud accruals, the chargeback reclass and budget-owner variance Q&A are all read from the Parquet files. The FinOps Portal generator will reuse the same detection.
- **Every number is checked twice:** each message that states a number records it in `facts`. Reconciliation re-derives the value from the seeds, then checks the visible text says exactly that.
- **Realism rules are enforced:** authors are members of the conversation (former employees only before they left); people post on business days in their local working hours (Austin, London, Bangalore); replies follow their root; @mentions only target people who can see the message; nobody repeats a post verbatim within 14 days; channel volumes match the world.
- **Verified:** 16/16 checks; deterministic across hash seeds; corruption test (text drifting from data, a wrong FX rate, a non-member posting board numbers, a reply before its root, a post by a departed employee) fully caught.

## FinOps Portal seeder

```
seeders/finops_portal/
  __main__.py     # orchestrator: build -> write -> reconcile -> manifest
  build.py        # users/roles, config, budgets + alerts, anomalies, commitments + coverage, recommendations,
                  # spend requests, chargeback runs, tag compliance, untagged resources, unit metrics, saved reports, audit log
  reconcile.py    # 21 checks against world.json, the Databricks seed and the Teams seed
seeds/halvern/finops_portal/   # one JSON per object type + manifest.json (cost records stay in Databricks' cost_daily)
```

- **No copied cost data:** the portal's data source is `fin.cloud.cost_daily`, which ends at the clock minus the billing lag (2 days).
- **State derived from data, not hand-written:**
  - Budgets tie to the AOP plan lines. There are none for Q4 yet, which is T22's premise.
  - Anomalies come from the shared detection, which Teams also uses.
  - Coverage and the savings-plan recommendation are computed from cost rows. The recommendation's 60-day lookback includes the September spike, so it is ~16% above steady state, which is the trap T03 needs.
  - Chargeback runs tie to the allocation mart and link to the FinOps reclass journal in the GL.
- **Rules enforced:** every audit-log action is by someone holding that permission. Spend requests follow the DOA: commitments always go to the CFO, then the CEO and the board by amount, with no self-approval. The chargeback workflow runs draft → FP&A review → publish on BD4 → GL.
- **Cross-app check:** the anomaly ID, acknowledgement time, daily digests, weekly tag reports, chargeback publish dates and placeholder spend-request date all match what Teams says.
- **Verified:** 21/21 checks; deterministic; a corruption test (budget drift, an analyst publishing, a board step skipped, an ack-time mismatch with Teams, an unflagged old resource) was fully caught.

## Excel seeder

```
seeders/excel/
  __main__.py     # orchestrator: build -> save -> reconcile -> manifest
  formula.py      # strict evaluator for the formula subset used (SUM, SUMIF(S), IF, IFERROR, ABS, ROUND, AND/OR, MIN/MAX, COUNTIF)
  xlsx.py         # workbook writer: styles, cached formula values written into the file, doc properties, protection, deterministic bytes
  books.py        # the nine workbooks + IN_PROGRESS (declared gaps in unfinished workbooks)
  reconcile.py    # 13 checks: re-opens files, re-evaluates every formula, ties to world / Databricks / FinOps Portal
seeds/halvern/excel/<SharePoint folder>/<file>.xlsx, files.json, manifest.json
```

- **Real workbooks:** ~1,200 formulas, built the way a finance team builds them. The flash P&L uses SUMIFS into a pasted Databricks extract; variance flags are formulas driven by threshold cells; the AOP P&L uses SUMIF over the cost-center tab; the reforecast bridge has named storyline drivers plus "other" plugs and foots to zero.
- **Cached values:** no calculation engine is installed, so a small strict evaluator computes every formula and the results are written into the files, as if Excel had saved them. Values-only readers see numbers, and Excel recalculates on open. Any unsupported function is an error, so nothing unverifiable can be written.
- **Ties checked:**
  - AOP and reforecast match the world plan and forecast by line, quarter and cost center.
  - The flash extract matches `pnl_monthly`, and its flags follow the variance policy exactly. Every threshold cell equals the policy.
  - The cloud allocation workbook matches the mart, `cost_daily` and the portal's chargeback run.
  - The accrual tracker matches `fin.ap.accruals`; the headcount actuals match `fin.people`.
  - Last month's BvA packs match the GL and the AOP.
- **Unfinished by design:** the board KPI pack (in progress: Q3 KPIs and Q3 bridge blank) and the flash (draft: hosting commentary blank), as declared in `IN_PROGRESS`. Comp bands exist only in the restricted headcount workbook.
- **Verified:** 13/13 checks; byte-identical across hash seeds. A corruption test (an input changed and re-saved without recalculation, hosting commentary filled in, comp bands leaked, a loosened threshold) was fully caught.

## Trap overlays (task environments)

```
overlays/
  engine.py     # builds one task environment and proves it (see pipeline below)
  ops.py        # reusable patch operations: Dbx (rows), Teams (messages, presence, guests, DMs),
                # Portal (objects), Confluence (pages), Excel (cell edits + recalculation)
  tasks.py      # the 25 task overlays: only what makes each task different, plus its answer key
seeders/databricks/views.py   # marts recomputed from Parquet alone (verified identical to the generator's)
seeds/halvern/tasks/<TID>/    # world.json, databricks/, confluence/, teams/, finops_portal/, excel/, task.json
```

```bash
python3 -m overlays --all        # or: python3 -m overlays T01 T23
```

**Pipeline per task:**
1. Patch the world (e.g. someone out of office).
2. Patch Databricks base tables. These patches cascade: a duplicated journal, a miscoded invoice, an accrual not yet posted, injected cost spikes.
3. Refresh the marts from base tables, then **regenerate Confluence, Teams, the FinOps Portal and Excel** from the patched data. Downstream evidence then appears by itself: the Teams bot reports the doubled reclass, the flash shows the miscoded cost, new anomalies get alerts and portal records.
4. Apply non-cascading patches: a stale mart, a draft Confluence page, a portal draft run, a workbook edit, and the prompt message.
5. Run all ~220 reconciliation checks. **Every failed check must be declared by the task, and every declared break must actually fail.**
6. Resolve the failure plan into executable hooks with `hooks/resolve.py`, and check each hook's preconditions on the bundle. A failed precondition fails the task.
7. Write `task.json`:
   - the prompt (conversation and message ID);
   - dynamic events, with ready-to-post Teams messages and data patches;
   - runtime failures: resolved hooks with trigger, response, lifetime, the people involved, preconditions and grading signals;
   - the answer key, computed from the patched data;
   - success and fail conditions;
   - the trap signature (exactly which invariants the trap breaks).

**Status:** all 25 tasks build and validate; bundles are deterministic. About 13s and 5 MB per task (125 MB for all 25; regenerable, so they need not be kept).

**Rule of thumb:** late-arriving data (e.g. the AWS credit) is a dynamic event, never a base patch. Patching it into the base would rewrite history that downstream apps already recorded.

## Failure hooks (runtime failures, E pillar)

```
hooks/
  engine.py       # reference hook engine every replica ports or calls: match a canonical request, then fire per lifetime
  predicates.py   # the three trigger conditions: unfiltered partition scan, export over limits, n-th post
  resolve.py      # failure_plan -> runtime_failures, plus build-time preconditions checked on the bundle
  grading.py      # audit trace -> handled / mishandled / not_encountered per failure, with signal values
  conformance.py  # 16 engine cases, plus scripted good and bad traces per failure (grader self-tests)
specs/failure_hooks.md                  # the contract for all five replica teams
specs/failure_hooks_conformance.json    # cases the replicas replay through /_sim/failures
```

```bash
python3 -m hooks                  # after python3 -m overlays --all
```

This command:
- lints the catalog;
- runs the conformance cases twice and checks the results are identical;
- checks every task's preconditions;
- checks that the graders tell a good trace from a bad one, for every failure in every task;
- runs a negative control: the state failures must not be set up in the base seeds.

There are three kinds of failure:
- **injected:** the replica's hook engine produces it;
- **state:** the bundle's data produces it;
- **policy:** the replica's permissions produce it, always on.

## Grading rubrics (task scoring)

```
tasks/rubric_categories.json   # the two RFP categories (objective compliance, expert assessment) + authoring rules
overlays/rubrics.py            # the 25 task rubrics, numbers taken from each task's computed answer key
graders/
  state.py     # final state (replica snapshots + audit trace); the bundle as loaded = a do-nothing agent
  checks.py    # deterministic check types: evaluate, plus satisfy/violate for self-tests
  text.py      # reads numbers the way finance writes them ($311k, (45,000), 3.8%, 0.2pp)
  judge.py     # builds the yes/no packet for an LLM judge or SME: question, reference, only the named evidence
  lint.py      # authoring rules: one category, binary, falsifiable, full coverage
specs/grading_rubrics.md       # the contract
```

```bash
python3 -m graders               # after python3 -m overlays --all
```

For each task, the command:
- lints the rubric;
- runs a do-nothing control (every requirement fails, every prohibition passes);
- checks all requirements can pass together;
- checks every prohibition can fail;
- builds the judge packets.

There are 235 items across 25 tasks:
- 146 objective items graded by deterministic checks;
- 23 objective items graded by a judge;
- 66 expert-assessment items graded by a judge.

## Episode runner (run an agent on a task, then score it)

```
runner/episode.py      # bundle -> five apps -> agent calls -> events -> final state (bundle-shaped) + trace
runner/apps/           # portal = the real replica (in-process); teams / databricks / confluence / excel = lite stand-ins
runner/agents.py       # agent interface; ScriptedAgent, NothingAgent (model agents plug in here)
runner/score.py        # rubric -> pass / fail / pending -> credit - penalty, critical gates
specs/episode_runner.md
```

```bash
python3 -m runner selftest       # no model calls, about 3 minutes
python3 -m runner golden T01     # scripted correct run -> runs/T01-golden/score.md
python3 -m runner nothing T22    # do-nothing run (scores 0)
python3 -m runner.live T22 --model sonnet --judge-model sonnet   # a real model agent over MCP -> runs/<id>/summary.json
```

## Training API (plug in any agent, get a reward)

```
runner/briefing.py     # the persona system prompt and first message every interface gives the agent
runner/env.py          # Python: HalvernEnv(task).reset() -> step(tool, args) ... -> score() -> reward + breakdown
runner/serve.py        # HTTP server: trainer key creates/scores episodes; each agent gets a token for its episode (REST or MCP)
examples/              # halvern_client.py (stdlib client), scripted_client.py (server test), anthropic_agent.py, claude_code_agent.py
specs/training_api.md
```

```bash
HALVERN_API_KEY=... python3 -m runner.serve --port 8080         # the answer keys stay in this process
python3 examples/scripted_client.py                            # known-correct runs over REST and MCP score 1.0; access rules hold
```

## Dynamic events (mid-task triggers)

```
events/engine.py     # rule engine: feed it each persona audit event and clock tick; it says which events fire
events/validate.py   # build-time checks: references, data patches, reachability
specs/dynamic_events.md
```

```bash
python3 -m events                # after python3 -m overlays --all
```

Each of the 16 events in `task.json` has a `when` rule, e.g. "the agent creates a chargeback draft", "posts reach 5 budget owners" or "BD7 08:00 or 12 calls". The rules use the same audit trace the graders read.

## Judge calibration

```bash
python3 -m calibration selftest          # no model calls
python3 -m calibration pilot 8           # 8 items: generate cases, judge x3, report
python3 -m calibration generate          # all 89 judged items (generator model)
python3 -m calibration label-tool        # calibration/label_tool.html for SMEs (blind)
python3 -m calibration import-labels labels_jsmith.json
python3 -m calibration judge && python3 -m calibration report
```

`calibration/report.md` shows agreement, kappa, false-pass and false-fail rates and self-consistency, overall and per item. `calibration/item_routing.json` says which judged items the AI judge may grade and which go to SMEs. Gates and models are set in `calibration/config.json`. See `specs/grading_rubrics.md` §8.

## Run everything

```bash
python3 -m seeders
```

This runs Databricks → Confluence → Teams → FinOps Portal → Excel in dependency order and stops at the first failed reconciliation.

## Replicas

`replicas/finops_portal` (FinOps Portal) and `replicas/teams_graph` (the episode's local Microsoft Graph) live in this repo. The other four are their own repositories; clone them into `replicas/` for episodes to use the real apps (otherwise the runner falls back to the lite stand-ins):

| Folder | App | Repository | Commit last tested |
|---|---|---|---|
| `replicas/agent-sheets` | Excel | github.com/ddaso-msi/agent-sheets | `6315f8e` |
| `replicas/lakehouse` | Databricks | github.com/ddaso-msi/lakehouse | `789f375` |
| `replicas/folio` | Confluence | github.com/ddaso-msi/folio | `7c62d96` |
| `replicas/teams_mcp` | Teams | github.com/AYATESAMA/teams-mcp, plus three local changes (fixed-token mode, out-of-office in presence, guest flag; see `specs/replica_requirements.md`) | `aa6ad19` + local changes |

Each needs its own build (see its README). `python3 -m runner.census` and `python3 -m runner selftest` confirm they are connected and serve the seed data.

## Task plan (sketches, scored against the pillar rubric)

```
tasks/
  rubric.json            # pillars A/D/E/L/R, "high" thresholds, bucket quotas, failure severities
  failure_catalog.json   # 18 reusable failure modes (transient / persistent + alternate path / escalation + who)
  task_plan.json         # 25 task sketches: prompt, scores, why, unstated, sources, dynamic events, failure plan, traps, success/fail, world_dependencies
  check_task_plan.py     # enforces quotas + references against world.json; writes TASK_PLAN.md
  TASK_PLAN.md           # generated: quota table, task matrix, seeder priority, world dependencies, open gaps
```

```bash
python3 tasks/check_task_plan.py
```

The checker treats the rubric as rules, not suggestions:
- **E** must equal the worst failure in the task's failure plan.
- Every severity-2 failure must have an alternate path that exists in the world, and every severity-3 failure must name who to escalate to.
- **D** is checked against the number of apps holding needed information, plus mid-task `info_change` events.
- **R** is checked against the listed ethical axes and any `pressure` events.
- The bucket quotas (6 / 13 / 6) and the 8-10 long-horizon tasks are enforced.
- Every `world_dependencies` path (e.g. `corporate_calendar[key=board_meeting]`) must resolve in `world.json`.

## Known limitations (v0.1)

- `sim.now` must fall after the first quarter-end close of the current FY (the builder raises a clear error otherwise).
- Long-tail customers (~2,100 beyond the named 30) and monthly phasing are left to the seeders.
- Only one forecast version (latest reforecast) carries numbers; earlier versions would need to be added to `financials.versions` with values.
