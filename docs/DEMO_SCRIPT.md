# Five-minute demo script

Start: `.\scripts\run_demo.ps1`, open http://127.0.0.1:8000. All data is synthetic; say so first (banner at the top).

**1. Overview (30 s)** - 4 runs, ~12.5k stored records, 8 quarantined, duplicates ignored. "Batch / demo mode" badge: Kafka is optional and the UI says so honestly.

**2. Clean run (30 s)** - Campaigns & Runs > RUN-001-CLEAN: 0 issues, 0 findings, full timeline. Explain: no finding does not prove health.

**3. Data-quality run (90 s)** - RUN-002-DQ-DEFECTS: timeline shows a ~5 s feed-line dropout. Data Quality > filter this run: missing sequences, a time gap, 30 duplicate deliveries (INFO), 15 late events, out-of-order arrivals. Quarantine tab: 8 rejected records (bad unit, non-numeric, bad timestamp, unknown sensor, out-of-bounds...). Click one: original payload preserved. Export CSV.

**4. Anomaly with evidence (60 s)** - Anomalies > RUN-003-SUSPICIOUS: spike (95 bar vs 41.7 bar baseline), drop, flatline, drift, clock offset, phase glitch. Open AN_SPIKE: explanation generated from the numbers, evidence window chart, threshold, limitations. Mark one *False positive* with a note; re-analysis keeps it.

**5. Run comparison (60 s)** - Run Comparison > RUN-001 vs RUN-004: warnings that sampling intervals and durations differ; overlay with elapsed-time alignment (timestamps untouched); per-sensor change vs baseline (+4% thrust gain). Point out the footnote: a difference does not prove improvement.

**6. Report (15 s)** - click *HTML report*. Executive summary is computed, not written by a model.

**Optional pipeline moment**: with Kafka running, Ingestion page > start consumer + generator; watch counts and lag change; note 2% injected duplicates were ignored (idempotent storage), and that delivery is at-least-once, not "exactly-once".
