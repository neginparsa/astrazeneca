-- Dashboard-friendly views (catalog: main). Run after QUICKSTART or gold notebooks.

CREATE OR REPLACE VIEW main.gold.v_dashboard_kpis AS
SELECT
  SUM(active_patients) AS total_active_patients,
  ROUND(SUM(total_paid_amount), 2) AS total_paid_amount,
  COUNT(DISTINCT therapy_code) AS therapy_count,
  ROUND(AVG(avg_days_supply), 1) AS avg_days_supply,
  MAX(metric_date) AS latest_metric_date
FROM main.gold.daily_therapy_metrics
WHERE metric_date >= date_sub(current_date(), 120);

CREATE OR REPLACE VIEW main.gold.v_dashboard_daily_trend AS
SELECT
  metric_date,
  therapy_code,
  active_patients,
  total_paid_amount,
  avg_days_supply,
  new_starts,
  discontinuations
FROM main.gold.daily_therapy_metrics
WHERE metric_date >= date_sub(current_date(), 120)
ORDER BY metric_date, therapy_code;

CREATE OR REPLACE VIEW main.gold.v_dashboard_therapy_summary AS
SELECT
  therapy_code,
  SUM(active_patients) AS patient_fills,
  ROUND(SUM(total_paid_amount), 2) AS paid_amount,
  ROUND(AVG(avg_days_supply), 1) AS avg_days_supply
FROM main.gold.daily_therapy_metrics
WHERE metric_date >= date_sub(current_date(), 120)
GROUP BY therapy_code;

CREATE OR REPLACE VIEW main.gold.v_dashboard_journey_status AS
SELECT
  therapy_code,
  on_therapy,
  COUNT(*) AS patient_count,
  ROUND(AVG(days_since_last_fill), 1) AS avg_days_since_fill,
  SUM(pa_denial_count) AS pa_denials
FROM main.gold.patient_journey
GROUP BY therapy_code, on_therapy;

CREATE OR REPLACE VIEW main.gold.v_dashboard_at_risk AS
SELECT
  patient_token,
  therapy_code,
  last_fill_date,
  days_since_last_fill,
  pa_denial_count,
  hub_interactions_30d,
  on_therapy
FROM main.gold.patient_journey
WHERE NOT on_therapy OR days_since_last_fill > 30
ORDER BY days_since_last_fill DESC
LIMIT 200;
