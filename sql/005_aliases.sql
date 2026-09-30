-- Flat, schema-less names for the three headline warehouse tables, so ad-hoc
-- queries and the report can use one short name per Lambda layer:
--
--   public.speed_vitals_alerts         -> speed.vitals_alerts          (speed layer)
--   public.batch_lab_results           -> batch.patient_lab_results    (batch layer)
--   public.daily_patient_risk_summary  -> batch.patient_risk_daily     (batch layer)
--
-- Plain views (no data copy), idempotent via CREATE OR REPLACE.
CREATE OR REPLACE VIEW public.speed_vitals_alerts AS SELECT * FROM speed.vitals_alerts;
CREATE OR REPLACE VIEW public.batch_lab_results AS SELECT * FROM batch.patient_lab_results;
CREATE OR REPLACE VIEW public.daily_patient_risk_summary AS SELECT * FROM batch.patient_risk_daily;
