-- =====================================================================
-- One-command deployment of the attrition warehouse objects, in order,
-- stopping at the first error (plain GO-separated scripts keep running
-- after a THROW, so a failed safety check would otherwise be ignored).
--
-- Run from the folder holding all the .sql files, in SQLCMD mode:
--   sqlcmd -S localhost -E -I -b -i deploy_attrition.sql
--     -E  Windows Authentication     -I  QUOTED_IDENTIFIER ON (needed for the filtered index)
--     -b  abort batch on error
-- In SSMS: Query > SQLCMD Mode, then execute this file.
-- Safe to rerun: every script is idempotent.
--
-- Phase 1-2  schema, staging, churn rules, data-quality view
-- Phase 3    MERGE load procedure
-- Phase 4    metrics procedure (features, risk flags, cohort retention)
-- Phase 5    run log + audit procedure (attrition_ops.sql)
-- Phase 6    reporting views for Tableau
-- =====================================================================
:on error exit

:r warehouse_star_schema.sql
:r attrition_schema.sql
:r attrition_staging.sql
:r attrition_churn_rules.sql
:r attrition_dq_checks.sql
:r attrition_load_warehouse.sql
:r attrition_metrics.sql
:r attrition_reporting_views.sql
:r attrition_ops.sql
