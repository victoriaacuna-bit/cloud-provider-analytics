-- Keyspace: cloud_analytics (se crea desde el portal de Astra; Astra no admite CREATE KEYSPACE en CQL)
USE cloud_analytics;

-- Consulta que resuelve: costos y requests diarios de una organización por servicio en un rango de fechas.
-- Partición: org_id (cada consulta lee una sola partición, ~2.200 filas/año por organización).
-- Clustering: usage_date DESC (rangos de fechas, lo más reciente primero) + service (unicidad de la fila).
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id text, usage_date date, service text,
    org_name text, plan_tier text,
    daily_cost_usd double, requests double, cpu_hours double, storage_gb_hours double,
    genai_tokens bigint, carbon_kg double, events int, run_id text,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
