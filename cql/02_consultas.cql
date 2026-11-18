USE cloud_analytics;

-- Q1: costos y requests diarios por servicio de una organización en un rango de fechas (14 días)
SELECT usage_date, service, daily_cost_usd, requests
FROM org_daily_usage_by_service
WHERE org_id = '<ORG_ID>' AND usage_date >= '<DESDE>' AND usage_date <= '<HASTA>';

-- Q2: detalle por servicio de una organización en un día puntual
SELECT service, daily_cost_usd, requests, events
FROM org_daily_usage_by_service
WHERE org_id = '<ORG_ID>' AND usage_date = '<FECHA>';

-- Idempotencia: filas de una organización (debe ser igual antes y después de recargar)
SELECT COUNT(*) FROM org_daily_usage_by_service WHERE org_id = '<ORG_ID>';
