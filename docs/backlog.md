# Backlog priorizado para el MVP

**Cloud Provider Analytics · versión 18/11/2026**

## Obligatorio (fase 3)

| # | Ítem | Base |
|---|---|---|
| O1 | Mart `revenue_by_org_month` en Gold y tabla en Cassandra (Silver de billing ya implementado) | Consulta 4 |
| O2 | Tabla Cassandra para top-N de servicios por costo en 14 días | Consulta 2 |
| O3 | Ingesta de `support_tickets` y mart `tickets_by_org_date` con SLA breach rate y CSAT | Consulta 3 |
| O4 | Mart `genai_tokens_by_org_date` con costo estimado y tabla en Cassandra | Consulta 5 |
| O5 | Tabla Cassandra para `cost_anomaly_mart` | Analítica |
| O6 | Pruebas automatizadas de reglas de calidad y transformaciones | Pruebas |
| O7 | Diccionario de datos de Bronze, Silver y Gold | Documentación |
| O8 | Presentación ejecutiva, video y guion de la defensa | Entrega final |

## Deseable

| # | Ítem |
|---|---|
| D1 | Modularizar el notebook en `src/` (ingesta, calidad, serving) con configuración en archivo |
| D2 | Regla de calidad para la incoherencia servicio-métrica (por ejemplo, `storage` con `cpu_hours`) tras medir su frecuencia |
| D3 | Tratamiento explícito de eventos con `value` nulo y costo presente |
| D4 | Conciliación entre costo de eventos y subtotal facturado |
| D5 | Carga a Cassandra distribuida (`foreachBatch` o Spark Cassandra Connector) |
| D6 | Reemplazar nulos por valores no seteados en los inserts para evitar tombstones |

## Fuera de alcance

| Ítem | Motivo |
|---|---|
| Ingesta de `marketing_touches`, `nps_surveys` y `resources` | No intervienen en las consultas obligatorias |
| Dashboard de visualización | El consumo se demuestra con consultas CQL |
| Modelos de ML supervisados | La analítica requerida se cubre con detección robusta de anomalías |
| Orquestación (Airflow u otro) | El notebook se ejecuta completo y es idempotente |
