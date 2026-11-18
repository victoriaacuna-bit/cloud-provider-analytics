# Gobierno preliminar

**Cloud Provider Analytics · versión 18/11/2026**

## Controles de calidad

| Control | Dónde | Evidencia |
|---|---|---|
| Esquemas explícitos (sin inferencia) | Bronze | Notebook 02, secciones 1 y 2 |
| Conciliación Landing vs Bronze | Bronze | `run_summary.json` → `stream_bronze` |
| 6 reglas de calidad con quarantine y `dq_reason` | Bronze → Silver | `quarantine_muestra.csv` |
| Validación de NPS y corrección de FX | Silver | `run_summary.json` → `silver_maestros` |
| Conciliación de costo Silver vs Gold | Gold | `run_summary.json` → `gold` |
| Conteos antes/después de re-ejecutar | Todas las capas | `idempotencia.csv` |

## Metadatos y linaje

- **Por registro:** `ingest_ts`, `source_file`, `run_id` y `ingest_date` en Bronze; `dq_reason` y `quarantine_ts` en quarantine; `unit_imputada`, `fx_corregido` y `nps_valido` en Silver; `run_id` en Cassandra.
- **Linaje:** cualquier fila de Cassandra se rastrea por `run_id` hasta la ejecución, y cada evento de Bronze por `source_file` hasta su archivo de Landing.
- **Por ejecución:** log en `run_<RUN_ID>.log` y resumen en `run_summary.json`.

## Responsabilidades

| Rol | Responsabilidad |
|---|---|
| Ingeniería de datos | Ingesta, zonas del lake y serving |
| Calidad y gobierno | Reglas, revisión de quarantine y linaje |
| FinOps (área usuaria) | Validar supuestos de FX, créditos y revenue neto (D-07, D-08, D-13) |

En el proyecto, los roles técnicos los asume una sola persona.

## Acceso y seguridad

- El token de Astra se guarda en los Secretos de Colab y el Secure Connect Bundle en Drive. Ninguno se versiona (`.gitignore`).
- Token con rol *Database Administrator* solo para el laboratorio; en producción, un rol de escritura acotado al keyspace.
- `users.email` es dato personal: se conserva en Bronze, de acceso restringido, y no se propaga a Silver ni a Gold.
- Landing es de solo lectura: el pipeline nunca escribe en esa zona.
