# Cloud Provider Analytics

**Proyecto Integrador · Minería de Datos II · ISTEA · 2.º cuatrimestre 2026**
Profesor: Diego Mosquera · Autora: Victoria Acuña · Modalidad: individual

Pipeline de datos para un proveedor de nube: ingesta batch y streaming con PySpark, Data Lake en Parquet (Landing → Bronze → Silver → Gold) y serving en Cassandra/AstraDB.

---

## Estado del proyecto

| Fase | Fecha | Estado |
|---|---|---|
| Fase 1 · Diseño y fundación | 07/10/2026 | Entregada (v1.0) |
| Fase 2 · Pipeline end-to-end mínimo | 18/11/2026 | **Entregada (v2.0)** |
| Fase 3 · MVP integrado | 09/12/2026 | En curso (ver [`docs/backlog.md`](docs/backlog.md)) |

## Qué hace el pipeline hoy

| Capa | Implementado |
|---|---|
| Batch → Bronze | `customers_orgs`, `users` y `billing_monthly` con esquema explícito, `ingest_ts`, `source_file`, `run_id` y deduplicación por clave |
| Streaming → Bronze | `usage_events_stream` con Structured Streaming: esquema explícito, watermark medido, `dropDuplicatesWithinWatermark(event_id)`, checkpoint y anti-join por clave natural |
| Silver | Clientes (NPS validado), facturación (FX = 1 en USD, revenue neto) y eventos conformados, con 6 reglas de calidad, quarantine y join con clientes |
| Features | `daily_cost_usd`, `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens`, `carbon_kg` |
| Gold | `org_daily_usage_by_service` (org × día × servicio) y `cost_anomaly_mart` |
| Analítica | Z-score robusto (MAD) por organización y servicio |
| Serving | Keyspace `cloud_analytics`, tabla query-first y 2 consultas CQL |
| Idempotencia | Re-ejecución completa con conteos antes/después |

### Resultados de la ejecución de referencia

| Control | Resultado |
|---|---|
| Eventos Landing → Bronze | 43.200 → 43.200 (0 descartados, 0 duplicados) |
| Bronze → Silver / Quarantine | 42.989 / 211 (costo < −0,01) |
| Unidades imputadas | 2.038 |
| Facturas USD con FX corregido | 160 |
| NPS fuera de rango | 1 |
| Filas Gold `org_daily_usage_by_service` | 11.050 (costo Silver = Gold = 148.351,58 USD) |
| Anomalías de costo | 139 (1,3 %) |

El detalle completo está en `evidence/run_summary.json`.

## Quickstart

### Requisitos

| Herramienta | Versión |
|---|---|
| Google Colab | Runtime estándar Python 3 |
| PySpark | 4.0.x (`pip install pyspark`) |
| cassandra-driver | 3.29 o superior (`pip install cassandra-driver`) |
| DataStax Astra DB | Base Serverless (plan gratuito) |

### Pasos

1. **Datos.** Copiar el dataset en Google Drive según [`data/README.md`](data/README.md).
2. **Astra DB.** Crear una base Serverless, agregar el keyspace `cloud_analytics`, generar un Application Token (rol *Database Administrator*) y descargar el Secure Connect Bundle en `<BASE>/secrets/` de Drive.
3. **Secretos.** En Colab, panel 🔑 *Secretos*: crear `ASTRA_TOKEN` con el token y habilitar el acceso al notebook.
4. **Configuración.** Abrir [`notebooks/02_pipeline_end_to_end.ipynb`](notebooks/02_pipeline_end_to_end.ipynb) en Colab y revisar en la sección 0 las variables `BASE` (o la variable de entorno `CPA_BASE`) y `ASTRA_SCB`.
5. **Ejecución.** *Entorno de ejecución → Ejecutar todas*. Tiempo aproximado: 25 minutos.
6. **Verificación.** Comparar las salidas con la tabla de resultados de este README. La sección 7 debe mostrar `igual = True` en todas las capas.

### Limpieza y reinicio

La sección 9 del notebook cierra las conexiones. Para empezar de cero, descomentar sus líneas: borran el lake local y los checkpoints y vacían la tabla (`TRUNCATE`). Landing nunca se modifica.

### Problemas frecuentes

| Síntoma | Solución |
|---|---|
| `No encuentro LANDING` | Revisar `BASE` en la sección 0 |
| `No encuentro el bundle` | Revisar que el nombre del `.zip` coincida con `ASTRA_SCB` |
| `Unable to load a default connection class` | `!apt-get -qq install -y libev4 libev-dev` y reinstalar `cassandra-driver` |
| Error de autenticación en Astra | El token debe estar en el secreto `ASTRA_TOKEN` y con acceso habilitado |

## Estructura del repositorio

```
cloud-provider-analytics/
├── README.md                Este archivo
├── DECISIONS.md             Registro de decisiones (D-01 a D-19)
├── .gitignore
├── cql/                     DDL y consultas CQL
├── docs/                    Diseño, diagramas, gobierno y backlog
├── notebooks/
│   ├── 01_exploracion_landing.ipynb
│   └── 02_pipeline_end_to_end.ipynb
├── evidence/                Logs, conteos, tamaños, quarantine y consultas
└── data/                    Instrucciones para obtener los datos
```

## Documentación

| Documento | Contenido |
|---|---|
| [`docs/arquitectura_v2.png`](docs/arquitectura_v2.png) | Arquitectura implementada en la fase 2 |
| [`DECISIONS.md`](DECISIONS.md) | Decisiones técnicas, alternativas y trade-offs |
| [`docs/gobierno.md`](docs/gobierno.md) | Controles, metadatos, linaje, responsabilidades y acceso |
| [`docs/backlog.md`](docs/backlog.md) | Backlog priorizado para el MVP |
| [`docs/diseno_entrega1.md`](docs/diseno_entrega1.md) | Documento de diseño de la fase 1 |

## Seguridad

El repositorio no contiene credenciales. El token de Astra vive en los Secretos de Colab y el Secure Connect Bundle en Drive; ambos están excluidos por `.gitignore`.
