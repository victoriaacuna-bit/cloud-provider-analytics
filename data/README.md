# Datos

Los datos no se versionan en este repositorio.

## Origen

*Cloud Provider Analytics Challenge — Synthetic Dataset* (`cloud_provider_challenge_dataset_v1`).

## Ubicación esperada

Copiar el contenido del dataset en Google Drive respetando esta ruta:

```
MyDrive/Parcial Mineria de Datos II/Proyecto/cloud_provider_challenge_dataset_v1/datalake/landing

├── customers_orgs.csv
├── users.csv
├── resources.csv
├── support_tickets.csv
├── marketing_touches.csv
├── nps_surveys.csv
├── billing_monthly.csv
└── usage_events_stream/   (120 archivos events_part_XXXX.jsonl)
```

## Verificación

Al ejecutar `notebooks/01_exploracion_landing.ipynb` se deben obtener estos conteos:

| Fuente | Filas |
|---|---|
| customers_orgs | 80 |
| users | 800 |
| resources | 400 |
| support_tickets | 1.000 |
| marketing_touches | 1.500 |
| nps_surveys | 92 |
| billing_monthly | 240 |
| usage_events (120 archivos) | 43.200 |

## Regla de inmutabilidad

Los archivos de Landing no deben modificarse. Toda transformación se escribe en las zonas Bronze, Silver, Gold o Quarantine.
