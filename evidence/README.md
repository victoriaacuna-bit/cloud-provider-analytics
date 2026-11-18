# Evidencias de ejecución

Generadas por `notebooks/02_pipeline_end_to_end.ipynb` en `_evidence/` y copiadas aquí.

| Archivo | Contenido |
|---|---|
| `run_<RUN_ID>.log` | Log completo de la ejecución |
| `run_summary.json` | Conteos por capa, watermark, reglas, anomalías, serving, idempotencia y tamaños |
| `simulacion_watermark.csv` | Eventos descartados según el watermark |
| `quarantine_muestra.csv` | Muestra de registros en quarantine con su motivo |
| `tamanos_rutas.csv` | Archivos, particiones y MB por tabla |
| `q1_resultado.csv`, `q2_resultado.csv` | Resultados de las consultas CQL |
| `idempotencia.csv` | Conteos antes y después de re-ejecutar |
| `captura_cql_console.png` | Consulta Q1 ejecutada en la CQL Console de Astra |
