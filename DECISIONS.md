# Registro de decisiones técnicas

**Cloud Provider Analytics · Minería de Datos II · ISTEA · 2C 2026**

Cada decisión registra su contexto, la opción elegida, las alternativas consideradas y sus consecuencias. Estados posibles: **Aceptada**, **Abierta**, **Reemplazada**.

| ID | Decisión | Estado | Fecha |
|---|---|---|---|
| D-01 | Patrón Lambda | Aceptada | 07/10/2026 |
| D-02 | Facturación procesada en batch mensual | Aceptada | 07/10/2026 |
| D-03 | Spark como motor de procesamiento | Aceptada | 07/10/2026 |
| D-04 | Parquet como formato del Data Lake | Aceptada | 07/10/2026 |
| D-05 | Particionado por fecha | Actualizada | 18/11/2026 |
| D-06 | Casteo con fallback controlado | Aceptada | 07/10/2026 |
| D-07 | Tipo de cambio igual a 1 en facturas USD | Aceptada | 07/10/2026 |
| D-08 | Créditos nulos equivalen a 0 | Aceptada | 07/10/2026 |
| D-09 | Costos negativos a Quarantine, no agregados | Aceptada | 07/10/2026 |
| D-10 | Escala agregada para NPS | Aceptada | 07/10/2026 |
| D-11 | Data Lake alojado en Google Drive | Reemplazada por D-16 | 18/11/2026 |
| D-12 | Método de detección de anomalías | Aceptada | 18/11/2026 |
| D-13 | Inclusión de impuestos en el revenue | Abierta | — |
| D-14 | Claves de las tablas de Cassandra | Aceptada | 18/11/2026 |
| D-15 | Watermark calculado a partir del desorden medido | Aceptada | 18/11/2026 |
| D-16 | Procesamiento local en Colab con copia a Drive | Aceptada | 18/11/2026 |
| D-17 | Imputación de `unit` a partir de la métrica | Aceptada | 18/11/2026 |
| D-18 | Doble capa de idempotencia en streaming | Aceptada | 18/11/2026 |
| D-19 | Carga a Cassandra con el driver de Python | Aceptada | 18/11/2026 |

---

## D-01 · Patrón Lambda

- **Contexto:** el perfilado de las fuentes mostró dos ritmos de llegada claramente diferenciados. Los eventos de uso se generan de forma continua (720 eventos por día, recibidos en micro-lotes) y FinOps necesita seguir el costo con baja latencia para detectar desvíos a tiempo. En cambio, los maestros de CRM cambian con poca frecuencia y la facturación se emite una vez por mes por organización. Un único modo de procesamiento obligaría a resignar latencia o simplicidad.
- **Decisión:** arquitectura Lambda, con un camino batch para maestros, facturación y encuestas, y un camino streaming para los eventos de uso, que convergen en Gold y en el serving.
- **Alternativas:** Kappa, que trataría todas las fuentes como streams; o un esquema 100 % batch.
- **Consecuencias:** se evita convertir en streams fuentes que no lo requieren, sin resignar la visibilidad casi en tiempo real del gasto. El riesgo de duplicar lógica se mitiga porque Spark usa la misma API de DataFrames para batch y streaming.

## D-02 · Facturación procesada en batch mensual

- **Contexto:** `billing_monthly` contiene una factura por organización por mes, siempre fechada el día 1 (240 = 80 × 3). No existe información de emisión intra-mes.
- **Decisión:** procesar la facturación en batch mensual.
- **Alternativas:** procesarla como stream, lo que tendría sentido si en producción la facturación fuera continua por ciclos de cliente.
- **Consecuencias:** el seguimiento del costo en tiempo cercano al real se obtiene de `cost_usd_increment` en los eventos; la factura se mantiene como dato oficial. La decisión es reversible sin reescribir transformaciones.

## D-03 · Spark como motor de procesamiento

- **Contexto:** el dataset disponible es reducido (unos 13 MB de eventos) y podría procesarse en una sola máquina con pandas. Sin embargo, representa a un proveedor cuyos eventos de uso crecen de forma sostenida, y la arquitectura elegida (D-01) requiere procesamiento batch y streaming.
- **Decisión:** utilizar PySpark.
- **Alternativas:** pandas, más simple y rápido para volúmenes chicos, pero limitado a la memoria de una sola máquina y sin soporte nativo de streaming.
- **Consecuencias:** un mismo motor y una misma API cubren batch y streaming, y el procesamiento puede escalar horizontalmente sin reescribir el código. Se acepta una sobrecarga de arranque y ejecución en el entorno local, justificada por el escenario productivo al que apunta el diseño.

## D-04 · Parquet como formato del Data Lake

- **Contexto:** la exploración mostró que los formatos de origen no preservan tipos: al leer los eventos en JSON, `value` y `timestamp` se infirieron como texto, lo que obligaría a re-tipar en cada lectura. Además, las consultas analíticas usan pocas columnas de cada tabla: el costo diario por organización y servicio utiliza 4 de las 13 columnas de los eventos.
- **Decisión:** Parquet en Bronze, Silver, Gold y Quarantine; Landing conserva el formato original para preservar el dato crudo.
- **Alternativas:** mantener CSV/JSON en todas las zonas (descartado por la pérdida de tipos); Delta Lake o Iceberg, que aportan transacciones ACID y time travel, pero agregan dependencias al entorno y no son necesarios para el volumen actual.
- **Consecuencias:** el esquema se persiste junto con los datos y, al ser un formato columnar, las lecturas acceden solo a las columnas requeridas. Al no contar con ACID, la idempotencia se garantiza mediante checkpoints, claves naturales y upserts. La reducción de tamaño respecto de CSV/JSON se medirá en la segunda entrega.

## D-05 · Particionado por fecha

- **Contexto:** las consultas de negocio de FinOps, Soporte y Producto filtran predominantemente por rangos de fechas (últimos 14 o 30 días, un mes determinado).
- **Decisión:** particionar eventos por `event_date`, facturación por `month` y los marts por la fecha de su grano. Los maestros no se particionan, dado su tamaño reducido (entre 4 KB y 100 KB).
- **Alternativas:** particionar por `event_date` y `org_id`, que generaría 4.800 particiones con unos 9 eventos cada una.
- **Consecuencias:** las lecturas descartan las particiones fuera del rango consultado, sin incurrir en el problema de archivos pequeños, que multiplica la cantidad de tareas y el costo de apertura de archivos. Las consultas por organización se resuelven en la capa de serving.

- **Actualización (18/11/2026):** en la implementación, los maestros `customers_orgs` y `users` se particionan por `ingest_date`. Cada carga queda como una foto (snapshot) completa del maestro: Silver toma la más reciente, el historial de fotos queda disponible para analizar cambios y la re-ejecución del mismo día reemplaza solo esa partición (overwrite dinámico), lo que la vuelve idempotente.

## D-06 · Casteo con fallback controlado

- **Contexto:** `value` y `timestamp` llegan como texto. Spark 4 ejecuta con modo ANSI activo por defecto, por lo que un `cast` sobre un valor inválido detiene la ejecución.
- **Decisión:** utilizar `try_cast` y `try_to_timestamp`; los valores no convertibles se envían a Quarantine.
- **Alternativas:** `cast` estándar, o desactivar el modo ANSI, lo que convertiría errores en nulos silenciosos.
- **Consecuencias:** ningún registro inválido detiene el pipeline y todos quedan trazados. En la exploración, el 100 % de los valores resultó convertible.

## D-07 · Tipo de cambio igual a 1 en facturas USD

- **Contexto:** las facturas emitidas en USD presentan `exchange_rate_to_usd` entre 0,855 y 1,118, cuando la conversión de una moneda a sí misma es, por definición, 1.
- **Decisión:** en Silver, forzar `exchange_rate_to_usd = 1` cuando `currency = 'USD'` y registrar la cantidad de facturas corregidas.
- **Alternativas:** utilizar el valor informado, lo que introduce hasta un 15 % de error en el revenue.
- **Consecuencias:** revenue en USD consistente. Supuesto a validar con FinOps, área responsable de la métrica.

## D-08 · Créditos nulos equivalen a 0

- **Contexto:** `credits` presenta 137 nulos sobre 240 facturas.
- **Decisión:** interpretar el nulo como ausencia de créditos (0).
- **Alternativas:** enviar esas facturas a Quarantine, lo que excluiría el 57 % de la facturación.
- **Consecuencias:** se preserva la totalidad del revenue. Supuesto documentado, a validar con FinOps.

## D-09 · Costos negativos a Quarantine, no agregados

- **Contexto:** 216 eventos (0,5 %) presentan `cost_usd_increment` negativo. Un costo incremental negativo no tiene interpretación operativa en un evento de uso: puede corresponder a un ajuste o reembolso mal registrado, o a un error del sistema de medición.
- **Decisión:** aplicar la regla `cost_usd_increment >= -0.01`. Los eventos que no la cumplen se envían a Quarantine con `dq_reason` y flag de anomalía, y no participan de los agregados de Gold.
- **Alternativas:** incluirlos en las sumas, lo que subestimaría los costos; eliminarlos, lo que impediría su auditoría; o fijar el límite estricto en 0, lo que marcaría como inválidas diferencias mínimas producto del redondeo.
- **Consecuencias:** agregados no distorsionados y registros conservados para su revisión. El margen de -0,01 absorbe errores de redondeo en incrementos fraccionarios.

## D-10 · Escala agregada para NPS

- **Contexto:** valores de `nps_score` fuera del rango 0-10 (por ejemplo, -3, 11 y 15) podían interpretarse como errores, si se asumía la escala de respuesta individual.
- **Decisión:** adoptar la escala agregada de NPS (-100 a 100), verificada mediante la distribución (rango de -38 a 101 en clientes y de -16 a 68 en encuestas). Regla de calidad: valor entre -100 y 100.
- **Alternativas:** escala individual (0 a 10), que habría marcado como inválidos datos correctos.
- **Consecuencias:** un único valor inválido (101). Se adopta como criterio general verificar la distribución de una variable antes de definir reglas de rango.

## D-11 · Data Lake alojado en Google Drive

- **Contexto:** Colab elimina `/content/` al cerrar la sesión.
- **Decisión:** alojar el Data Lake y los checkpoints de streaming en Google Drive, montado desde Colab.
- **Alternativas:** almacenamiento local de Colab, o almacenamiento en la nube (GCS o S3).
- **Consecuencias:** persistencia entre sesiones sin costo. Se acepta una menor velocidad de E/S, adecuada al volumen del proyecto.

## D-12 · Método de detección de anomalías

- **Contexto:** `cost_usd_increment` tiene mediana 1,00, p99 16,72 y máximo 317,43 USD. Los picos inflan la media y el desvío estándar que usa el z-score clásico.
- **Decisión:** z-score robusto por organización y servicio sobre el costo diario: `z = 0,6745 × (costo − mediana) / MAD`, con anomalía si `z > 3,5` (umbral de Iglewicz y Hoaglin). Se marcan solo los picos hacia arriba; los costos negativos ya se aíslan en quarantine (D-09).
- **Alternativas:** z-score clásico (sesgado por los propios picos); percentil 99 fijo (no se adapta al nivel de gasto de cada organización).
- **Consecuencias:** 139 de 11.050 días-servicio marcados (1,3 %). Resultado en `gold/cost_anomaly_mart`. Cuando el MAD es 0 (consumo constante), no se calcula score.

## D-13 · Inclusión de impuestos en el revenue *(abierta)*

- **Contexto:** cada factura informa por separado `subtotal`, `credits` y `taxes`, en tres monedas distintas.
- **Propuesta:** `revenue_usd = (subtotal − credits) × fx` como revenue neto, con los impuestos informados en una columna separada. Los impuestos son montos que la empresa recauda por cuenta del fisco y no constituyen ingreso; incluirlos inflaría el revenue y distorsionaría la comparación entre organizaciones sujetas a tasas impositivas distintas.
- **Resolución prevista:** validación con FinOps antes de implementar el mart de revenue.

## D-14 · Claves de las tablas de Cassandra

- **Contexto:** la consulta principal de FinOps pide costos y requests diarios de una organización, por servicio, en un rango de fechas.
- **Decisión:** `PRIMARY KEY ((org_id), usage_date, service)` con `CLUSTERING ORDER BY (usage_date DESC, service ASC)`.
- **Alternativas:** partición `(org_id, usage_date)`, que obligaría a una consulta por día para leer un rango; partición por `service`, que no corresponde al filtro de la consulta.
- **Consecuencias:** cada consulta lee una sola partición, sin `ALLOW FILTERING`. Tamaño acotado (~2.200 filas por organización por año). La clave natural convierte cada insert en upsert, lo que hace idempotente la carga.

## D-15 · Watermark calculado a partir del desorden medido

- **Contexto:** el diagnóstico mostró que cada archivo de eventos contiene eventos de los 60 días del período: el atraso máximo entre micro-lotes es de 1.439,9 horas. Una simulación sobre los 43.200 eventos mostró que un watermark de 1 hora descartaría el 91,6 % de los eventos; uno de 30 días, el 45,7 %.
- **Decisión:** `WATERMARK_HOURS` = atraso máximo observado + 1 hora de margen (1.441 h), calculado en cada ejecución.
- **Alternativas:** un valor fijo convencional (1 hora), que pierde datos válidos sin error visible.
- **Consecuencias:** 0 eventos descartados (conciliación Landing = Bronze = 43.200). El costo es mantener en estado los `event_id` de toda la ventana, despreciable con el volumen actual. Con llegada en orden casi real, el mismo cálculo daría pocas horas sin cambiar código.

## D-16 · Procesamiento local en Colab con copia a Drive

- **Contexto:** escribir cientos de archivos Parquet y checkpoints de streaming directamente sobre Google Drive es lento y poco confiable.
- **Decisión:** el lake y los checkpoints se procesan en `/content/lake` y, al finalizar, se copian a Drive (`datalake_output/`). Reemplaza a D-11.
- **Alternativas:** escribir directo en Drive (D-11 original).
- **Consecuencias:** ejecución más rápida y estable. Si la sesión se corta antes de la copia, se re-ejecuta el notebook completo, que es idempotente.

## D-17 · Imputación de `unit` a partir de la métrica

- **Contexto:** 2.075 eventos tienen `unit` nulo. El cruce métrica × unidad mostró que cada métrica tiene una única unidad (`cpu_hours` → `hours`, `requests` → `count`, `storage_gb_hours` → `gb_hours`).
- **Decisión:** imputar la unidad con la más frecuente de su métrica, marcando `unit_imputada`. La regla "unit no nula cuando existe value" se evalúa después de imputar.
- **Alternativas:** enviar los 2.075 eventos a quarantine, perdiendo su costo en los marts.
- **Consecuencias:** 2.038 unidades imputadas; las 37 restantes tienen `value` nulo, por lo que la regla no aplica. 0 eventos en quarantine por esta regla.

## D-18 · Doble capa de idempotencia en streaming

- **Contexto:** el checkpoint evita reprocesar archivos, pero si se borra o corrompe, un reproceso duplicaría Bronze.
- **Decisión:** además del checkpoint, cada micro-lote descarta en `foreachBatch` los `event_id` ya presentes en Bronze (anti-join por clave natural).
- **Alternativas:** depender solo del checkpoint.
- **Consecuencias:** la re-ejecución con el mismo checkpoint y el reproceso completo con un checkpoint nuevo producen los mismos conteos (sección 7 del notebook).

## D-19 · Carga a Cassandra con el driver de Python

- **Contexto:** el conector Spark-Cassandra requiere dependencias de JVM específicas por versión de Spark, frágiles en Colab con Spark 4.
- **Decisión:** cargar Gold con `cassandra-driver`, sentencias preparadas y `execute_concurrent_with_args`, con credenciales en los Secretos de Colab.
- **Alternativas:** Spark Cassandra Connector.
- **Consecuencias:** carga simple y estable para ~11.000 filas. Para volúmenes mayores, el conector o `foreachBatch` distribuido es el camino (backlog).
