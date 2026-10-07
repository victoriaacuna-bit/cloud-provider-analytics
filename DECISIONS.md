# Registro de decisiones técnicas

**Cloud Provider Analytics · Minería de Datos II · ISTEA · 2C 2026**

Cada decisión registra su contexto, la opción elegida, las alternativas consideradas y sus consecuencias. Estados posibles: **Aceptada**, **Abierta**, **Reemplazada**.

| ID | Decisión | Estado | Fecha |
|---|---|---|---|
| D-01 | Patrón Lambda | Aceptada | 07/10/2026 |
| D-02 | Facturación procesada en batch mensual | Aceptada | 07/10/2026 |
| D-03 | Spark como motor de procesamiento | Aceptada | 07/10/2026 |
| D-04 | Parquet como formato del Data Lake | Aceptada | 07/10/2026 |
| D-05 | Particionado por fecha | Aceptada | 07/10/2026 |
| D-06 | Casteo con fallback controlado | Aceptada | 07/10/2026 |
| D-07 | Tipo de cambio igual a 1 en facturas USD | Aceptada | 07/10/2026 |
| D-08 | Créditos nulos equivalen a 0 | Aceptada | 07/10/2026 |
| D-09 | Costos negativos a Quarantine, no agregados | Aceptada | 07/10/2026 |
| D-10 | Escala agregada para NPS | Aceptada | 07/10/2026 |
| D-11 | Data Lake alojado en Google Drive | Aceptada | 07/10/2026 |
| D-12 | Método de detección de anomalías | Abierta | — |
| D-13 | Inclusión de impuestos en el revenue | Abierta | — |
| D-14 | Claves de las tablas de Cassandra | Abierta | — |

---

## D-01 · Patrón Lambda

- **Contexto:** el perfilado de las fuentes mostró dos ritmos de llegada claramente diferenciados. Los eventos de uso se generan de forma continua (720 eventos por día, recibidos en micro-lotes) y FinOps necesita seguir el costo con baja latencia para detectar desvíos a tiempo. En cambio, los maestros de CRM cambian con poca frecuencia y la facturación se emite una vez por mes por organización. Un único modo de procesamiento obligaría a resignar latencia o simplicidad.
- **Decisión:** arquitectura Lambda, con un camino batch para maestros, facturación y encuestas, y un camino streaming para los eventos de uso, que convergen en Gold y en el serving.
- **Alternativas:** Kappa, que trataría todas las fuentes como streams; o un esquema 100 % batch.
- **Consecuencias:** se evita convertir en streams fuentes que no lo requieren, sin resignar la visibilidad casi en tiempo real del gasto. El riesgo de duplicar lógica se mitiga porque Spark usa la misma API de DataFrames para batch y streaming.

## D-02 · Facturación procesada en batch mensual

- **Contexto:** `billing_monthly` contiene una factura por organización por mes, siempre fechada el día 1 (240 = 80 × 3).No existe información de emisión intra-mes.
- **Decisión:** procesar la facturación en batch mensual.
- **Alternativas:** procesarla como stream, lo que tendría sentido si en producción la facturación fuera continua por ciclos de cliente.
- **Consecuencias:** el seguimiento del costo en tiempo cercano al real se obtiene de `cost_usd_increment` en los eventos; la factura se mantiene como dato oficial. La decisión es reversible sin reescribir transformaciones.

## D-03 · Spark como motor de procesamiento

- **Contexto:** el dataset provisto es pequeño (unos 13 MB de eventos) y podría procesarse con pandas.
- **Decisión:** utilizar PySpark.
- **Alternativas:** pandas.
- **Consecuencias:** el diseño se orienta al escenario productivo (crecimiento sostenido, pipelines repetibles, streaming), criterio favorable a Spark. Se acepta una sobrecarga en la ejecución local.

## D-04 · Parquet como formato del Data Lake

- **Contexto:** la exploración mostró que los formatos de origen no preservan tipos: al leer los eventos en JSON, `value` y `timestamp` se infirieron como texto, lo que obligaría a re-tipar en cada lectura. Además, las consultas analíticas usan pocas columnas de cada tabla: el costo diario por organización y servicio utiliza 4 de las 13 columnas de los eventos.
- **Decisión:** Parquet en Bronze, Silver, Gold y Quarantine; Landing conserva el formato original para preservar el dato crudo.
- **Alternativas:** mantener CSV/JSON en todas las zonas (descartado por la pérdida de tipos); Delta Lake o Iceberg, que aportan transacciones ACID y time travel, pero agregan dependencias al entorno y no son necesarios para el volumen actual.
- **Consecuencias:** el esquema se persiste junto con los datos y las lecturas acceden solo a las columnas requeridas. Al no contar con ACID, la idempotencia se garantiza mediante checkpoints, claves naturales y upserts. La reducción de tamaño respecto de CSV/JSON se medirá en la segunda entrega.

## D-05 · Particionado por fecha

- **Contexto:** las consultas obligatorias filtran por rangos de fechas.
- **Decisión:** particionar eventos por `event_date`, billing por `month` y los marts por la fecha de su grano. Los maestros no se particionan.
- **Alternativas:** `event_date` + `org_id`, que generaría 4.800 particiones de unos 9 eventos cada una.
- **Consecuencias:** poda de particiones en lectura sin incurrir en el problema de archivos pequeños. Las consultas por organización se resuelven en Cassandra.

## D-06 · Casteo con fallback controlado

- **Contexto:** `value` y `timestamp` llegan como texto. Spark 4 ejecuta con modo ANSI activo por defecto, por lo que un `cast` sobre un valor inválido detiene la ejecución.
- **Decisión:** `try_cast` y `try_to_timestamp`; los valores no convertibles van a Quarantine.
- **Alternativas:** `cast` estándar, o desactivar el modo ANSI.
- **Consecuencias:** ningún registro inválido detiene el pipeline y todos quedan trazados. En la exploración, el 100 % de los valores resultó convertible.

## D-07 · Tipo de cambio igual a 1 en facturas USD

- **Contexto:** las 160 facturas en USD presentan `exchange_rate_to_usd` entre 0,855 y 1,118.
- **Decisión:** en Silver, forzar `exchange_rate_to_usd = 1` cuando `currency = 'USD'` y registrar la cantidad de facturas corregidas.
- **Alternativas:** utilizar el valor informado, lo que introduce hasta un 15 % de error en el revenue.
- **Consecuencias:** revenue en USD consistente. Supuesto a confirmar con la cátedra.

## D-08 · Créditos nulos equivalen a 0

- **Contexto:** `credits` presenta 137 nulos sobre 240 facturas.
- **Decisión:** interpretar el nulo como ausencia de créditos (0).
- **Alternativas:** enviar esas facturas a Quarantine, lo que excluiría el 57 % de la facturación.
- **Consecuencias:** se preserva la totalidad del revenue. Supuesto documentado.

## D-09 · Costos negativos a Quarantine, no agregados

- **Contexto:** 216 eventos con `cost_usd_increment` negativo. Un costo incremental negativo no tiene interpretación operativa en un evento de uso: puede corresponder a un ajuste o reembolso mal registrado, o a un error del sistema de medición.
- **Decisión:** aplicar la regla `cost_usd_increment >= -0.01`.Los eventos que no cumplen la regla se envían a Quarantine con `dq_reason` y flag de anomalía; no participan de los agregados de Gold.
- **Alternativas:** incluirlos en las sumas, lo que subestimaría los costos; eliminarlos, lo que impediría su auditoría; o fijar el límite estricto en 0, lo que marcaría como inválidas diferencias mínimas producto del redondeo.
- **Consecuencias:** agregados no distorsionados y registros conservados para su revisión. El margen de -0,01 absorbe errores de redondeo en incrementos fraccionarios.

## D-10 · Escala agregada para NPS

- **Contexto:** valores de `nps_score` fuera del rango 0-10 (por ejemplo, -3, 11 y 15) podían interpretarse como errores.
- **Decisión:** adoptar la escala agregada de NPS (-100 a 100), verificada mediante la distribución (rango de -38 a 101 en clientes y de -16 a 68 en encuestas). Regla de calidad: valor entre -100 y 100.
- **Alternativas:** escala individual (0 a 10), que habría marcado como inválidos datos correctos.
- **Consecuencias:** un único valor inválido (101). La decisión ilustra el criterio de verificar la distribución antes de definir una regla.

## D-11 · Data Lake alojado en Google Drive

- **Contexto:** Colab elimina `/content/` al cerrar la sesión.
- **Decisión:** alojar el Data Lake y los checkpoints de streaming en Google Drive, montado desde Colab.
- **Alternativas:** almacenamiento local de Colab, o almacenamiento en la nube (GCS o S3).
- **Consecuencias:** persistencia entre sesiones sin costo. Se acepta una menor velocidad de E/S, adecuada al volumen del proyecto.

## D-12 · Método de detección de anomalías *(abierta)*

- **Contexto:** `cost_usd_increment` tiene mediana 1,00, p99 16,72 y máximo 317,43 USD.
- **Opciones:** z-score, MAD o percentiles.
- **Criterio preliminar:** la asimetría favorece métodos robustos (MAD o percentiles), ya que los picos inflan la media y el desvío estándar que usa el z-score.
- **Resolución prevista:** segunda entrega.

## D-13 · Inclusión de impuestos en el revenue *(abierta)*

- **Contexto:** cada factura informa por separado `subtotal, credits y taxes`, en tres monedas distintas.
- **Propuesta:** `revenue_usd = (subtotal − credits) × fx` como revenue neto, con los impuestos en una columna separada.
- **Resolución prevista:** validación con FinOps antes de implementar el mart de revenue.

## D-14 · Claves de las tablas de Cassandra *(abierta)*

- **Criterio:** modelado query-first: cada tabla se diseña a partir de una consulta de negocio priorizada por FinOps, Soporte o Producto, de modo que pueda resolverse sin joins ni filtros fuera de la clave.
- **Resolución prevista:** segunda entrega.
