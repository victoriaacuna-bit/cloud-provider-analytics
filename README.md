# Cloud Provider Analytics

**Proyecto Integrador · Minería de Datos II · ISTEA · 2.º cuatrimestre 2026**
Profesor: Diego Mosquera · Autora: Victoria Acuña · Modalidad: individual

Pipeline de datos para el área de datos de un proveedor de nube: ingesta batch y streaming con PySpark, Data Lake en Parquet (Landing → Bronze → Silver → Gold) y serving de marts analíticos en Cassandra/AstraDB para FinOps, Soporte y Producto.

---

## Estado del proyecto

| Instancia | Fecha | Estado |
|---|---|---|
| 1.ª evaluación parcial: diseño y fundación | 07/10/2026 | **Entregada (v1.0)** |
| 2.ª evaluación parcial: implementación técnica | 18/11/2026 | Pendiente |
| Evaluación final: MVP integrado | 09/12/2026 | Pendiente |

## Documentación

| Documento | Contenido |
|---|---|
| [`docs/diseno_entrega1.md`](docs/diseno_entrega1.md) | Documento de diseño de la primera entrega (ítems 1 a 12 de la consigna) |
| [`docs/arquitectura_v1.png`](docs/arquitectura_v1.png) | Diagrama de arquitectura v1 |
| [`DECISIONS.md`](DECISIONS.md) | Registro de decisiones técnicas, alternativas y trade-offs |
| [`notebooks/01_exploracion_landing.ipynb`](notebooks/01_exploracion_landing.ipynb) | Exploración y perfil inicial de las fuentes (evidencia) |

## Arquitectura (resumen)

- **Patrón:** Lambda. Camino batch para maestros, facturación y encuestas; camino streaming para eventos de uso.
- **Procesamiento:** PySpark (DataFrames y Structured Streaming).
- **Almacenamiento:** Parquet particionado por fecha en Google Drive.
- **Serving:** Cassandra/AstraDB con tablas modeladas por consulta (query-first).

## Estructura del repositorio

```
cloud-provider-analytics/
├── README.md          Este archivo
├── DECISIONS.md       Registro de decisiones
├── .gitignore         Exclusión de credenciales y datos generados
├── docs/              Documento de diseño y diagramas
├── notebooks/         Exploración y prácticas
└── data/              Instrucciones para obtener los datos (no se versionan)
```

Las carpetas `src/`, `tests/`, `config/` y `evidence/`, previstas en la sección 8.1 de la consigna, se incorporarán en la segunda entrega, junto con el código del pipeline.

## Requisitos

| Herramienta | Versión |
|---|---|
| Google Colab | Runtime estándar de Python 3 |
| PySpark | 4.0.4 (instalado con `pip install pyspark`) |
| Google Drive | Para alojar el dataset y el Data Lake |

## Cómo reproducir la exploración

1. Obtener el dataset según [`data/README.md`](data/README.md) y ubicarlo en Google Drive en:
   `MyDrive/Parcial Mineria de Datos II/Proyecto/cloud_provider_challenge_dataset_v1/datalake/landing`
2. Abrir `notebooks/01_exploracion_landing.ipynb` en Google Colab.
3. Ejecutar las celdas en orden. Colab solicitará permiso para montar Google Drive.
4. Resultado esperado: perfil de las ocho fuentes (filas, esquema, nulos), chequeos de calidad de eventos y verificación de NPS y tipo de cambio. Las salidas de referencia están guardadas en el propio notebook.

> **Nota:** Colab elimina el contenido de `/content/` al cerrar la sesión. Todos los datos de entrada y salida se mantienen en Google Drive.

## Seguridad

Este repositorio no contiene credenciales ni datos sensibles. Las credenciales de AstraDB se gestionarán mediante variables de entorno y están excluidas por `.gitignore`.

## Limitaciones actuales

- La primera entrega es de diseño: no incluye todavía el código del pipeline.
- El dataset es sintético y de volumen reducido; el diseño apunta al escenario productivo que representa.
