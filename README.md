# 📊 Proyecto Final – Grupo 15  
## Pipeline ELT con Arquitectura Medallón para análisis de reclamos y NPS

---

## 👥 Integrantes
- Ricardo Odicio  
- Jhosselyn Durán  

---

## 🎯 Objetivo

Diseñar e implementar un pipeline de datos bajo el enfoque **ELT** utilizando **Databricks** y la **arquitectura medallón (Raw → Bronze → Silver → Gold)**, para transformar datos de reclamos en un **modelo estrella** consumible por **Power BI**, permitiendo analizar desempeño operativo y satisfacción del cliente (NPS).

---

## 🧠 Contexto de negocio

Las organizaciones reciben reclamos desde múltiples canales (call center, app, oficinas).  
Sin un modelo estructurado, es difícil:
- identificar causas frecuentes  
- medir tiempos de resolución  
- evaluar satisfacción (NPS)  
- priorizar mejoras operativas  

Este proyecto consolida y transforma los datos para responder esas preguntas de negocio.

---

## 📂 Fuente de datos

Dataset en formato CSV con:
- id_reclamo, fecha_reclamo  
- cliente_id  
- canal, producto, motivo, estado  
- tiempo_resolucion_horas  
- monto_reembolso  
- nps  

La fuente se registró como **tabla en Databricks**:
`workspace.default.dataset_reclamos_bigdata_1000`

---

## 🏗️ Arquitectura

<img width="1293" height="844" alt="image" src="https://github.com/user-attachments/assets/72d93047-1a87-401f-922d-3ecb23eb183b" />


---

## 🥉 Capa Raw
- Ingesta sin transformaciones  
- Normalización de nombres  
- Trazabilidad (`fecha_carga`)  

**Tabla:** `raw_reclamos`

---

## 🥈 Capa Bronze
- Estandarización básica  
- Persistencia en formato Delta  

**Tabla:** `bronze_reclamos`

---

## 🥈 Capa Silver
- Tipado de columnas  
- Limpieza y control de nulos  
- Eliminación de duplicados  
- Validación de NPS (0–10)  
- Clasificación NPS:
  - Promotor (9–10)  
  - Neutro (7–8)  
  - Detractor (0–6)  
- Variables derivadas de fecha  

**Tabla:** `silver_reclamos`

---

## 🥇 Capa Gold (Modelo Estrella)

### 📌 Tabla de hechos
**fact_reclamos**
- id_reclamo  
- cliente_key  
- canal_key  
- producto_key  
- fecha_key  
- cantidad_reclamos  
- tiempo_resolucion_horas  
- monto_reembolso  
- nps  
- clasificacion_nps  

### 📌 Dimensiones
- **dim_fecha** → fecha, año, mes, trimestre  
- **dim_cliente** → cliente  
- **dim_canal** → canal  
- **dim_producto** → producto  

---

## 🔄 Orquestación

Se implementó un **Databricks Job** con ejecución secuencial:

1. 01_load_raw  
2. 02_bronze_reclamos  
3. 03_silver_reclamos  
4. 04_gold_dimensiones  
5. 05_gold_fact_reclamos  

✔ dependencias entre tareas  
✔ ejecución automática  
✔ pipeline reproducible  

---

## 📊 Visualización (Power BI)

Se construyó un dashboard de una página basado en la capa Gold.

### Indicadores:
- Total de reclamos  
- Clientes únicos  
- Tiempo promedio de resolución  
- Monto total de reembolsos  
- NPS promedio  

### Visuales:
- Reclamos por canal  
- Reclamos por producto  
- Evolución temporal de reclamos  
- Tendencia de reclamos  
- Distribución de NPS  

---

## 📈 Medidas DAX destacadas

```DAX
Total Reclamos = SUM(fact_reclamos[cantidad_reclamos])
NPS Promedio = AVERAGE(fact_reclamos[nps])
NPS Score = ([% Promotores] - [% Detractores])
Tendencia Reclamos = -- (regresión lineal aplicada sobre fechas)
```



❓ Preguntas de negocio
- ¿Qué canal genera más reclamos?
- ¿Qué productos presentan más incidencias?
- ¿Cómo evoluciona el NPS?
- ¿Existe tendencia creciente o decreciente en reclamos?
- ¿Cuál es el impacto económico de los reembolsos?
- 
⚙️ Tecnologías
- Databricks
- PySpark
- Delta Lake
- SQL
- Power BI
- GitHub

📁 Estructura del repositorio

<img width="1149" height="901" alt="image" src="https://github.com/user-attachments/assets/1ec0b510-a383-4b32-851a-e3ea0cc06ea3" />



📌 Conclusiones
- La arquitectura medallón permite estructurar datos de forma progresiva y controlada
- El enfoque ELT facilita la escalabilidad y mantenimiento
- El modelo estrella optimiza el consumo analítico
- La orquestación asegura continuidad operativa
- Power BI permite traducir datos en decisiones

💡 Mejoras futuras
- Ingesta desde APIs reales
- Procesamiento incremental
- Streaming (tiempo real)
- Data Quality automatizado
- Gobierno de datos
