# Análisis de inventario con Apache Spark (PySpark)

Proyecto del módulo de Big Data Aplicada: análisis de un inventario de productos usando RDDs de PySpark en modo local, más los ejercicios prácticos de introducción a Spark.

## Qué hace

- Carga un archivo de inventario con `sc.textFile()` usando rutas locales (`file://`).
- Limpia y parsea los datos con una sola llamada a `split()` por línea.
- Calcula, por categoría: precio máximo, mínimo y promedio, unidades totales y volatilidad.
- Calcula el valor del inventario (`quantity × price`) y el top 5 de productos.
- Guarda los resultados en `RESULTADOS/` como CSV.

## Estructura

```
spark-stocks/
├── README.md
├── requirements.txt
├── proyecto.py        # Proyecto: análisis del inventario
├── ejercicios.py      # Ejercicios prácticos 1 a 5
├── DATOS/
│   ├── stocks.txt     # Inventario: id,name,category,quantity,price
│   └── persons.csv    # name,age,gender,salary
└── RESULTADOS/        # Salida del proyecto
```

## Requisitos

- Python 3.8+
- Java 17 (`sudo apt install openjdk-17-jdk`)
- PySpark 4.3.0

## Cómo ejecutarlo

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 proyecto.py
python3 ejercicios.py
```

## Sobre los datos

Los archivos reales difieren de lo que describía el enunciado original:

- `stocks.txt` es un **inventario de productos** (`id,name,category,quantity,price`), no datos bursátiles con símbolo, fecha y volumen. Por eso el análisis se hizo por **categoría** y se sustituyó el "día con mayor volumen" por unidades y valor de inventario.
- `persons.csv` tiene las columnas `name,age,gender,salary`, no `name,age,city`. El código solo usa nombre y edad, así que no se ve afectado.

## Resultados destacados

| Métrica | Valor |
|---|---|
| Productos procesados | 30 |
| Categorías | 6 |
| Valor total del inventario | $73,932.41 |
| Producto más caro | Macbook ($1,299.99) |
| Producto más barato | Bananas ($0.25) |
| Categoría más volátil | Food (2296 %) |
| Categoría más estable | Accessories (20.01 %) |
| Categoría con más unidades | Food (450) |

**Top 5 por valor de inventario:** iPhone ($8,999.90), iPad ($7,499.85), LG TV ($6,999.90), Macbook ($6,499.95), Samsung TV ($6,399.92).

La volatilidad se calcula como `(precio_max - precio_min) / precio_min × 100`.

### Ejercicios

- Ejercicio 2: 20 elementos, suma 210, máx 20, mín 1.
- Ejercicio 3: 34 números mayores a 50 tras multiplicar por 3.
- Ejercicio 4: Laptop 3600, Mouse 55, Teclado 75.
- Ejercicio 5: 20 personas, 10 mayores de 30 años.

## Preguntas de reflexión

**1. Eficiencia.** `split()` recorre toda la línea y crea una lista nueva. Si se
llama una vez por columna, se repite ese trabajo. En `parsear_linea` se hace una
sola vez y se reutilizan `p[0]`...`p[4]`. Con 1 millón de líneas y 5 columnas,
serían 5 millones de splits en lugar de 1 millón (una quinta parte del trabajo de
parseo). Con las 30 líneas de este proyecto la diferencia no se nota.

**2. Particionamiento.** Mi RDD tiene 2 particiones (`rdd.getNumPartitions()`).
Cada partición se procesa como una tarea independiente, así que con 2 pueden
ejecutarse en paralelo. Con 1 sola partición todo correría en una única tarea,
sin paralelismo: en un clúster, los demás núcleos quedarían sin trabajo.

**3. Lazy evaluation.** Las transformaciones (`map`, `filter`, `reduceByKey`) no
se ejecutaron al escribirlas, sino en la primera acción: `productos.count()`.
La ventaja es que Spark ve el plan completo y lo optimiza, por ejemplo encadenando
`map` y `filter` en una sola pasada sin guardar resultados intermedios.

**4. Memoria.** `collect()` trae todos los datos al driver. Con un RDD de 10 GB
probablemente produciría `OutOfMemoryError`. Se evita con `take(n)` para
muestras, agregando antes de traer datos (`reduceByKey` redujo 30 filas a 6
categorías) o guardando con `saveAsTextFile`.

**5. Optimización (100x más datos).** Aumentaría las particiones
(`minPartitions` o `repartition`), mantendría `cache()` en el RDD que se reutiliza
si cabe en RAM, evitaría `collect()` sobre datos grandes, ajustaría
`spark.sql.shuffle.partitions` y migraría a DataFrames, que Spark optimiza mejor.

**6. HDFS vs. local.** HDFS reparte y replica los datos entre nodos: si uno falla,
se lee otra réplica, y Spark puede procesar cada bloque cerca de donde está
guardado. Un archivo local vive en una sola máquina, sin tolerancia a fallos ni
paralelismo entre nodos. Para este análisis (986 bytes) no hay diferencia práctica,
pero con datos grandes HDFS sería la opción correcta.

## Conceptos aplicados

- **Lazy evaluation:** las transformaciones no se ejecutan hasta llamar a una acción.
- **`reduceByKey` y `aggregateByKey`:** agregaciones por clave sin traer todos los datos al driver.
- **`cache()`:** el RDD de productos se reutiliza en varias acciones, así que se guarda en RAM.
- **`coalesce(1)`:** produce un único archivo de salida por resultado.
- **Rutas locales vs. HDFS:** el prefijo `file://` indica a Spark que lea del disco local.
