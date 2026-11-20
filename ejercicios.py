import os
from pyspark.sql import SparkSession

# ---------- Ejercicio 1: SparkSession personalizada ----------
spark = (SparkSession.builder
    .appName("MiPrimerAnalisis")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("=== EJERCICIO 1 ===")
print("Nombre:", sc.appName)
print("Versión:", spark.version)
print("Master:", sc.master)
print("executor.memory:", spark.conf.get("spark.executor.memory"))
print("shuffle.partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# ---------- Ejercicio 2: operaciones básicas ----------
print("\n=== EJERCICIO 2 ===")
rdd = sc.parallelize(list(range(1, 21)))
print("Total de elementos:", rdd.count())
print("Primeros 5:", rdd.take(5))
print("Suma:", rdd.sum())
print(f"Máximo: {rdd.max()}, Mínimo: {rdd.min()}")

# ---------- Ejercicio 3: map y filter ----------
print("\n=== EJERCICIO 3 ===")
nums = sc.parallelize(range(1, 51))
por_3 = nums.map(lambda x: x * 3)
mayores_50 = por_3.filter(lambda x: x > 50)
print("Total mayores a 50:", mayores_50.count())
print("Primeros 10:", mayores_50.take(10))

# ---------- Ejercicio 4: clave-valor ----------
print("\n=== EJERCICIO 4 ===")
ventas = [("Laptop", 1200), ("Mouse", 25), ("Laptop", 1100),
          ("Teclado", 75), ("Mouse", 30), ("Laptop", 1300)]
rdd_v = sc.parallelize(ventas)

totales = rdd_v.reduceByKey(lambda a, b: a + b)
print("Total por producto:", sorted(totales.collect()))

conteo = rdd_v.map(lambda kv: (kv[0], 1)).reduceByKey(lambda a, b: a + b)
print("Número de ventas:", sorted(conteo.collect()))

mayor = totales.sortBy(lambda kv: kv[1], ascending=False).first()
print(f"Producto con mayor total: {mayor[0]} ({mayor[1]})")

# ---------- Ejercicio 5: persons.csv ----------
print("\n=== EJERCICIO 5 ===")
rdd_p = sc.textFile("file://" + os.path.abspath("./DATOS/persons.csv"))
header = rdd_p.first()
sin_header = rdd_p.filter(lambda l: l != header)

def parsear_persona(linea):
    c = linea.split(",")          # un solo split
    return (c[0].strip(), int(c[1]))

nombre_edad = sin_header.map(parsear_persona)
mayores_30 = nombre_edad.filter(lambda x: x[1] > 30)

print("Encabezado eliminado:", header)
print("Total de personas:", nombre_edad.count())
print("Mayores de 30:", mayores_30.collect())
print("Total mayores de 30:", mayores_30.count())

spark.stop()
