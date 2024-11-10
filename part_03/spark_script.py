from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .appName("MyGoitSparkSandbox")
    .getOrCreate()
)

# Завантажуємо датасет
nuek_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("./data/nuek-vuh3.csv")
)

# Перерозподіл на 2 партіції
nuek_repart = nuek_df.repartition(2)

# Обробка даних із кешуванням
nuek_processed_cached = (
    nuek_repart.filter("final_priority < 3")
    .select("unit_id", "final_priority")
    .groupBy("unit_id")
    .count()
    .cache()
)  # Додано функцію cache

# Проміжний action: collect
nuek_processed_cached.collect()

# Додаткова фільтрація
nuek_processed = nuek_processed_cached.filter("count > 2")

# Останній action: collect
nuek_processed.collect()

# Очікуємо на натискання Enter для перегляду SparkUI
input("Press Enter to continue...")

# Звільняємо пам'ять від DataFrame
nuek_processed_cached.unpersist()

# Закриваємо сесію Spark
spark.stop()
