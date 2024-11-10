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

# Обробка даних: фільтрація, вибірка та групування
nuek_processed = (
    nuek_repart.where("final_priority < 3")
    .select("unit_id", "final_priority")
    .groupBy("unit_id")
    .count()
)

# Проміжна фільтрація
nuek_processed = nuek_processed.where("count > 2")

# Виконання дій (action)
nuek_processed.collect()

# Очікуємо на натискання Enter для перегляду SparkUI
input("Press Enter to continue...")

# Закриваємо сесію
spark.stop()
