"""
spark_pipeline.py
-----------------
PySpark job that:
  1. Reads  employees_raw.csv
  2. Cleans & transforms the data  (Task 3)
  3. Loads into PostgreSQL          (Task 4)

Run inside the Spark container:
  spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
    /opt/scripts/spark_pipeline.py
"""

import logging
import sys
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("EmployeePipeline")

# ── Config ────────────────────────────────────────────────────────────────────
RAW_CSV     = "/opt/data/employees_raw.csv"

JDBC_URL    = "jdbc:postgresql://postgres:5432/employeedb"
JDBC_TABLE  = "employees_clean"
JDBC_PROPS  = {
    "user":   "sparkuser",
    "password": "sparkpass",
    "driver": "org.postgresql.Driver",
}

TODAY       = date.today().isoformat()   # e.g. "2026-04-04"

# ═════════════════════════════════════════════════════════════════════════════
#  STEP 0 – Create Spark Session
# ═════════════════════════════════════════════════════════════════════════════

def create_spark_session() -> SparkSession:
    log.info("Creating Spark session …")
    spark = (
        SparkSession.builder
        .appName("EmployeeDataPipeline")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")   # quieten Spark's own logs
    log.info("Spark session ready ✓")
    return spark


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 1 – Read Raw CSV
# ═════════════════════════════════════════════════════════════════════════════

def read_raw(spark: SparkSession) -> DataFrame:
    log.info(f"Reading raw CSV: {RAW_CSV}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # keep everything as string first
        .option("multiLine", "true")      # handles quoted commas in salary
        .option("escape", '"')
        .csv(RAW_CSV)
    )
    log.info(f"  Raw row count : {df.count():,}")
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 2 – Data Quality Checks  (Task 3.1)
# ═════════════════════════════════════════════════════════════════════════════

def run_quality_checks(df: DataFrame) -> None:
    """Log quality metrics – does NOT modify the dataframe."""
    log.info("── Data Quality Report ──────────────────────────────────")

    total = df.count()

    # Missing critical fields
    null_id    = df.filter(F.col("employee_id").isNull() | (F.col("employee_id") == "")).count()
    null_fname = df.filter(F.col("first_name").isNull()  | (F.col("first_name")  == "")).count()
    null_email = df.filter(F.col("email").isNull()        | (F.col("email")        == "")).count()

    # Duplicates
    dupes = total - df.dropDuplicates(["employee_id"]).count()

    # Invalid emails  (must have @ and a dot after @)
    invalid_email = df.filter(
        ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    ).count()

    # Future hire dates
    future_hire = df.filter(F.col("hire_date") > F.lit(TODAY)).count()

    log.info(f"  Total rows          : {total:>6,}")
    log.info(f"  Null employee_id    : {null_id:>6,}")
    log.info(f"  Null first_name     : {null_fname:>6,}")
    log.info(f"  Null email          : {null_email:>6,}")
    log.info(f"  Duplicate IDs       : {dupes:>6,}")
    log.info(f"  Invalid emails      : {invalid_email:>6,}")
    log.info(f"  Future hire_dates   : {future_hire:>6,}")
    log.info("────────────────────────────────────────────────────────")


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 3 – Clean & Transform  (Task 3.2 + 3.3)
# ═════════════════════════════════════════════════════════════════════════════

def clean_and_transform(df: DataFrame) -> DataFrame:
    log.info("Applying cleaning & transformations …")

    # ── 3.1  Drop rows missing employee_id (un-fixable) ──────────────────
    df = df.filter(
        F.col("employee_id").isNotNull() & (F.col("employee_id") != "")
    )

    # ── 3.1  Remove duplicates – keep first occurrence ───────────────────
    df = df.dropDuplicates(["employee_id"])

    # ── 3.2a  Name standardisation  (initcap = Proper Case) ──────────────
    df = df.withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    df = df.withColumn("last_name",  F.initcap(F.trim(F.col("last_name"))))

    # ── 3.2b  Salary cleaning  →  strip $, commas, quotes → DECIMAL ──────
    df = df.withColumn(
        "salary",
        F.regexp_replace(F.col("salary"), r'[\$,\s"]', "")   # remove $ , " spaces
    )
    df = df.withColumn(
        "salary",
        F.when(
            F.col("salary").rlike(r"^\d+(\.\d+)?$"),           # valid number?
            F.col("salary").cast(DecimalType(10, 2))
        ).otherwise(F.lit(None).cast(DecimalType(10, 2)))      # else null
    )

    # ── 3.2c  Email cleanup  →  lowercase + validate ──────────────────────
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))

    # Mark invalid emails as null so UNIQUE constraint is not violated
    df = df.withColumn(
        "email",
        F.when(
            F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
            F.col("email")
        ).otherwise(F.lit(None))
    )

    # Drop rows where email is still null (can't load without unique email)
    df = df.filter(F.col("email").isNotNull())

    df = df.dropDuplicates(["email"])
    
    # ── 3.2d  Date parsing & hire_date validation ─────────────────────────
    df = df.withColumn(
        "hire_date",
        F.to_date(F.col("hire_date"), "yyyy-MM-dd")
    )
    # Nullify future hire dates
    df = df.withColumn(
        "hire_date",
        F.when(F.col("hire_date") <= F.lit(TODAY), F.col("hire_date"))
         .otherwise(F.lit(None).cast(DateType()))
    )
    # Drop rows where hire_date is null (NOT NULL constraint in DB)
    df = df.filter(F.col("hire_date").isNotNull())

    df = df.withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))

    # ── 3.2e  Age calculation  (from birth_date) ──────────────────────────
    df = df.withColumn(
        "age",
        F.when(
            F.col("birth_date").isNotNull(),
            (F.datediff(F.current_date(), F.col("birth_date")) / 365.25).cast(IntegerType())
        ).otherwise(F.lit(None).cast(IntegerType()))
    )

    # ── 3.2f  Tenure calculation  (years of service from hire_date) ───────
    df = df.withColumn(
        "tenure_years",
        F.round(
            F.months_between(F.current_date(), F.col("hire_date")) / 12,
            1
        ).cast(DecimalType(3, 1))
    )

    # ── 3.2g  Salary bands ────────────────────────────────────────────────
    df = df.withColumn(
        "salary_band",
        F.when(F.col("salary") < 50_000,  F.lit("Junior"))
         .when(F.col("salary") <= 80_000, F.lit("Mid"))
         .when(F.col("salary") >  80_000, F.lit("Senior"))
         .otherwise(F.lit("Unknown"))
    )

    # ── 3.3a  Full name ───────────────────────────────────────────────────
    df = df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )

    # ── 3.3b  Email domain ────────────────────────────────────────────────
    df = df.withColumn(
        "email_domain",
        F.when(
            F.col("email").isNotNull(),
            F.split(F.col("email"), "@").getItem(1)
        ).otherwise(F.lit(None))
    )

    # ── Status normalisation  (lowercase variants → proper case) ─────────
    df = df.withColumn("status", F.initcap(F.trim(F.col("status"))))

    # ── Cast employee_id and manager_id to integer ────────────────────────
    df = df.withColumn("employee_id", F.col("employee_id").cast(IntegerType()))
    df = df.withColumn(
        "manager_id",
        F.when(
            F.col("manager_id").isNotNull() & (F.col("manager_id") != ""),
            F.col("manager_id").cast(IntegerType())
        ).otherwise(F.lit(None).cast(IntegerType()))
    )

    # ── state  → uppercase + trim ─────────────────────────────────────────
    df = df.withColumn("state", F.upper(F.trim(F.col("state"))))

    log.info(f"  Rows after cleaning : {df.count():,}")
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 4 – Select final columns in DB order
# ═════════════════════════════════════════════════════════════════════════════

def select_final_columns(df: DataFrame) -> DataFrame:
    return df.select(
        "employee_id",
        "first_name",
        "last_name",
        "full_name",
        "email",
        "email_domain",
        "hire_date",
        "job_title",
        "department",
        "salary",
        "salary_band",
        "manager_id",
        "address",
        "city",
        "state",
        "zip_code",
        "birth_date",
        "age",
        "tenure_years",
        "status",
    )


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 5 – Load into PostgreSQL  (Task 4.2)
# ═════════════════════════════════════════════════════════════════════════════

def load_to_postgres(df: DataFrame) -> None:
    log.info(f"Loading {df.count():,} rows → PostgreSQL table '{JDBC_TABLE}' …")

    try:
        (
            df.write
            .jdbc(
                url=JDBC_URL,
                table=JDBC_TABLE,
                mode="append",          # use "overwrite" to reload from scratch
                properties=JDBC_PROPS,
            )
        )
        log.info("✅  Data loaded successfully into PostgreSQL!")

    except Exception as exc:
        log.error(f"❌  JDBC load failed: {exc}")
        raise


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 6 – Post-load verification
# ═════════════════════════════════════════════════════════════════════════════

def verify_load(spark: SparkSession) -> None:
    log.info("Running post-load verification …")
    try:
        verify_df = spark.read.jdbc(
            url=JDBC_URL,
            table=JDBC_TABLE,
            properties=JDBC_PROPS,
        )
        total = verify_df.count()
        log.info(f"  Rows in DB          : {total:,}")

        # Department distribution
        log.info("  Department breakdown:")
        verify_df.groupBy("department").count().orderBy("count", ascending=False).show(10, truncate=False)

        # Salary band breakdown
        log.info("  Salary band breakdown:")
        verify_df.groupBy("salary_band").count().orderBy("salary_band").show(truncate=False)

    except Exception as exc:
        log.warning(f"  Verification skipped (non-fatal): {exc}")


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("══════════════════════════════════════════════")
    log.info("  Employee Data Pipeline  –  Starting")
    log.info("══════════════════════════════════════════════")

    spark = create_spark_session()

    try:
        # 1. Read
        raw_df = read_raw(spark)

        # 2. Quality report (read-only)
        run_quality_checks(raw_df)

        # 3. Clean & transform
        clean_df = clean_and_transform(raw_df)

        # 4. Final column selection
        final_df = select_final_columns(clean_df)

        # 5. Load
        load_to_postgres(final_df)

        # 6. Verify
        verify_load(spark)

        log.info("══════════════════════════════════════════════")
        log.info("  Pipeline completed successfully  ✅")
        log.info("══════════════════════════════════════════════")

    except Exception as exc:
        log.error(f"Pipeline failed: {exc}", exc_info=True)
        sys.exit(1)

    finally:
        spark.stop()
        log.info("Spark session stopped.")


if __name__ == "__main__":
    main()
