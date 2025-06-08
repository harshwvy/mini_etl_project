from pyspark.sql.functions import col

def check_nulls(df, columns, logger):
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            logger.warning(f"Column '{col_name}' has {null_count} null values.")
        else:
            logger.info(f"No null values in column '{col_name}'.")

def check_row_count(df, expected_min, logger):
    actual_count = df.count()
    if actual_count < expected_min:
        logger.warning(f"Row count below expected: {actual_count} < {expected_min}")
    else:
        logger.info(f"Row count OK: {actual_count}")

def check_unique(df, column, logger):
    total = df.count()
    distinct = df.select(column).distinct().count()
    if total != distinct:
        logger.warning(f"Duplicate values found in column '{column}'. Total: {total}, Distinct: {distinct}")
    else:
        logger.info(f"Column '{column}' has unique values.")
