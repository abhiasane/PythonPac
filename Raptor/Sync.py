from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from concurrent.futures import ThreadPoolExecutor, as_completed

# Schema for results DataFrame
schema = StructType([
    StructField("source_table_name", StringType(), True),
    StructField("target_table_name", StringType(), True),
    StructField("schema_validation", StringType(), True),
    StructField("source_tbl_row_count", StringType(), True),
    StructField("target_tbl_row_count", StringType(), True),
    StructField("row_count_validation", StringType(), True),
    StructField("row_count_diff", StringType(), True),
    StructField("source_tbl_column_count", StringType(), True),
    StructField("target_tbl_column_count", StringType(), True),
    StructField("column_count_validation", StringType(), True),
    StructField("column_count_diff", StringType(), True),
    StructField("clm_avlb_in_src_bt_not_in_trgt", ArrayType(StringType()), True),
    StructField("clm_avlb_in_trgt_bt_not_in_src", ArrayType(StringType()), True)
])

def validate_single_table_pair(source_table_name, target_table_name, schema):
    """
    Validates a single pair of source and target tables and returns a DataFrame with the result.
    """
    try:
        source_table = spark.read.table(source_table_name)
        target_table = spark.read.table(target_table_name)

        # 1: Schema Validation
        schema1 = set((field.name, field.dataType) for field in source_table.schema.fields)
        schema2 = set((field.name, field.dataType) for field in target_table.schema.fields)
        schema_match = "match" if schema1 == schema2 else "mismatch"
        
        columns_in_source_not_target = list(set(source_table.columns) - set(target_table.columns))
        columns_in_target_not_source = list(set(target_table.columns) - set(source_table.columns))

        # 2: Row Count Comparison
        count_source = source_table.count()
        count_target = target_table.count()
        row_count_match = "match" if count_source == count_target else "mismatch"
        row_count_diff = abs(count_source - count_target)

        # 3: Column Count Validation
        column_count_source = len(source_table.columns)
        column_count_target = len(target_table.columns)
        column_count_match = "match" if column_count_source == column_count_target else "mismatch"
        column_count_diff = abs(column_count_source - column_count_target)

        # Return validation result as a DataFrame
        return spark.createDataFrame([{
            "source_table_name": source_table_name,
            "target_table_name": target_table_name,
            "schema_validation": schema_match,
            "source_tbl_row_count": str(count_source),
            "target_tbl_row_count": str(count_target),
            "row_count_validation": row_count_match,
            "row_count_diff": str(row_count_diff),
            "source_tbl_column_count": str(column_count_source),
            "target_tbl_column_count": str(column_count_target),
            "column_count_validation": column_count_match,
            "column_count_diff": str(column_count_diff),
            "clm_avlb_in_src_bt_not_in_trgt": columns_in_source_not_target,
            "clm_avlb_in_trgt_bt_not_in_src": columns_in_target_not_source
        }], schema=schema)

    except Exception as e:
        # If an error is raised the catch the error and store it in schema_validation column in the result DF
        return spark.createDataFrame([{
            "source_table_name": source_table_name,
            "target_table_name": target_table_name,
            "schema_validation": f"Error: {str(e)}",
            "source_tbl_row_count": "N/A",
            "target_tbl_row_count": "N/A",
            "row_count_validation": "N/A",
            "row_count_diff": "N/A",
            "source_tbl_column_count": "N/A",
            "target_tbl_column_count": "N/A",
            "column_count_validation": "N/A",
            "column_count_diff": "N/A",
            "clm_avlb_in_src_bt_not_in_trgt": [],
            "clm_avlb_in_trgt_bt_not_in_src": []
        }], schema=schema)

def validate_tables(source_tables, target_tables, primary_keys):
    results = []  # Store individual DataFrames to be unioned later
    
    # Using ThreadPoolExecutor to run validations concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit each validation job to the executor
        futures = {
            executor.submit(validate_single_table_pair, src, tgt, schema): (src, tgt) 
            for src, tgt in zip(source_tables, target_tables)
        }
        
        for future in as_completed(futures):
            src, tgt = futures[future]
            try:
                result_df = future.result()
                results.append(result_df)
                print(f"Completed validation for: {src} -> {tgt}")
            except Exception as exc:
                print(f"Table validation failed for {src} -> {tgt} with exception {exc}")

    # Combine all individual result DataFrames into one
    results_df = results[0]
    for df in results[1:]:
        results_df = results_df.union(df)
    
    return results_df