from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# Define the function to validate two tables
def validate_tables(table1: DataFrame, table2: DataFrame, primary_keys: list):
    # Step 1: Schema Validation
    schema1 = set((field.name, field.dataType) for field in table1.schema.fields)
    schema2 = set((field.name, field.dataType) for field in table2.schema.fields)
    
    if schema1 != schema2:
        print("Schema mismatch detected:")
        print("Fields in Table 1 but not in Table 2:", schema1 - schema2)
        print("Fields in Table 2 but not in Table 1:", schema2 - schema1)
    else:
        print("Schema validation passed: Schemas are identical")

    # Step 2: Row Count Comparison
    count_table1 = table1.count()
    count_table2 = table2.count()
    
    print(f"Table 1 Row Count: {count_table1}")
    print(f"Table 2 Row Count: {count_table2}")
    
    if count_table1 != count_table2:
        print(f"Row count mismatch: {abs(count_table2 - count_table1)}")

    else:
        print("Row count validation passed: Counts are identical")
    
    # Step 3: Data Validation by Primary Key
    # Find records in Table 1 not in Table 2, and vice versa, based on primary keys
    mismatch_t1_t2 = table1.alias("t1").join(table2.alias("t2"), on=primary_keys, how="left_anti")
    mismatch_t2_t1 = table2.alias("t2").join(table1.alias("t1"), on=primary_keys, how="left_anti")

    if mismatch_t1_t2.count() > 0:
        print("Records in Table 1 but not in Table 2:")
        mismatch_t1_t2.display(truncate=False)
    else:
        print("All records in Table 1 exist in Table 2.")

    if mismatch_t2_t1.count() > 0:
        print("Records in Table 2 but not in Table 1:")
        mismatch_t2_t1.display(truncate=False)
    else:
        print("All records in Table 2 exist in Table 1.")

    # Step 4: Column-wise Data Comparison and Mismatch Reporting
    mismatched_columns = []
    mismatch_details = []
    for column in set(table1.columns) - set(primary_keys):
        mismatched_rows = (
            table1.alias("t1")
            .join(table2.alias("t2"), on=primary_keys, how="inner")
            .filter(F.col(f"t1.{column}") != F.col(f"t2.{column}"))
            .select("t1.*", F.col(f"t2.{column}").alias(f"{column}_in_table2"))
        )
        
        diff_count = mismatched_rows.count()
        
        if diff_count > 0:
            mismatched_columns.append(column)
            print(f"Mismatch found in column '{column}': {diff_count} records differ.")
            mismatch_details.append((column, mismatched_rows))
        else:
            print(f"Column '{column}' validation passed: No differences found.")

    # Display all mismatched columns and their respective counts
    if mismatched_columns:
        print(f"\nSummary of Mismatched Columns: {mismatched_columns}")
        
        # Display detailed mismatches for the identified columns
        for col_name, mismatched_df in mismatch_details:
            print(f"\nDetails of mismatches for column '{col_name}':")
            mismatched_df.display(truncate=False)
    else:
        print("\nAll columns passed data comparison without mismatches.")
        
    # Display total count of mismatched records
    total_mismatched_records = sum(df.count() for _, df in mismatch_details)
    print(f"\nTotal number of mismatched records: {total_mismatched_records}")