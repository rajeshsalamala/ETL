from pyspark.sql.functions import col, regexp_replace,trim


def transform(data, methods_di):
    columns = data.columns  # Use the provided DataFrame 'data'
    
    if methods_di.get('spaces'):
        for col_name in columns:
            # Use the trim function to remove leading and trailing spaces
            data = data.withColumn(col_name, trim(col(col_name)))

    if methods_di.get('digits_splchar'):
        for col_name in columns:
            # Use regexp_replace to remove digits and special characters
            data = data.withColumn(col_name, regexp_replace(col(col_name), '[^a-zA-Z\s]', ''))

    if methods_di.get('duplicates'):
        # Drop duplicate rows based on all columns
        data = data.dropDuplicates()
    if methods_di.get('text_repace'):
        # Drop duplicate rows based on all columns
        for col_name in columns:
            data = data.withColumn(col_name, regexp_replace(col(col_name), methods_di.get("ip_text"), methods_di.get("op_text")))
    if methods_di.get('regex_repace'):
        # Apply regular expression if regex is provided
        for col_name in columns:
            data = data.withColumn(col_name, regexp_replace(col(col_name), methods_di.get("ip_regex"), methods_di.get("op_regex")))
    return data
