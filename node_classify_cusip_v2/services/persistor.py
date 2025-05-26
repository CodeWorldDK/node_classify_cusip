def persist_dataframe(df, target_table: str):
    df.write.mode("overwrite").save_as_table(target_table)
