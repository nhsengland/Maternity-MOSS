# Databricks notebook source
def save_to_Analysis_Lake(df, fileLoc_A):
    #Save Events Extract
    df_to_save = spark.table(df) # This takes the TEMP VIEW vw_mixed_final created using SQL and ensures that the function can read it. (I coded in SQL to create the TEMP VIEW.)
    path = "abfss://"+containerName_A+"@"+lakeName_A+folder_A+fileLoc_A
   
    # Delete the file if it already exists, avoiding an issue with permission to overwrite the file if it was previously created by someone else:
    if fn_test_file_exists(path) == True:

        dbutils.fs.rm(path, recurse=True) 
    
    fn_write_dataframe_to_csv(df=df_to_save, path_final_csv_save=path)


# COMMAND ----------

def fn_test_file_exists(path_final_csv_save: str):

    path = path_final_csv_save

    try:
        files = dbutils.fs.ls(path)
        return True
    except:
        return False

def fn_write_dataframe_to_csv(df: F.DataFrame, path_final_csv_save: str): 

    path_temp_csv_save = path_final_csv_save + "_temp" 
 
    df.coalesce(1) \
        .write.mode("overwrite") \
        .option("encoding", "UTF-8") \
        .csv(path_temp_csv_save, header=True)  
 
    list_files = dbutils.fs.ls(path_temp_csv_save) 
 
    for sub_files in list_files: 
 
        if sub_files.name[-4:] == ".csv": 
 
            dbutils.fs.cp("/".join([path_temp_csv_save, sub_files.name]), path_final_csv_save) 
 
    _ = dbutils.fs.rm(path_temp_csv_save, recurse=True) # assigning to _ to suppress output, returning this is not important
