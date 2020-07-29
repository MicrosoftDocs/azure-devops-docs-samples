# Databricks notebook source
dbutils.widgets.text("input", "","")
datafile = dbutils.widgets.get("input")
#datafile = "sample.csv"
storage_account_name = getArgument("storage_account_name")
storage_container_name = getArgument("storage_container_name")

#mount the blob storage that represents the target data
mount_point = "/mnt/prepared"
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()): 
  dbutils.fs.mount(
    source = "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net",
    mount_point = mount_point,
    extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":dbutils.secrets.get(scope = "testscope", key = "StorageKey")})

# read the files with column information
df = spark.read.format("csv")\
.option("inferSchema", 'true')\
.option("header",'true') \
.load(mount_point+"/"+datafile)

# transform the dataframe and keep only the 2 columns that are needed for this model training
df = df.select('MinTemp','MaxTemp').dropna()
filepath_to_save = '/dbfs' + mount_point + '/transformed.csv'
df.toPandas().to_csv(filepath_to_save,mode = 'w', index=False)


# COMMAND ----------

