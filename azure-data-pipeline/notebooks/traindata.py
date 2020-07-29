# Databricks notebook source
import pickle
import pandas as pd  
import numpy as np  
import matplotlib.pyplot as plt  
import seaborn as seabornInstance 
from sklearn.model_selection import train_test_split 
from sklearn.linear_model import LinearRegression
from sklearn import metrics

#dbutils.widgets.text("input", "","")
#datafile = dbutils.widgets.get("input")
datafile = "transformed.csv"
storage_account_name = getArgument("storage_account_name")
storage_container_name = getArgument("storage_container_name")

mount_point = "/mnt/prepared"
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()): 
  dbutils.fs.mount(
    source = "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net",
    mount_point = mount_point,
    extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":dbutils.secrets.get(scope = "testscope", key = "StorageKey")})

dataset = pd.read_csv("/dbfs/"+mount_point+"/"+datafile) 
X = dataset['MinTemp'].values.reshape(-1,1)
y = dataset['MaxTemp'].values.reshape(-1,1)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
regressor = LinearRegression()  
regressor.fit(X_train, y_train) 
print("Model trained.")

#To retrieve the intercept:
print("Regressor intercept: %f" % regressor.intercept_)
#For retrieving the slope:
print("Regressor coef: %f" % regressor.coef_)

filepath_to_save = '/dbfs' + mount_point + '/regression.pkl'

s = pickle.dump(regressor, open(filepath_to_save, "wb"))


# COMMAND ----------

