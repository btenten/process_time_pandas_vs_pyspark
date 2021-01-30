# %%
import pandas as pd
import numpy as np
import pyspark
from pyspark.sql import SparkSession
import os
import time
import seaborn as sns
import matplotlib.pyplot as plt

# %%
path = 'path/to/your/csv/file'

# %%
def pd_import_file():
    start_time = time.time()
    df = pd.read_csv(f"{path}/PS_20174392719_1491204439457_log.csv")
    total_run_time = time.time() - start_time
    return total_run_time

# %%
def pyspark_import_file():
    start_time = time.time()
    df = spark.read.csv(f"{path}/PS_20174392719_1491204439457_log.csv", header = True, inferSchema = True)
    total_run_time = time.time() - start_time
    return total_run_time

# %%
def pd_filter_groupby(df_pd):
    start_time1 = time.time()
    df_pd_filter = df_pd[(df_pd['type'].isin(['CASH_OUT', 'TRANSFER'])) & (df_pd['amount'] >= 100000)]
    filter_runtime = time.time()-start_time1

    start_time2 = time.time()
    df_pd_groupby = df_pd.groupby(['type'])['isFraud', 'isFlaggedFraud'].sum()
    groupby_runtime = time.time()-start_time2

    return filter_runtime, groupby_runtime

# %%
def pyspark_filter_groupby(df_pyspark):
    start_time1 = time.time()
    df_pyspark_filter = spark.sql("""Select *
                                        From transactions
                                        Where type in('CASH_OUT', 'TRANSFER')
                                        and amount >= 100000""")
    #df_pyspark_filter.show()
    filter_runtime = time.time()-start_time1

    start_time2 = time.time()
    df_pyspark_groupby = spark.sql("""Select
                                        type,
                                        sum(isFraud) as total_is_fraud,
                                        sum(isFlaggedFraud) as total_is_flagged_fraud
                                      From transactions
                                      Group by 1""")
    #df_pyspark_groupby.show()
    groupby_runtime = time.time()-start_time2

    return filter_runtime, groupby_runtime

# %%
spark = SparkSession.builder.master("local[*]").getOrCreate()
print('Spark session created.')

# %%
df_read_csv_time = pd.DataFrame(columns = ['loop_num', 'pandas', 'pyspark'])

# %%
for i in range(1,50):
    print(f"Start loop number {i}..")
    df_read_csv_time = df_read_csv_time.append({'loop_num': i,
                                                'pandas': pd_import_file(),
                                                'pyspark': pyspark_import_file()},
                                                ignore_index = True)

# %%
df_read_csv_time.describe()
print(f"Avg read_csv time (sec):\n--pandas: {df_read_csv_time.describe()['pandas']['mean']}\n--pyspark: {df_read_csv_time.describe()['pyspark']['mean']}")

# %%
df_filter_groupby_time = pd.DataFrame(columns = ['loop_num', 'pandas_filter', 'pandas_groupby', 'pyspark_filter', 'pyspark_groupby'])

# %%
df_pd_ori = pd.read_csv(f"{path}/PS_20174392719_1491204439457_log.csv")
df_pyspark_ori = spark.read.csv(f"{path}/PS_20174392719_1491204439457_log.csv", header = True, inferSchema = True)

# %%
for i in range(0, 50):
    df_pd = df_pd_ori.copy()
    df_pyspark = df_pyspark_ori
    df_pyspark.createOrReplaceTempView('transactions')

    print(f"Start loop | pandas | number {i}..")
    pd_filter_runtime, pd_groupby_runtime = pd_filter_groupby(df_pd)

    print(f"Start loop | pyspark | number {i}..")

    pyspark_filter_runtime, pyspark_groupby_runtime = pyspark_filter_groupby(df_pyspark)

    df_filter_groupby_time = df_filter_groupby_time.append({'loop_num': i,
                                                'pandas_filter': pd_filter_runtime,
                                                'pandas_groupby': pd_groupby_runtime,
                                                'pyspark_filter': pyspark_filter_runtime,
                                                'pyspark_groupby': pyspark_groupby_runtime},
                                                ignore_index = True)

# %%
_ = sns.lineplot(data = df_read_csv_time, x = 'loop_num', y = 'pandas')
_ = sns.lineplot(data = df_read_csv_time, x = 'loop_num', y = 'pyspark')
_.set_title('read_csv run time | pandas vs pyspark')
_.set_ylabel('Run time (sec)')
_.set_xlabel('Loop number')
_.legend(['pandas', 'pyspark'])
plt.show()

# %%
_ = sns.lineplot(data = df_filter_groupby_time, x = 'loop_num', y = 'pandas_filter')
_ = sns.lineplot(data = df_filter_groupby_time, x = 'loop_num', y = 'pyspark_filter')
_.set_title('filter run time | pandas vs pyspark')
_.set_ylabel('Run time (sec)')
_.set_xlabel('Loop number')
_.legend(['pandas', 'pyspark'])
plt.show()

# %%
_ = sns.lineplot(data = df_filter_groupby_time, x = 'loop_num', y = 'pandas_groupby')
_ = sns.lineplot(data = df_filter_groupby_time, x = 'loop_num', y = 'pyspark_groupby')
_.set_title('groupby run time | pandas vs pyspark')
_.set_ylabel('Run time (sec)')
_.set_xlabel('Loop number')
_.legend(['pandas', 'pyspark'])
plt.show()
