# process_time_pandas_vs_pyspark
Compare data processing time between pandas and pyspark (roughly)  
by capturing run time of data-loading, filtering, and grouping process.
  
  
  
### Environment  
-- Macbook Pro 2019  
-- MacOS: BigSur  
-- Memory: 8GB

  
### Dataset used
-- Synthetic Financial Datasets For Fraud Detection  
-- https://www.kaggle.com/ntnu-testimon/paysim1  
-- 11 columns  
-- 6.3M rows
  
  
   
### Results
  ![alt text](https://github.com/btenten/process_time_pandas_vs_pyspark/blob/main/read_csv_run_time.png?raw=true)  
  ![alt text](https://github.com/btenten/process_time_pandas_vs_pyspark/blob/main/filter_run_time.png?raw=true)  
  ![alt text](https://github.com/btenten/process_time_pandas_vs_pyspark/blob/main/groupby_run_time.png?raw=true)  
