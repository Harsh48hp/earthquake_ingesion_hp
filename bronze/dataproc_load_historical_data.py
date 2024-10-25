# Run below command in gcloud console
# 
# gcloud dataproc jobs submit pyspark gs://earthquake_analysis_by_hp/dataproc/bronze/load_historical_data_pyspark.py --cluster=harshal-learning-dataproc-24 --region=us-central1 --files=gs://earthquake_analysis_by_hp/dataproc/bronze/util.py