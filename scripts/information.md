Bronze Bucket Name - bronze-yt-aws-pipeline-eu-dev
Silver Bucket Name - silver-yt-aws-pipeline-eu-dev
Gold Bucket Name - gold-yt-aws-pipeline-eu-dev
Script Bucket Name - script-yt-aws-pipeline-eu-dev

glue
bronze_database - bronze_yt_pipeline_dev
bronze_table - raw_statistics
silver_database - silver_yt_pipeline_dev
silver_table - clean_statistics
gold_database - gold_yt_pipeline_dev

SNS ARN - arn:aws:sns:eu-north-1:877314709092:yt-data-pipeline-alerts-dev:880b97f5-ef84-4884-a5e2-e3e1e8f357a5

     --silver_database       — silver_yt_pipeline_dev
    --gold_bucket           — gold-yt-aws-pipeline-eu-dev
    --gold_database         — gold_yt_pipeline_dev

Na kraju projekta dodaj sledece:

- Da se csv i json file automatski skidaju sa website-a jednom dnevno nocu i salju na aws
- Da se Terraform koristi umesto gui-a za ra rad sa aws
- Da ubacim Airfrlow za okrestraciju na dockeru
- da ubacim PySpark za obradu velikih fajlova
- da gold sloj stavim na dwh na redshift za bolji reporting
  ?
