#!/bin/bash

# Script to copy the downloaded YouTube data files to the appropriate S3 buckets for each region.

# Region: CA
aws s3 cp CAvideos.csv s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics/region=ca/
aws s3 cp CA_category_id.json s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics_reference_data/region=ca/

# Region: DE
aws s3 cp DEvideos.csv s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics/region=de/
aws s3 cp DE_category_id.json s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics_reference_data/region=de/

# Region: FR
aws s3 cp FRvideos.csv s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics/region=fr/
aws s3 cp FR_category_id.json s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics_reference_data/region=fr/

# Region: GB
aws s3 cp GBvideos.csv s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics/region=gb/
aws s3 cp GB_category_id.json s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics_reference_data/region=gb/

# Region: IN
aws s3 cp INvideos.csv s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics/region=in/
aws s3 cp IN_category_id.json s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics_reference_data/region=in/

# Region: JP
aws s3 cp JPvideos.csv s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics/region=jp/
aws s3 cp JP_category_id.json s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics_reference_data/region=jp/

# Region: KR
aws s3 cp KRvideos.csv s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics/region=kr/
aws s3 cp KR_category_id.json s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics_reference_data/region=kr/

# Region: MX
aws s3 cp MXvideos.csv s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics/region=mx/
aws s3 cp MX_category_id.json s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics_reference_data/region=mx/

# Region: RU
aws s3 cp RUvideos.csv s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics/region=ru/
aws s3 cp RU_category_id.json s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics_reference_data/region=ru/

# Region: US
aws s3 cp USvideos.csv s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics/region=us/
aws s3 cp US_category_id.json s3://bronze-yt-aws-pipeline-eu-dev/youtube/raw_statistics_reference_data/region=us/