from moto import mock_aws
import boto3
import json
import os
import sys
import importlib.util

# 1. OVE LINIJE MORAJU BITI PRVE (Pre uvoza boto3 i originalnog koda)
os.environ["S3_BUCKET_SILVER"] = "mock-silver-bucket"
os.environ["GLUE_DB_SILVER"] = "yt_pipeline_silver_dev"
os.environ["GLUE_TABLE_REFERENCE"] = "clean_reference_data"
os.environ["SNS_ALERT_TOPIC_ARN"] = ""


# 2. UČITAVANJE ORIGINALNOG KODA PREKO APSOLUTNE PUTANJE
putanja_do_lambda_fajla = r"C:\Python projekti\youtube_aws_pipeline_2026\lambdas\json_to_parquet\lambda_function.py"

try:
    spec = importlib.util.spec_from_file_location(
        "modul_lambda", putanja_do_lambda_fajla)
    lambda_modul = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lambda_modul)
    lambda_handler = lambda_modul.lambda_handler
    print("[USPEH] Originalni lambda_function je uspešno učitan!")
except Exception as e:
    print(
        f"[GREŠKA] Ne mogu da pronađem fajl na putanji: {putanja_do_lambda_fajla}")
    print(f"Detalji greške: {e}")
    sys.exit(1)

# 3. PODACI ZA TEST (Tvoj YouTube JSON primer)
test_json_data = {
    "kind": "youtube#videoCategoryListResponse",
    "etag": "\"ld9biNPKjAjgjV7EZ4EKeEGrhao/1v2mrzYSYG6onNLt2qTj13hkQZk\"",
    "items": [
        {"kind": "youtube#videoCategory", "id": "1", "snippet": {
            "channelId": "UCBR8-60", "title": "Film & Animation"}},
        {"kind": "youtube#videoCategory", "id": "2", "snippet": {
            "channelId": "UCBR8-60", "title": "Autos & Vehicles"}},
        {"kind": "youtube#videoCategory", "id": "10", "snippet": {
            "channelId": "UCBR8-60", "title": "Music"}}
    ]
}

# 4. SIMULACIJA AWS OKRUŽENJA I POKRETANJE
with mock_aws():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="mock-silver-bucket")

    # Stavljamo fajl u "region=US" folder kako bi tvoj korak 5 uspešno izvukao region "US"
    test_key = "raw_data/region=US/category.json"
    s3.put_object(Bucket="mock-silver-bucket", Key=test_key,
                  Body=json.dumps(test_json_data))

    # Pravimo lažni event koji simulira S3 okidač
    event = {
        "Records": [{
            "s3": {
                "bucket": {"name": "mock-silver-bucket"},
                "object": {"key": test_key}
            }
        }]
    }

    # Pokretanje
    print("\n[START] Pokrećem lambda_handler sa mock podacima...\n")
    rezultat = lambda_handler(event, None)
    print("\n=== REZULTAT IZVRŠAVANJA ===")
    print(json.dumps(rezultat, indent=2))
