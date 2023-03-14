import os
import json
import mysql.connector
import urllib.parse
import boto3

s3 = boto3.client('s3')


def lambda_handler(event, context):
    # Event içerisinden bucket'a her .tsv dosyası eklendiğinde dosyanın
    # ismini al ve s3-url oluştur
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    s3_url = f"s3://{bucket}/{key}"
    print(s3_url)

    # Aurora mysql baglantisi
    cnx = mysql.connector.connect(host=os.environ["RDS_HOSTNAME"], user=os.environ["RDS_USERNAME"],
                                  passwd=os.environ["RDS_PASSWORD"],
                                  database=os.environ["RDS_DB_NAME"], port=os.environ["RDS_PORT"])

    print("connection succesfull")
    aurora_insert = f"LOAD DATA FROM S3 '{s3_url}' INTO TABLE {os.environ['RDS_DB_NAME']}.BTCUSDT FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' (bid, parameter, price, quantity, time, maker);"

    print(f"{aurora_insert=}")

    cur = cnx.cursor()
    cur.execute(aurora_insert)
    cnx.commit()
    cnx.close()

    return {"statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": json.dumps(cur.lastrowid, indent=4, sort_keys=True, default=str)}
