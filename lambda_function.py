import os
import json
import mysql.connector
import urllib.parse
import boto3

s3 = boto3.client('s3', region_name="eu-north-1")


# If you want to see and track lambda logs, go to CloudWatch and find stream log groups.
def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print(f"{bucket_name=}")
    print(f"{key=}")

    s3_url = f"s3://{bucket_name}/{key}"
    print(s3_url)

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

    print(f"{key.split('/')[-1]} succesfully inserted to RDS Table.")

    # write copy statement
    response = s3.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key': key},
        Key=f"data_finished/{key.split('/')[-1]}",
    )

    print(f"File '{key.split('/')[-1]}' copied to '{bucket_name}/data_finished/'")
    print(response)

    s3.delete_object(Bucket=bucket_name, Key=key)
    print(f"File '{key.split('/')[-1]}' succesfully deleted from '{bucket_name}/data_1_min/'")

    return {
        'statusCode': 200,
        'body': json.dumps('File has been Successfully Copied')
    }