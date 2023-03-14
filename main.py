import asyncio
import time
import datetime
import boto3
from binance import AsyncClient, BinanceSocketManager


def upload_file_to_s3(local_file_path, remote_file_path, s3_bucket_name='tudihello'):
    """
    Uploads a local file to an S3 bucket.

    Args:
    local_file_path (str): The file path of the local file to upload.
    s3_bucket_name (str): The name of the S3 bucket to upload to.
    s3_object_key (str): The object key to use for the uploaded file in the S3 bucket.

    Returns:
    None
    """
    s3_client = boto3.client('s3')
    with open(local_file_path, "rb") as f:
        s3_client.upload_fileobj(f, s3_bucket_name, remote_file_path)


async def main():
    active_file_time = int(round(time.time()) / 60)
    new_local_data_file_path = f'./data/{int(active_file_time * 60)}.tsv'

    f = open(new_local_data_file_path, 'w')
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    trade_socket = bm.trade_socket('BTCUSDT')
    async with trade_socket as tscm:
        while True:
            res = await tscm.recv()
            new_file_time = int(res['T'] / (1000 * 60))
            print(res)
            if new_file_time != active_file_time:
                f.close()
                local_data_file_path = f'./data/{str(active_file_time * 60)}.tsv'
                remote_data_file_path = f'data_1_min/{str(active_file_time * 60)}.tsv'

                upload_file_to_s3(local_data_file_path, remote_data_file_path)
                active_file_time = new_file_time
                new_local_data_file_path = f'./data/{int(active_file_time * 60)}.tsv'

                f = open(new_local_data_file_path, 'w')
                print(' #' * 50)
                print(' #' * 50)
                print(' #' * 50)
                print(' #' * 20 + ' new file:' + new_local_data_file_path + ' #' * 20)
                print(' #' * 50)
                print(' #' * 50)
                print(' #' * 50)

            timestamp = f"{datetime.datetime.fromtimestamp(int(res['T'] / 1000)):%Y-%m-%d %H:%M:%S}"
            maker = '0'
            if res['m']:  # 1 if bought, 0 if sold
                maker = '1'

            line = str(res['t']) + '\t'
            line += str(res['s']) + '\t'
            line += '{:.2f}'.format(round(float(res['p']), 2)) + '\t'
            line += str(res['q'])[:-3] + '\t'
            line += str(timestamp) + '\t'
            line += maker + '\n'
            f.write(line)
            print(line)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
