import json
import time
import logging
import boto3
from datetime import datetime, timedelta, timezone


WAITING_TIME = 250
# データ確認用のバケット
BUCKET = "test-awsbatch"
JOB_SENDER = "batch-job-sender"
DATA_CHECKER = "data_checker"
# 存在を確認するファイル名
FILE_NAME = "test.txt"
# loggerの作成
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, _context):
    LOGGER.info(event)

    s3_client = boto3.client("s3")
    lambda_client = boto3.client("lambda")

    str_dt = get_date()
    # 前日の日付を取得する
    one_days_before = add_days(str_dt, -1)
    status = check_data(s3_client, one_days_before)
    print(status)
    event["cnt"] = +1
    execute(event, WAITING_TIME, lambda_client, status)


def execute(event, waiting_time, lambda_client, status):

    if status:  # 対象のバケットにファイルが存在しているとき
        lambda_client.invoke(
            FunctionName=JOB_SENDER,
            InvocationType="RequestResponse",
            Payload=json.dumps(event),
            LogType="Tail"
        )

    else:  # データが無い場合
        time.sleep(waiting_time)
        lambda_client.invoke(
            FunctionName=DATA_CHECKER,
            InvocationType="Event",
            Payload=json.dumps(event),
            LogType="None"
        )


def check_data(s3_client, date):

    prefix = date
    response = s3_client.list_objects(
        Bucket=BUCKET,
        Prefix=prefix
    )

    assumed_keys = [f'{date}/{FILE_NAME}']
    try:
        keys = [content['Key'] for content in response['Contents']]
        print("keys:", keys)
        status = set(assumed_keys).issubset(keys)
    except KeyError:
        status = False

    return status


def get_date():
    jst = timezone(timedelta(hours=+9), "JST")
    jst_now = datetime.now(jst)
    dt = datetime.strftime(jst_now, "%Y-%m-%d")

    return dt


def datetime_to_str(date: datetime) -> str:
    year = str(date.year)
    month = str("{0:02d}".format(date.month))
    day = str("{0:02d}".format(date.day))
    str_date = '{0}-{1}-{2}'.format(year, month, day)

    return str_date


def str_to_datetime(str_date: str) -> datetime:

    return datetime.strptime(str_date, '%Y-%m-%d')


def add_days(str_dt: str, days: int) -> str:
    datetime_dt = str_to_datetime(str_dt)
    n_days_after = datetime_dt + timedelta(days=days)
    str_n_days_after = datetime_to_str(n_days_after)

    return str_n_days_after

