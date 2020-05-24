lambdaimport boto3
import os
import datetime
import subprocess
import sys
import json
import shutil
import math

for root, dirs, files in os.walk('/tmp'):
    for f in files:
        os.unlink(os.path.join(root, f))
    for d in dirs:
        shutil.rmtree(os.path.join(root, d))
        
subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')
import yfinance as yf

def lambda_handler(event, context):
    #initialize boto3 client
    fh = boto3.client("firehose","us-east-2")
    
    ticker_list=["FB","SHOP","BYND","NFLX","PINS","SQ","TTD","OKTA","SNAP","DDOG"]
    batch_data = []
    for ti in ticker_list:
        tickers = yf.Ticker(ti)
        data = tickers.history(start="2020-05-14", end="2020-05-15", interval="1m")
        data = data.reset_index() 

        for i, row in data.iterrows():
            json_dict = {}
            json_dict['high'] = row.loc["High"]
            json_dict['low'] = row.loc["Low"]
            json_dict['ts'] = str(row.loc['Datetime'])
            json_dict['name'] = ti

            batch_data.append({
                'Data': json.dumps(json_dict).encode('utf-8')
        })
        
    fives = math.ceil((len(batch_data)/500))
    for i in range(fives):
        if i == 0:
            start = 0
        else:
            start = i*500+(1)
        if ((i+1)*500) > len(batch_data):
            stop = len(batch_data)
        else:
            stop = (i+1)*500
        response = fh.put_record_batch(
            DeliveryStreamName="sta9760-p3-collector_to_transformer",
            Records=batch_data[start:stop])
    # return
    return {
        'statusCode': 200,
        'body': json.dumps(f'Done! Recorded: {data}')
    }