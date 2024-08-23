import dotenv
import pandas as pd
from fyers_apiv3 import fyersModel
import webbrowser
from fyers_apiv3.FyersWebsocket import data_ws
import redis
import os
import datetime
from dotenv import load_dotenv
from models import create_table, insert_data

load_dotenv()
create_table()

redirect_uri = os.getenv('REDIRECT_URI')
client_id = os.getenv('CLIENT_ID')
secret_key = os.getenv('SECRET_KEY')
grant_type = "authorization_code"
response_type = "code"
state = "sample"

epoch_to_datetime = lambda epoch: datetime.datetime.fromtimestamp(epoch)
temp_tick = {}
temp_time = None

appSession = fyersModel.SessionModel(client_id=client_id, redirect_uri=redirect_uri, response_type=response_type, state=state, secret_key=secret_key, grant_type=grant_type)

rd = redis.Redis(host='localhost',
                 port=6379,
                 decode_responses=True)


# Login - Generates Auth Code
def genAuthCode():
    generateTokenUrl = appSession.generate_authcode()
    print(generateTokenUrl)
    webbrowser.open(generateTokenUrl, new=1)


auth_code = os.getenv('AUTH_CODE')
access_token = os.getenv('ACCESS_KEY')


# Generates Access Token
def genaccesstoken():
    appSession.set_token(auth_code)
    response = appSession.generate_token()

    dotenv_file = dotenv.find_dotenv()
    print('dotenv_file: ', dotenv_file)
    print('response: ', response)

    # AUTH CODE expired
    if (response['code'] == -413) or (response['code'] == -8):
        genAuthCode()
        auth_code_new = str(input('Enter Auth Code: '))
        dotenv.set_key(dotenv_path=dotenv_file, key_to_set='AUTH_CODE', value_to_set=auth_code_new)

        fyers_ws.close_connection()
        exit('Re-run the program now!!')

    try:
        access_token = response['access_token']

        dotenv.set_key(dotenv_path=dotenv_file, key_to_set='ACCESS_KEY', value_to_set=access_token)
        print('Access Token generated and saved!')

        fyers_ws.close_connection()
        exit('Re-run the program now!!')

    except Exception as e:
        print(f"Access Token Exception: {e}")


# Fyers Web Socket
def onmessage(message):
    global temp_time
    # print("Response: ", message)
    try:
        if message['code'] == 200:
            print("Connected!")
    except:
        try:
            timestamp = epoch_to_datetime(message['last_traded_time'])

            # minute update
            if (temp_time != timestamp.strftime("%Y-%m-%d %H:%M")) and temp_tick:
                temp_time = timestamp.strftime("%Y-%m-%d %H:%M")

                try:
                    df = pd.DataFrame.from_dict(temp_tick[message["symbol"]])
                    df = df.sort_values(by='timestamp')
                    df = df.drop(index=df.index[0])     # drop the first row

                    df.set_index('timestamp', inplace=True)

                    print(f"Live ticks: {temp_tick}")

                    df = df.resample('min').agg(
                        open=('c', 'first'),
                        high=('c', 'max'),
                        low=('c', 'min'),
                        close=('c', 'last')
                    ).dropna()

                    print(f'DF: {df}')

                    row = df.iloc[-1]
                    timestamp = df.index[-1]

                    insert_data(
                        symbol=message["symbol"],
                        open=float(row['open']),
                        high=float(row['high']),
                        low=float(row['low']),
                        close=float(row['close']),
                        timestamp=timestamp
                    )

                except Exception as e:
                    print(f"Data Processing Exception: {e}")

                temp_tick.clear()

            temp_tick.setdefault(message["symbol"], {}).setdefault('c', []).append(message["ltp"])
            temp_tick.setdefault(message["symbol"], {}).setdefault('timestamp', []).append(timestamp)

            # Write to Redis
            # rd.hset(f'symbol: {message["symbol"]}', mapping={
            #     'O': message['open_price'],
            #     'H': message['high_price'],
            #     'L': message['low_price'],
            #     'C': message['ltp'],
            #     'timestamp': timestamp
            # })
            #
            # Fetch from Redis
            # tick = rd.hgetall('symbol: NSE:SBIN-EQ')
            # store into DB...

        except:
            pass


def onerror(message):
    print("Connection Error: ", message)

    # Invalid ACCESS TOKEN
    if (message['code'] == -300) or (message['code'] == -99):
        print('Invalid token. Resetting the Access Token!')
        genaccesstoken()
        exit()


def onclose(message):
    print("Connection Closed: ", message)


def onopen():
    symbols = ['NSE:SBIN-EQ']
    fyers_ws.subscribe(symbols=symbols)
    fyers_ws.keep_running()


if __name__ == '__main__':
    fyers_ws = data_ws.FyersDataSocket(
        access_token=access_token,
        log_path="",
        litemode=False,
        write_to_file=False,
        reconnect=True,
        on_connect=onopen,
        on_close=onclose,
        on_error=onerror,
        on_message=onmessage
    )

    fyers_ws.connect()
