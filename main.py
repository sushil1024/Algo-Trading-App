'''
Project Title: Market Stream Metrics
Author: Sushil Waghmare
Email id: sushilwaghmare2048@gmail.com
Github: https://github.com/sushil1024
'''

import dotenv
import pandas as pd
from fyers_apiv3 import fyersModel
import webbrowser
from fyers_apiv3.FyersWebsocket import data_ws
import redis
import os
from dotenv import load_dotenv
from models import create_table, insert_data
from log_config import get_logger
from helper import epoch_to_datetime, tick_to_df

logger = get_logger(__name__)

load_dotenv()
create_table()


class FyersConnect:

    def __init__(self):
        self.__redirect_uri = os.getenv('REDIRECT_URI')
        self.__client_id = os.getenv('CLIENT_ID')
        self.__secret_key = os.getenv('SECRET_KEY')
        self.__grant_type = "authorization_code"
        self.__response_type = "code"
        self.__state = "sample"

        self.temp_tick = {}
        self.temp_time = None

        self.appSession = fyersModel.SessionModel(
            client_id=self.__client_id,
            redirect_uri=self.__redirect_uri,
            response_type=self.__response_type,
            state=self.__state,
            secret_key=self.__secret_key,
            grant_type=self.__grant_type
        )

        rd = redis.Redis(host='localhost',
                         port=6379,
                         decode_responses=True)

        self.auth_code = os.getenv('AUTH_CODE')
        self.access_token = os.getenv('ACCESS_KEY')

        self.fyers_ws = data_ws.FyersDataSocket(
            access_token=self.access_token,
            log_path="",
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=self.onopen,
            on_close=self.onclose,
            on_error=self.onerror,
            on_message=self.onmessage
        )

        self.fyers_ws.connect()

    # Login - Generates Auth Code
    def genAuthCode(self):
        generateTokenUrl = self.appSession.generate_authcode()
        webbrowser.open(generateTokenUrl, new=1)

    # Generates Access Token
    def genaccesstoken(self):
        self.appSession.set_token(self.auth_code)
        response = self.appSession.generate_token()

        dotenv_file = dotenv.find_dotenv()
        logger.info(f"response:  {response}")

        # AUTH CODE expired
        if (response['code'] == -413) or (response['code'] == -8):
            self.genAuthCode()
            auth_code_new = str(input('Enter Auth Code: '))
            dotenv.set_key(dotenv_path=dotenv_file, key_to_set='AUTH_CODE', value_to_set=auth_code_new)

            self.fyers_ws.close_connection()
            exit('Re-run the program now!!')

        try:
            access_token = response['access_token']

            dotenv.set_key(dotenv_path=dotenv_file, key_to_set='ACCESS_KEY', value_to_set=access_token)
            logger.info('Access Token generated and saved!')

            self.fyers_ws.close_connection()
            exit('Re-run the program now!!')

        except Exception as e:
            logger.info(f"Access Token Exception: {e}")

    # Fyers Web Socket
    def onmessage(self, message):
        global temp_time
        try:
            timestamp = epoch_to_datetime(message['last_traded_time'])

            # minute update
            if (temp_time != timestamp.strftime("%Y-%m-%d %H:%M")) and self.temp_tick:
                temp_time = timestamp.strftime("%Y-%m-%d %H:%M")

                try:
                    df = tick_to_df(tick=self.temp_tick, symbol=message["symbol"])

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
                    logger.info(f"Data Processing Exception: {e}")

                self.temp_tick.clear()

            # fetch ticks
            self.temp_tick.setdefault(message["symbol"], {}).setdefault('c', []).append(message["ltp"])
            self.temp_tick.setdefault(message["symbol"], {}).setdefault('timestamp', []).append(timestamp)

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

    def onerror(self, message):
        logger.info(f"Connection Error: {message}")

        # Invalid ACCESS TOKEN
        if (message['code'] == -300) or (message['code'] == -99):
            logger.info('Invalid token. Resetting the Access Token!')
            self.genaccesstoken()
            exit()

    def onclose(self, message):
        logger.info(f"Connection Closed: {message}")

    def onopen(self):
        logger.info("Socket connected!")
        symbols = ['NSE:SBIN-EQ']
        self.fyers_ws.subscribe(symbols=symbols)
        self.fyers_ws.keep_running()


if __name__ == '__main__':
    obj = FyersConnect()
