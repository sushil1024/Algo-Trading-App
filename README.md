# Market-Stream-Metrics
Python backend to retrieve realtime market data for desired symbol(s), determine minute candle data and populate database.

# API used
Fyers API

# Requirements excluding libraries
1. Fyers trading account
2. API Auth Code (unique each day)
3. API Secret Key (unique each day)
4. Client ID
5. Secret Key
6. env file containing all above

# Create Fyers API
1. Login to Fyers account.
2. Navigate to API on https://myapi.fyers.in/dashboard
3. Create an app

   ![img_1.png](img/img_1.png)

4. Enter all required details

   ![img_2.png](img/img_2.png)

5. You will get your app ID and secret key

   ![img_3.png](img/img_3.png)

6. Your API dashboard will show your app

    ![img_4.png](img/img_4.png)

# API Integration
1. Fyers library and documentation: https://pypi.org/project/fyers-apiv3/
2. Required logs will be generated in the home directory by the library.

# Output:

1. CLI:

![img.png](img/op1.png)

2. Database:

![img.png](img/op2.png)

