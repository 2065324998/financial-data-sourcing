import requests
import json

# use your own account number, password and appid to authenticate 
account = "702197"
password = "90F1B289"
appid = "656797f7-ee55-48a6-9361-dec2c78fade2"

# Build auth url
authurl = f"https://ftl.fasttrack.net/v1/auth/login?account={account}&pass={password}&appid={appid}"

# make authentication request
authresponse = requests.get(authurl)

# parse result and extract token
token = authresponse.json()['token']

def etf_example():
    tickers = ["SPAB", "SPBO", "SPDW", "SPEM", "SPEU", "SPGM", "SPHY", "SPIB", "SPIP", "SPLB", "SPLG", "SPMB", "SPMD", "SPSB", "SPSM", "SPTI", "SPTL", "SPTM", "SPTS", "SPYD", "SPYG", "SPYV"]
    # construct a header object containing your credentials.
    headers = {
        'appid': appid,
        'token': token
    }

    # Initialize an empty list to hold all details
    all_details = []

    for ticker in tickers:
        # Build URL for each ticker
        url = f"https://ftl.fasttrack.net/v1/ref/{ticker}/details"

        # Make GET request for each ticker
        response = requests.get(url, headers=headers)

        # Check if the response is successful
        if response.status_code == 200:
            detail = response.json()
            # Append the detail to the all_details list
            all_details.append(detail)
        else:
            print(f"Failed to get data for ticker: {ticker}")

    # Print all items in all_details
    print("Ticker, Expense, Objective, Security Type")
    for detail in all_details:
        print(
            detail['ticker'],
            ",",
            '{:.2f}%'.format(float(detail["expenseratio"])*100),
            ",",
            detail["objective"],
            ",",
            detail["security_type"]
        )
        print(detail)


def equity_example():
    tickers = ["META"]
    ticker = tickers[0]
    # construct a header object containing your credentials.
    headers = {
        'appid': appid,
        'token': token
    }

    # Initialize an empty list to hold all details
    all_details = []

    for ticker in tickers:
        # Build URL for each ticker
        url = f"https://ftl.fasttrack.net/v1/data/{ticker}/range"

        # Make GET request for each ticker
        response = requests.get(url, headers=headers)

        # Check if the response is successful
        if response.status_code == 200:
            detail = response.json()
            # Append the detail to the all_details list
            all_details.append(detail)
            print(detail)
        else:
            print(f"Failed to get data for ticker: {ticker}")




if __name__ == "__main__":
    etf_example()
    equity_example()