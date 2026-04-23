from yahoofinancials import YahooFinancials
tickers = ['AAPL', 'GOOG', 'C']
yahoo_financials:YahooFinancials = YahooFinancials(tickers, concurrent=False, max_workers=1, country="US")
prices = yahoo_financials.get_stock_price_data()
print(prices)