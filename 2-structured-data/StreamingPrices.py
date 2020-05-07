import eikon as ek
ek.set_app_key('80a60244246c4c139ea016a0c9dde616194983de')

df, err = ek.get_data(
    instruments = ['GOOG.O','MSFT.O', 'FB.O'],
    fields = ['BID','ASK']
)
print(df)

streaming_prices = ek.StreamingPrices(
    instruments = ['AAPL='],
    fields = ['DSPLY_NAME', 'BID', 'ASK'],
    on_update = lambda streaming_price, instrument_name, fields :
        print("Update received for {}: {}".format(instrument_name, fields))
)

streaming_prices.open()