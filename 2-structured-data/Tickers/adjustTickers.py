# EU: .FTSE, .FTSC,.GDAXI, .FCHI, .IBEX, .AEX, .BFX
# USA: .IXIC, .DJI, .NYA, .SPX
# ASIA PACIFIC: .N225, .HSI, .SSEC, .TWII, .JKSE, .NSEI

import pandas as pd
import eikon as ek
ek.set_app_id('9FB32FA719C8F1EE8CDEF1A')

df = ek.get_data('.NSEI', ['TR.IndexConstituentRIC' , 'TR.IndexConstituentName'], raw_output=True)
with open('NIFTY50.txt', 'a') as the_file:
    for idx in df["data"]:
        ticker = idx[1]
        if ticker is not None:
            the_file.write(ticker+'\n')
