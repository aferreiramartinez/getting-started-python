# EU: .FTSE, .FTSC,.GDAXI, .FCHI, .IBEX, .AEX, .BFX
# USA: .IXIC, .DJI, .NYA, .SPX/.INX
# ASIA PACIFIC: .N225, .HSI, .SSEC, .TWII, .JKSE, .NSEI

import pandas as pd
import eikon as ek
ek.set_app_id('80a60244246c4c139ea016a0c9dde616194983de')

df = ek.get_data('.INX', ['TR.IndexConstituentRIC' , 'TR.IndexConstituentName'], raw_output=True)
with open('SP500.txt', 'a') as the_file:
    for idx in df["data"]:
        ticker = idx[1]
        if ticker is not None:
            the_file.write(ticker+'\n')
