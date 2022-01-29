import os

import crypto_extract as c

if __name__ == "__main__":
    # Below file need to have tokens , no space, no funny characters pls.
    # X-CMC_PRO_API_KEY=<your token from coin market cap api>
    # X-CoinAPI-Key=<your token from coin api>

    filename = "C:\\Users\\" + "jaydi"  + "\\Documents\\newtokens.txt"
    tokens = {}
    with open(filename) as file:
        for line in file:
            x = line.strip().strip('"').split("=")
            tokens[x[0]] = x[1]
    c.extract(tokens, False)
