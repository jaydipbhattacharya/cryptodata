import os

import crypto_extract as c

if __name__ == "__main__":
    # Below file need to have tokens , no space, no funny characters pls.
    # X-CMC_PRO_API_KEY=af3d24ae-b0b8-46b6-a557-e2f7a159b412
    # X-CoinAPI-Key=DCF01C8D-C34A-4BD2-8BB3-09810C7BB090

    filename = "C:\\Users\\" + "jaydi"  + "\\Documents\\newtokens.txt"
    tokens = {}
    with open(filename) as file:
        for line in file:
            x = line.strip().strip('"').split("=")
            tokens[x[0]] = x[1]
    c.extract(tokens)
