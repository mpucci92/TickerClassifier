import pandas as pd
import numpy as np
import requests
import zipfile as zip
from io import BytesIO
import yfinance as yf
import os
import glob
from datetime import date

url = "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/us-spdrs-allholdings-monthly.zip"
root_directory_etfs = os.path.dirname(os.path.realpath(__file__))
today = date.today()

def extractZip(url_zip, extract_path):
    """
    :param url_zip: Url address pointing to the Zip file of SPDR monthly holdings
    :param extract_path: Path to unzip the zip file contents to
    """
    req = requests.get(url_zip)
    print('Downloading Completed')
    zipfile = zip.ZipFile(BytesIO(req.content))
    zipfile.extractall(extract_path)

def nameETF(path_etf):
    """
    :param path_etf: path to etf excel spreadsheet
    :return: name of the ETF
    """
    df = pd.read_excel(path_etf,nrows=1)
    return df.columns[1]

def tickerGroupings(path_etf):
    """
    :param path_etf: path to etf excel spreadsheet
    :return: return dataframe of company name, ticker and the sector
    """
    df = pd.read_excel(path_etf, skiprows=4, skipfooter=21)
    df = df.loc[:,['Name', 'Ticker', 'Sector']]
    return df

def tickerSector(ticker):
    """
    :param ticker: Ticker
    :return: Sector of Ticker
    """
    ticket = yf.Ticker(ticker)
    return ticket.info['sector']

def stripRegistered(dataframe):
    noTrademark = []
    text = dataframe['Category']

    for category in text:
        cleanText = []
        for item in category.split(" "):
            item = item.strip('®')
            item = item.strip('℠')
            item = item.strip('™')
            cleanText.append(item)

        newString = " ".join(cleanText)

        noTrademark.append(newString)

    return noTrademark

def replace(dataframe):
    """
    :param dataframe: dataframe to apply data postprocessing
    :return: Dataframe with a post-processed Category Column
    """
    dataframe['Category'] = dataframe['Category'].str.replace("The Communication Services Select Sector SPDR Fund",
                                                              "Communication Services")
    dataframe['Category'] = dataframe['Category'].str.replace("The Consumer Discretionary Select Sector SPDR Fund",
                                                              "Consumer Discretionary")
    dataframe['Category'] = dataframe['Category'].str.replace("The Consumer Staples Select Sector SPDR Fund",
                                                              "Consumer Staples")
    dataframe['Category'] = dataframe['Category'].str.replace("The Energy Select Sector SPDR Fund", "Energy")
    dataframe['Category'] = dataframe['Category'].str.replace("The Financial Select Sector SPDR Fund", "Financial")
    dataframe['Category'] = dataframe['Category'].str.replace("The Health Care Select Sector SPDR Fund", "Health Care")
    dataframe['Category'] = dataframe['Category'].str.replace("The Industrial Select Sector SPDR Fund", "Industrial")
    dataframe['Category'] = dataframe['Category'].str.replace("The Materials Select Sector SPDR Fund", "Materials")
    dataframe['Category'] = dataframe['Category'].str.replace("The Real Estate Select Sector SPDR Fund", "Real Estate")
    dataframe['Category'] = dataframe['Category'].str.replace("The Technology Select Sector SPDR Fund", "Technology")
    dataframe['Category'] = dataframe['Category'].str.replace("The Utilities Select Sector SPDR Fund", "Utilities")
    dataframe['Category'] = dataframe['Category'].str.replace("SPDR","")
    dataframe['Category'] = dataframe['Category'].str.replace("ETF Trust", "")
    dataframe['Category'] =dataframe['Category'].str.replace("ETF","")
    dataframe['Category'] =dataframe['Category'].str.replace("Kensho","")
    dataframe['Category'] = dataframe['Category'].str.replace("  "," ")
    dataframe['Category'] = dataframe['Category'].str.strip()

    return dataframe

if __name__ == '__main__':
    url = "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/us-spdrs-allholdings-monthly.zip"
    root_directory_etfs = os.path.dirname(os.path.realpath(__file__))
    today = date.today()
    df_cols = ['Name', 'Ticker' , 'Sector', 'Category']
    dfBase = pd.DataFrame(columns=df_cols)

    for path_etf in glob.glob(root_directory_etfs+'\MonthlyHoldings\*.xlsx'):
        try:
            tickerGroups = tickerGroupings(path_etf)
        except Exception as e:
            continue

        nameCol = [nameETF(path_etf)] * len(tickerGroups)
        tickerGroups['Category'] = nameCol
        dfAppend = tickerGroups
        dfBase = dfBase.append(dfAppend,ignore_index=True)

    dfBase['Category'] = stripRegistered(dfBase)
    dfBase = replace(dfBase)
    dfBase.to_csv(root_directory_etfs+f'\TickerCategorization\{today}.csv',index=False)


### Testing Output ###
# print(nameETF("D:\TickerGroupings\MonthlyHoldings\holdings-daily-us-en-splg.xlsx"))
# print(tickerGroupings("D:\TickerGroupings\MonthlyHoldings\holdings-daily-us-en-splg.xlsx"))
# print(tickerSector("AAPL"))


