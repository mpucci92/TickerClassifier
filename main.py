import pandas as pd
import numpy as np
import requests
import zipfile as zipp
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
    :param extract_path: Path to unzip the zipp file contents to
    """
    req = requests.get(url_zip)
    print('Downloading Completed')
    zipfile = zipp.ZipFile(BytesIO(req.content))
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
    df = df.loc[:,['Name', 'Ticker', 'Sector','Weight']]
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

def replaceCategory(dataframe):
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

    oldLabels = ['Global Dow', 'S&P Aerospace & Defense', 'S&P 500 Fossil Fuel Reserves Free', 'Financial', 'Consumer Discretionary', 'S&P Semiconductor', 'SSGA Gender Diversity Index', 'S&P Insurance', 'S&P Retail', 'S&P New Economies Composite', 'S&P 400 Mid Cap Growth', 'Dow Jones Global Real Estate', 'S&P Dividend', 'S&P Software & Services', 'S&P Emerging Markets Small Cap', 'Consumer Staples', 'Dow Jones International Real Estate', 'S&P 600 Small Cap Value', 'Dow Jones REIT', 'SSGA US Large Cap Low Volatility Index', 'S&P Future Security', 'Portfolio S&P 500 High Dividend', 'Materials', 'S&P Telecom', 'MSCI EAFE StrategicFactors', 'MSCI ACWI Low Carbon Target', 'MSCI EAFE Fossil Fuel Reserves Free', 'S&P Global Infrastructure', 'Communication Services', 'S&P Transportation', 'S&P 500', 'Portfolio S&P 600 Small Cap', 'EURO STOXX 50', 'Portfolio S&P 1500 Composite Stock Market', 'S&P Pharmaceuticals', 'SSGA US Small Cap Low Volatility Index', 'S&P Global Natural Resources', 'MSCI Emerging Markets Fossil Fuel Reserves Free', 'S&P Bank', 'Portfolio S&P 400 Mid Cap', 'ICE Preferred Securities', 'Utilities', 'MSCI USA StrategicFactors', 'Portfolio Emerging Markets', 'S&P International Dividend', 'S&P Metals & Mining', 'Energy', 'S&P 600 Small Cap', 'Portfolio Developed World ex-US', 'Portfolio S&P 500 Growth', 'S&P Biotech', 'S&P Homebuilders', 'S&P North American Natural Resources', 'S&P China', 'Portfolio Europe', 'MSCI ACWI ex-US', 'S&P MIDCAP 400', 'S&P Clean Power', 'S&P 600 Small Cap Growth', 'S&P Intelligent Structures', 'S&P Smart Mobility', 'S&P Oil & Gas Equipment & Services', 'S&P Internet', 'Portfolio S&P 500 Value', 'MSCI World StrategicFactors', 'S&P 1500 Value Tilt', 'Russell 1000 Momentum Focus', 'Health Care', 'S&P 1500 Momentum Tilt', 'S&P Emerging Asia Pacific', 'S&P 400 Mid Cap Value', 'NYSE Technology', 'Technology', 'Portfolio S&P 500', 'Portfolio MSCI Global Stock Market', 'S&P Health Care Equipment', 'S&P 500 ESG', 'S&P Final Frontiers', 'Russell 1000 Yield Focus', 'S&P Global Dividend', 'Dow Jones Industrial Average', 'S&P Health Care Services', 'S&P Oil & Gas Exploration & Production', 'Real Estate', 'S&P Capital Markets', 'MSCI Emerging Markets StrategicFactors', 'S&P International Small Cap', 'S&P Emerging Markets Dividend', 'FactSet Innovative Technology', 'Industrial', 'S&P Regional Banking', 'Russell 1000 Low Volatility Focus']
    newLabels = ['Global Dow', 'Aerospace & Defense', 'Fossil Fuel Reserves', 'Financial', 'Consumer Discretionary', 'Semiconductor', 'Gender Diversity', 'Insurance', 'Retail', 'New Economies', 'S&P 400 Mid Cap Growth', 'Global Real Estate', 'Dividend', 'Software & Services', 'Emerging Markets Small Cap', 'Consumer Staples', 'International Real Estate', 'S&P 600 Small Cap Value', 'Dow Jones REIT', 'US Large Cap Low Volatility', 'Future Security', 'Portfolio S&P 500 High Dividend', 'Materials', 'Telecom', 'EAFE StrategicFactors', 'ACWI Low Carbon Target', 'EAFE Fossil Fuel Reserves', 'Global Infrastructure', 'Communication Services', 'Transportation', 'S&P 500', 'Portfolio S&P 600 Small Cap', 'EURO STOXX 50', 'Portfolio S&P 1500 Composite Stock Market', 'Pharmaceuticals', 'US Small Cap Low Volatility', 'Global Natural Resources', 'Emerging Markets Fossil Fuel Reserves', 'Bank', 'Portfolio S&P 400 Mid Cap', 'Preferred Securities', 'Utilities', 'USA StrategicFactors', 'Portfolio Emerging Markets', 'International Dividend', 'Metals & Mining', 'Energy', 'S&P 600 Small Cap', 'Portfolio Developed World ex-US', 'Portfolio S&P 500 Growth', 'Biotech', 'Homebuilders', 'North American Natural Resources', 'China', 'Portfolio Europe', 'ACWI ex-US', 'S&P MIDCAP 400', 'Clean Power', 'S&P 600 Small Cap Growth', 'Intelligent Structures', 'Smart Mobility', 'Oil & Gas Equipment & Services', 'Internet', 'Portfolio S&P 500 Value', 'World StrategicFactors', 'S&P 1500 Value Tilt', 'Russell 1000 Momentum Focus', 'Health Care', 'S&P 1500 Momentum Tilt', 'Emerging Asia Pacific', 'S&P 400 Mid Cap Value', 'NYSE Technology', 'Technology', 'Portfolio S&P 500', 'Portfolio MSCI Global Stock Market', 'Health Care Equipment', ' ESG', 'Final Frontiers', 'Russell 1000 Yield Focus', 'S&P Global Dividend', 'Dow Jones Industrial Average', 'Health Care Services', 'Oil & Gas Exploration & Production', 'Real Estate', 'Capital Markets', 'Emerging Markets StrategicFactors', 'International Small Cap', 'Emerging Markets Dividend', 'Innovative Technology', 'Industrial', 'Regional Banking', 'Russell 1000 Low Volatility Focus']

    res = dict(zip(oldLabels, newLabels))
    dataframe = dataframe.replace({'Category': res})

    return dataframe

if __name__ == '__main__':
    url = "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/us-spdrs-allholdings-monthly.zip"
    root_directory_etfs = os.path.dirname(os.path.realpath(__file__))
    today = date.today()
    df_cols = ['Name', 'Ticker' , 'Sector', 'Category','Weight']
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
    dfBase = replaceCategory(dfBase)

    dfBase.to_csv(root_directory_etfs+f'\TickerCategorization\{today}.csv',index=False)


### Testing Output ###
# print(nameETF("D:\TickerGroupings\MonthlyHoldings\holdings-daily-us-en-splg.xlsx"))
# print(tickerGroupings("D:\TickerGroupings\MonthlyHoldings\holdings-daily-us-en-splg.xlsx"))
# print(tickerSector("AAPL"))


