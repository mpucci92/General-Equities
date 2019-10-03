
from pathlib import Path
import pandas as pd
import numpy as np
import sys, os

import tqdm

DIR = Path("/media/zquantz/0236e42e-4d80-45cf-b1f7-f12ee259facd/StockAlgoData/")

anchor = pd.read_csv(DIR/"PriceData/IAK.csv", parse_dates=["DATE"]).sort_values("DATE").reset_index(drop=True)
anchor = anchor[['DATE', 'ADJ_OPEN', 'ADJ_HIGH', 'ADJ_LOW', 'ADJ_CLOSE', 'ADJ_VOLUME', 'EX_DIVIDEND', 'SPLIT_RATIO']]
anchor.columns = "date,open,high,low,close,volume,dividend,split".split(",")
anchor = anchor[anchor.date.dt.year > 2000]

for ticker in tqdm.tqdm(os.listdir(DIR/"PriceData")):
    
    df = pd.read_csv(DIR/("PriceData/{}".format(ticker)), parse_dates=["DATE"]).sort_values("DATE").reset_index(drop=True)
    df.ADJ_VOLUME.replace('Infinity', 0, inplace=True)
    df['ADJ_VOLUME'] = df[['ADJ_VOLUME', 'VOLUME']].max(axis=1)
    df = df[['DATE', 'ADJ_OPEN', 'ADJ_HIGH', 'ADJ_LOW', 'ADJ_CLOSE', 'ADJ_VOLUME', 'EX_DIVIDEND', 'SPLIT_RATIO']]
    df.columns = "date,open,high,low,close,volume,dividend,split".split(",")
    
    if df.shape[0] == 0:
        continue
    
    df = df[df.date.dt.year > 2000]
    tmp = anchor[anchor.date >= df.date.min()][['date']]
    tmp['filler'] = 0
    
    df = tmp.merge(df, how='outer', on='date', sort=True).dropna().drop('filler', axis=1)
    tmp.drop('filler', inplace=True, axis=1)
    df['filler'] = 0
    
    df = df.merge(tmp, how='outer', on='date', sort=True)
    
    if df.isnull().sum().max() > 30:
        continue
        
    df.volume.fillna(0, inplace=True)
    df.fillna(method='ffill', inplace=True)
    df.drop('filler', inplace=True, axis=1)
    
    if df.shape[0] < int((df.date.max() - df.date.min()).days * 0.68):
        continue
        
    if df.shape[0] == 0:
        continue
    
    df.to_csv(DIR/("ZiplinePriceData/{}".format(ticker)), index=False)