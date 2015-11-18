import requests,zipfile,StringIO,os

#create directory of not exists
if not os.path.exists('./data'):
   os.makedirs('./data')

source =requests.get("https://resources.lendingclub.com/LoanStats3d.csv.zip")
stringio = StringIO.StringIO(source.content)
unzipped = zipfile.ZipFile(stringio)
import pandas as pd
clean_csv = pd.read_csv(unzipped.open('LoanStats3d.csv'),skiprows=1,skipfooter=2,engine='python')
stored_csv = clean_csv.to_csv('./data/stored_csv.csv')
