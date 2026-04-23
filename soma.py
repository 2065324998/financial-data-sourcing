import requests as r
import pandas as pd
from os.path import exists
import json

def get_url(url:str, security_cusip:str):
    file = 'data/raw_soma/'+url.replace('/','_')+'.json'

    use_cache = True

    if exists(file) and use_cache:
        with open(file, 'r') as f:
            file_contents = f.read()

            if len(file_contents) == 0:
                return None, 0

            holdings = json.loads(file_contents)
            return holdings, len(holdings['soma']['holdings'])

    response = r.get(url)
    if response.status_code != 200:
        print("Issue with "+ security_cusip +". "+ response.text)
        return None, 0

    holdings = response.json()
    
    with open(file, 'w') as f:
        f.write(response.text)

    return holdings, len(holdings)

def get_data_from_soma_api(security_cusip):
    url = f"https://markets.newyorkfed.org/api/soma/tsy/get/cusip/{security_cusip}.json"
    
    holdings = get_url(url, security_cusip) 
    
    return holdings

def get_soma_holdings(security_cusip:str, only_include_changes:bool=True, is_tips:bool=False):
    holdings, len_holdings = get_data_from_soma_api(security_cusip)

    if len_holdings == 0 or len(holdings['soma']['holdings']) == 0:
        return None, 0

    soma = get_dataframe(holdings)

    if soma is None:
        return None, 0

    #Calculate the diff in the par value
    soma['parValueDiff'] = soma['parValue'].diff()
    #Setting the diff on the first row to the initial value
    soma.at[0, 'parValueDiff'] = soma.at[0, 'parValue']

    soma['changeFromPriorWeek'] = soma['parValueDiff']

    if only_include_changes:
        soma = soma[soma['changeFromPriorWeek'] != 0.0]

    return soma, len(soma)

def get_dataframe(holdings):
    soma = pd.DataFrame.from_records(holdings["soma"]["holdings"])

    if len(soma.columns) == 0:
        return None
    # soma['changeFromPriorWeek'] = soma['changeFromPriorWeek'].astype(float)
    # soma['parValue'] = soma['parValue'].astype(float)
    soma['changeFromPriorWeek'] = pd.to_numeric(soma['changeFromPriorWeek'], errors='coerce')
    soma['parValue'] = pd.to_numeric(soma['parValue'], errors='coerce')

    soma['changeFromPriorYear'] = pd.to_numeric(soma['changeFromPriorYear'], errors='coerce')

    return soma

def check_errors_in_soma(cusip):
    from reconcile_transactions import get_soma_holdings
    soma, num_holdings = get_soma_holdings(cusip, False)
    
    calc_par_value = 0
    
    for i in range(0, num_holdings):
        row = soma.iloc[i,:]
        par_value = row['parValue']
        change_from_prior_week = row['changeFromPriorWeek']
        
        calc_par_value += change_from_prior_week
        if calc_par_value != par_value:
            print(f"Error: {soma[:i+1]}")
            return soma
        


if __name__ == "__main__":
    df, len_df = get_soma_holdings("912810FH6", is_tips=True)
    print(df)
