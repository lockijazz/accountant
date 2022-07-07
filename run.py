import pandas as pd
import numpy as np
import os
import datetime as dt
from dateutil import parser
import yaml
import re

# Reads csvs from Desktop
# Normalizes, categorizes, sorts
# Writes output to Desktop/output.csv

START_VENMO_BALANCE = 0
END_VENMO_BALANCE = 0

with open("./config.yaml", "r") as cf:
    config = yaml.safe_load(cf)

# load and parse relevant csvs in Desktop/ folder into dfs
desktop_path = "/Users/tomgong/Desktop/"
dfs = []
for file_name in os.listdir(desktop_path):
    if file_name[-4:] == ".csv":
        file_path = os.path.join(desktop_path, file_name)
        
        if file_name == "output.csv":
            continue

        if file_name[:5] == "venmo":
            # venmo statement
            df = pd.read_csv(file_path, skiprows=2)
            df["Source"] = "Venmo"
            START_VENMO_BALANCE = df["Beginning Balance"].unique()[0]
            END_VENMO_BALANCE = df["Ending Balance"].unique()[-1]
        elif file_name == "stmt.csv":
            # bofa checking acct statement
            df = pd.read_csv(file_path, skiprows=6)
            df["Source"] = "BofA Checking"
            df.drop(columns=["Running Bal."], inplace=True)
            dfs.append(df)
        elif file_name[:5] == "Chase":
            # chase cc statement
            df = pd.read_csv(file_path)
            df["Source"] = "Chase CC"
            df.drop(columns=["Post Date", "Category", "Type", "Memo"], inplace=True)
            df.rename(columns={"Transaction Date": "Date"}, inplace=True)
            dfs.append(df)
        elif file_name[:7] == "History":
            # alliant savings statement
            df = pd.read_csv(file_path)
            df["Source"] = "Alliant Savings"
            df.drop(columns=["Balance"], inplace=True)
            # df["Date"] = df["Date"].apply(lambda s: dt.datetime.strptime(s, "%M/%d/%Y").date())
            dfs.append(df)
        else:
            # bofa cc statement
            df = pd.read_csv(file_path)
            df["Source"] = "BofA CC"
            df.drop(columns=["Reference Number", "Address"], inplace=True)
            df.rename(columns={"Posted Date": "Date", "Payee": "Description"}, inplace=True)
            dfs.append(df)

# consolidate normalized transactions
all_transactions = pd.concat(dfs, ignore_index=True)

# filter out transactions that aren't from the past month
all_transactions["Date"] = all_transactions["Date"].apply(lambda s: parser.parse(s))
month_num = dt.date.today().month
prev_month_num = 12 if month_num == 1 else month_num-1
start_dt = dt.datetime(year=2022, month=prev_month_num, day=1)
end_dt = dt.datetime(year=2022, month=month_num, day=1)
all_transactions = all_transactions.loc[
    (all_transactions["Date"] >= start_dt)
    & (all_transactions["Date"] < end_dt)
]

# categorize based on regex rules in config
regex_rules_to_category = { rr:k for k,v in config["category_regex_rules"].items() for rr in v}
def categorize(description: str) -> str:
    for regex_rule, category in regex_rules_to_category.items():
        if re.search(regex_rule, description) is not None:
            return category
    return "uncategorized"
        
all_transactions["Category"] = all_transactions["Description"].apply(lambda d: categorize(d))

# reorder columns and sort
all_transactions = all_transactions[["Date", "Description", "Amount", "Category", "Source"]]
all_transactions.sort_values(by=["Category", "Description"], inplace=True, ignore_index=True)

# write output to csv
output_path = os.path.join(desktop_path, "output.csv")
all_transactions.to_csv(output_path)

print(f"START_VENMO_BALANCE: {START_VENMO_BALANCE}")
print(f"END_VENMO_BALANCE: {END_VENMO_BALANCE}")

