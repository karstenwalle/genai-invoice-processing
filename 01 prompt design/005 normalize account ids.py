import os
import pandas as pd

run_name = "Your run name here"



input_file = 'runs/' + run_name + '/004 Booking of the voucher/completed_invoices.csv'
accounts_file = 'context/accounts.csv'
output_file = 'runs/' + run_name + '/005 Normalize account numbers/completed_invoices.csv'

os.makedirs(os.path.dirname(output_file), exist_ok=True)

completed_invoices = pd.read_csv(input_file, encoding='ISO-8859-1')
accounts = pd.read_csv(accounts_file, encoding='ISO-8859-1')

account_mapping = dict(zip(accounts['number'], accounts['account_id']))

def replace_account(account):
    if isinstance(account, int) and 1000 <= account <= 9999:  # Check if it's a 4-digit code
        return account_mapping.get(account, account)  # Replace if match exists, otherwise keep original
    return account

completed_invoices['account'] = completed_invoices['account'].apply(replace_account)

completed_invoices.to_csv(output_file, index=False, encoding='utf-8')

print(f"Normalized file saved to '{output_file}'")
