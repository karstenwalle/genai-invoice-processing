import pandas as pd

run_name = "Your run name here"



completed_invoices = pd.read_csv('runs/' + run_name + '/005 Normalize account numbers/completed_invoices.csv', encoding='ISO-8859-1')
supplier_postings = pd.read_csv('runs/' + run_name + '/006 Is it correct/supplier_postings.csv', encoding='ISO-8859-1')

grouped_completed_invoices = completed_invoices.groupby(
    ['voucher', 'account', 'department', 'vatType'],
    as_index=False
).agg({
    'amount': 'sum',          # Aggregate amount by sum
    'description': 'first'    # Take the first value for description
})

grouped_supplier_postings = supplier_postings.groupby(
    ['voucher', 'account', 'department', 'vatType'],
    as_index=False
).agg({
    'amount': 'sum',
    'description': 'first'
})

grouped_completed_invoices.to_csv('runs/' + run_name + '/006 Is it correct/output-grouped_completed_invoices.csv', index=False, encoding='utf-8')
grouped_supplier_postings.to_csv('runs/' + run_name + '/006 Is it correct/output-grouped_supplier_postings.csv', index=False, encoding='utf-8')

print("Grouped files saved as 'grouped_completed_invoices.csv' and 'grouped_supplier_postings.csv'")
