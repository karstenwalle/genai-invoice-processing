import os
import pandas as pd
import json
import google.generativeai as genai

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Your Google credentials as a JSON file here"

genai.configure()

run_name = "Your run name here"




result_df = pd.read_csv('runs/' + run_name + '/002 Supplier prediction/result.csv', encoding='ISO-8859-1')
double_checked_results_df = pd.read_csv('runs/' + run_name + '/003 Supplier sense-check/double_checked_results.csv', encoding='ISO-8859-1')

suppliers_with_id = pd.read_csv('context/suppliers_with_id.csv', encoding='ISO-8859-1')
supplier_postings = pd.read_csv('context/supplier_postings_2022-01-01_-_2022-08-31.csv', encoding='ISO-8859-1')
accounts_df = pd.read_csv('context/accounts.csv', encoding='ISO-8859-1')
departments_df = pd.read_csv('context/departments.csv', encoding='ISO-8859-1')
vat_codes_df = pd.read_csv('context/vat_codes.csv', encoding='ISO-8859-1')

# Filter only invoices marked as "correct" in double_checked_results
confirmed_invoices = double_checked_results_df[
    double_checked_results_df['status'] == 'correct'
]

# Keep only rows where supplier matches and data is marked as "correct"
filtered_results = result_df.merge(
    confirmed_invoices[['invoice_number']],
    on=['invoice_number'],
    how='inner'
)

merged_df = filtered_results.merge(
    suppliers_with_id,
    left_on='supplier_number',
    right_on='supplierNumber',
    how='inner'
)
print(merged_df)
print(merged_df.head())


def extract_invoice_details(invoice_text, supplier_data, historical_postings):
    prompt = f"""
        You are a Norwegian accountant following Norwegian accounting standards. Book this invoice by giving me the posting(s) in the JSON format below.
        - use the department ID as the department identifier
        - The invoices has either net and gross or just net values. Net = without VAT.
        - Book them net, but with the correct vatType. Do not create a separate line for VAT.
        - Do not create a seperate line for the payable to the supplier. That's ignored in this excercise.
        - If there is supplier context, please adhere to it when booking.
        - Negative amounts are for credit notes. Positive amounts are for costs.
       

        ### **Historical Context:**  
        {json.dumps(historical_postings, indent=2)}

        ### **Supplier:**  
        {json.dumps(supplier_data, indent=2)}

        ### **Accounts:**  
        {json.dumps(accounts_df.to_dict(orient='records'), indent=2)}

        ### **Departments:**  
        {json.dumps(departments_df.to_dict(orient='records'), indent=2)}

        ### **VAT Codes:**  
        {json.dumps(vat_codes_df.to_dict(orient='records'), indent=2)}

        ### **Invoice Text:**  
        {invoice_text}

        ### **Return the result as RAW JSON:**  
        - Do NOT format the JSON in markdown.  
        - Do NOT use backticks.  
        - Return raw JSON directly, like this:  
        [
            {{
                "date": "",
                "description": "",
                "account": "",
                "customer": "",
                "supplier": "",
                "department": "",
                "vatType": "",
                "amount": ""
            }}
        ]
        """

    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)

    if not response.candidates:
        print("No response from Gemini.")
        return None
    
    # Extract text response
    json_text = response.candidates[0].content.parts[0].text.strip()

    # Remove markdown formatting if present
    if json_text.startswith("```json"):
        json_text = json_text[7:]
    if json_text.endswith("```"):
        json_text = json_text[:-3]

    # Additional cleanup (in case of trailing spaces or newlines)
    json_text = json_text.strip()

    try:
        result = json.loads(json_text)
        return result
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}\nResponse:\n{json_text}")
        return None

# Process each invoice
input_folder = "runs/" + run_name + "/001 Output from OCR"
output_csv_path = "runs/" + run_name + "/004 Booking of the voucher/completed_invoices.csv"
result_output = []

for _, row in merged_df.iterrows():
    voucher_id = row['invoice_number']
    supplier_id = row['id']
    supplier_data = row.to_dict()
    
    # Get last 4 postings for this supplier (moved to inside the loop)
    supplier_postings_subset = supplier_postings[
        supplier_postings['supplier'] == supplier_id
    ].tail(4).to_dict(orient='records')
    
    # Read invoice text
    file_path = os.path.join(input_folder, f"{voucher_id}.txt")
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            invoice_text = file.read()
        
        # Get prediction from Gemini
        invoice_details = extract_invoice_details(invoice_text, supplier_data, supplier_postings_subset)

        if invoice_details:
            # Add voucher ID separately after Gemini response
            for item in invoice_details:
                item['voucher'] = voucher_id
                result_output.append(item)

# Step 6: Convert to DataFrame and save to CSV
result_df = pd.DataFrame(result_output)
result_df.to_csv(output_csv_path, index=False, encoding='utf-8')

print(f"Results saved to {output_csv_path}")
