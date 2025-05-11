import os
import pandas as pd
import json
import google.generativeai as genai

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Your Google credentials as a JSON file here"

# Configure Gemini API
genai.configure()

run_name = "Your run name here"

result_df = pd.read_csv('runs/' + run_name + '/002 Supplier prediction/result.csv', encoding='ISO-8859-1')

suppliers_with_id = pd.read_csv('context/suppliers_with_id.csv', encoding='ISO-8859-1')
supplier_postings = pd.read_csv('context/supplier_postings_2022-01-01_-_2022-08-31.csv', encoding='ISO-8859-1')
accounts_df = pd.read_csv('context/accounts.csv', encoding='ISO-8859-1')
departments_df = pd.read_csv('context/departments.csv', encoding='ISO-8859-1')
vat_codes_df = pd.read_csv('context/vat_codes.csv', encoding='ISO-8859-1')

filtered_postings = pd.read_csv('context/supplier_postings_2022-01-01_-_2022-08-31/filtered_supplier_postings.csv')

merged_df = result_df.merge(
    suppliers_with_id,
    left_on='supplier_number',
    right_on='supplierNumber',
    how='inner'
)


def get_first_voucher(supplier_id, df_of_vouchers, vat_codes_df):
    # Filter for the supplier
    df_filtered = df_of_vouchers[df_of_vouchers['supplier'] == supplier_id]

    if df_filtered.empty:
        return False

    # Get the first voucher (by date)
    first_voucher_row = df_filtered.sort_values(by='date').iloc[0]
    voucher_id = first_voucher_row['voucher']
    voucher_rows = df_filtered[df_filtered['voucher'] == voucher_id]

    # Prepare VAT code mapping (convert % strings to float)
    vat_codes_df = vat_codes_df.copy()
    vat_codes_df['VAT rate'] = vat_codes_df['VAT rate'].str.replace('%', '').astype(float) / 100

    # Merge in VAT rates
    merged = pd.merge(voucher_rows, vat_codes_df, how='left', left_on='vatType', right_on='VAT code')

    # Calculate VAT for each row and total payable gross amount
    merged['vat_amount'] = merged['amount'] * merged['VAT rate']
    payable_gross_amount = (merged['amount'] + merged['vat_amount']).sum()

    # Build voucher structure
    voucher_data = {
        "date": first_voucher_row["date"],
        "general description": first_voucher_row["description"],
        "payable_gross_amount": round(payable_gross_amount, 2),
        "vat_lines": []
    }

    # Fill VAT lines
    for _, row in merged.iterrows():
        voucher_data["vat_lines"].append({
            "vatType": row["vatType"],
            "net_amount": row["amount"]
        })

    return voucher_id, voucher_data



def load_voucher_text(voucher_id):
    folder_path = 'context/supplier_postings_2022-01-01_-_2022-08-31/ocr/'
    file_path = os.path.join(folder_path, f'{voucher_id}.txt')

    if not os.path.exists(file_path):
        return None  # or raise FileNotFoundError

    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()
    




def extract_invoice_details(invoice_text, supplier_data, has_old_voucher, old_voucher, old_voucher_return):

    if has_old_voucher:
        prompt = f"""
            Please find the sum payable and group the attached invoice by VAT type.
            - The payable amount should be a gross amount, i.e. should include VAT
            - The sum per VAT type should be net, i.e. should not include VAT
            - Only use the attached VAT codes.
            - If there is supplier context, please adhere to it
            - Negative amounts are for credit notes. Positive amounts are for costs.
            - If the invoice mentions "credit note", multiply the amounts by -1. (100 becomes -100).
            - If the invoice is from outside of Norway, it's import and the VAT type should be 22 for food items and 21 for non-food items (0% VAT).

            ### **Supplier:**  
            {json.dumps(supplier_data, indent=2)}

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
                    "general description": "",
                    "payable_gross_amount": "",
                    "vat_lines": 
                        [
                            {{
                                "vatType": "",
                                "net_amount": ""
                            }}
                        ]
                }}
            ]


            Below is an old invoice and the correct return value for it. Use it to understand how to solve the task above.

            ### **The old invoice:**
            {old_voucher}
            
            ### The return value for the old invoice:
            {old_voucher_return}
        """
    else:
        prompt = f"""
            Please find the sum payable and group the attached invoice by VAT type.
            - The payable amount should be a gross amount, i.e. should include VAT
            - The sum per VAT type should be net, i.e. should not include VAT
            - Only use the attached VAT codes.
            - If there is supplier context, please adhere to it
            - Negative amounts are for credit notes. Positive amounts are for costs.

            ### **Supplier:**  
            {json.dumps(supplier_data, indent=2)}

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
                    "general description": "",
                    "payable_gross_amount": "",
                    "vat_lines": 
                        [
                            {{
                                "vatType": "",
                                "net_amount": ""
                            }}
                        ]
                }}
            ]
        """


    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(
        prompt,
        generation_config={"temperature": 1}
    )

    if not response.candidates:
        print("No response from Gemini.")
        return []

    json_text = response.candidates[0].content.parts[0].text.strip()

    if json_text.startswith("```json"):
        json_text = json_text[7:]
    if json_text.endswith("```"):
        json_text = json_text[:-3]

    json_text = json_text.strip()

    try:
        result = json.loads(json_text)
        if isinstance(result, list):
            return [item for item in result if isinstance(item, dict)]
        else:
            print(f"Unexpected result format (not a list): {result}")
            return []
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}\nResponse:\n{json_text}")
        return []




input_folder = f"runs/{run_name}/001 Output from OCR"
output_csv_path = f"runs/{run_name}/004 Booking of the voucher/vat_lines.csv"
result_output = []

for _, row in merged_df.iterrows():
    voucher_id = row['invoice_number']
    supplier_id = row['id']
    supplier_data = row.to_dict()

    voucher_result = get_first_voucher(supplier_id, filtered_postings, vat_codes_df)

    if not voucher_result:
        print("No voucher found.")
        old_voucher_id = 0
    else:
        old_voucher_id, old_voucher_data = voucher_result
        old_voucher = load_voucher_text(old_voucher_id)

    # Read invoice text
    file_path = os.path.join(input_folder, f"{voucher_id}.txt")
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            invoice_text = file.read()

        if not voucher_result:
            invoice_details = extract_invoice_details(invoice_text, supplier_data, False, "", "")
        else:
            invoice_details = extract_invoice_details(invoice_text, supplier_data, True, old_voucher, old_voucher_data)

        for item in invoice_details:
            item['voucher'] = voucher_id
            item['old_voucher'] = old_voucher_id
            result_output.append(item)
    else:
        print(f"⚠️ Invoice text file not found for voucher {voucher_id}")

# Flatten vat_lines and save to CSV
flattened_result = []

for item in result_output:
    voucher = item.get('voucher')
    old_voucher = item.get('old_voucher')
    date = item.get('date')
    general_description = item.get('general description')
    payable_gross_amount = item.get('payable_gross_amount')
    vat_lines = item.get('vat_lines', [])

    for vat_line in vat_lines:
        if isinstance(vat_line, dict):
            flattened_result.append({
                "voucher": voucher,
                "date": date,
                "general description": general_description,
                "payable_gross_amount": payable_gross_amount,
                "vatType": vat_line.get("vatType"),
                "net_amount": vat_line.get("net_amount"),
                "old_voucher_id": old_voucher
            })

# Save to CSV
flattened_df = pd.DataFrame(flattened_result)
flattened_df.to_csv(output_csv_path, index=False, encoding='utf-8')

print(f"flattened results saved to {output_csv_path}")
