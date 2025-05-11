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

# Add supplier ID by merging with suppliers_with_id
merged_df = result_df.merge(
    suppliers_with_id,
    left_on='supplier_number',
    right_on='supplierNumber',
    how='inner'
)

def extract_invoice_details(invoice_text, supplier_data, historical_postings):
    prompt = f"""
        Please find the sum payable and group the attached invoice by VAT type by following these steps:
        1. Find the gross payable amount. It's usually the largest of the numbers.
        2. Find all VAT types, and sum net amount for each VAT type.
        3. Take the net sums for each VAT type and add the VAT to get the gross amount for each.
        4. Check if the sum of the gross amounts is equal to the gross payable amount.
        5. If the sums are equal, great job! If not, please revert to step 1 and give it another try.
 
        - Use . as decimal seperator and no group separator. (Ex: 12473.47)
        - Only use the attached VAT codes.
        - Negative amounts are for credit notes. Positive amounts are for costs.

        ### **Supplier:**  
        {json.dumps(supplier_data, indent=2)}

        ### **VAT Codes:**  
        {json.dumps(vat_codes_df.to_dict(orient='records'), indent=2)}

        ### **Invoice Text:**  
        {invoice_text}

        Please reason through the excercise using the steps above and return the below format.
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

        

        ### **Example of return data:**
        [
            {{
                "date": "2025-02-01",
                "general description": "Rent for Idrettsveien 2A, Oslo",
                "payable_gross_amount": "100.4",
                "vat_lines": 
                    [
                        {{
                            "vatType": "1",
                            "net_amount": "80.32"
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

    # Get last 4 postings for this supplier
    supplier_postings_subset = supplier_postings[
        supplier_postings['supplier'] == supplier_id
    ].tail(4).to_dict(orient='records')

    # Read invoice text
    file_path = os.path.join(input_folder, f"{voucher_id}.txt")
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            invoice_text = file.read()

        invoice_details = extract_invoice_details(invoice_text, supplier_data, supplier_postings_subset)

        for item in invoice_details:
            item['voucher'] = voucher_id
            result_output.append(item)
    else:
        print(f"⚠️ Invoice text file not found for voucher {voucher_id}")

# Flatten vat_lines and save to CSV
flattened_result = []

for item in result_output:
    voucher = item.get('voucher')
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
                "net_amount": vat_line.get("net_amount")
            })

# Save to CSV
flattened_df = pd.DataFrame(flattened_result)
flattened_df.to_csv(output_csv_path, index=False, encoding='utf-8')

print(f"flattened results saved to {output_csv_path}")
