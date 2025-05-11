import os
import pandas as pd
import google.generativeai as genai
import json, ast, re

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "project-apai-c86fa58c275f.json"

# Configure Gemini API
genai.configure()

run_name = "2025-04-21 VAT line prediction - supplier specific one-shot"

# ✅ Load existing data
# result_df = pd.read_csv('runs/' + run_name + '/002 Supplier prediction/result.csv', encoding='ISO-8859-1')
vat_line_predictions = pd.read_csv('runs/' + run_name + '/004 Booking of the voucher/finished vat_lines.csv')


# ✅ Import context files
suppliers_with_id = pd.read_csv('context/suppliers_with_id.csv', encoding='ISO-8859-1')
supplier_postings = pd.read_csv('context/supplier_postings_2022-01-01_-_2022-08-31.csv', encoding='ISO-8859-1')
accounts_df = pd.read_csv('context/accounts.csv', encoding='ISO-8859-1')
departments_df = pd.read_csv('context/departments.csv', encoding='ISO-8859-1')
vat_codes_df = pd.read_csv('context/vat_codes.csv', encoding='ISO-8859-1')

filtered_postings = pd.read_csv('context/supplier_postings_2022-01-01_-_2022-08-31/filtered_supplier_postings.csv')

# ✅ Step 3: Add supplier ID by merging with suppliers_with_id
# merged_df = result_df.merge(
#     suppliers_with_id,
#     left_on='supplier_number',
#     right_on='supplierNumber',
#     how='inner'
# )


def construct_vat_lines(voucher_id: int, vat_lines_preds):
    # keep only the voucher’s rows and the two relevant columns
    vat_lines = vat_lines_preds.loc[
        vat_lines_preds["voucher"] == voucher_id, ["vatType", "net_amount"]
    ]

    if vat_lines.empty:
        return []

    # build the list of dicts
    return [
        {
            "vat_lines": [{
                "vatType": row["vatType"],
                "net_amount": row["net_amount"],
                "department": "",
                "account": ""
            }]
            
        }
        for _, row in vat_lines.iterrows()
    ]


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
    # payable_gross_amount = (merged['amount'] + merged['vat_amount']).sum()

    # Build voucher structure
    voucher_data = {
        "vat_lines": []
    }

    # Fill VAT lines
    for _, row in merged.iterrows():
        voucher_data["vat_lines"].append({
            "vatType": row["vatType"],
            "net_amount": row["amount"],
            "account": "",
            "department": ""
        })

    return voucher_id, [voucher_data]




def get_first_voucher_answer(supplier_id, df_of_vouchers, vat_codes_df):
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

        "vat_lines": []
    }

    # Fill VAT lines
    for _, row in merged.iterrows():
        voucher_data["vat_lines"].append({
            "vatType": row["vatType"],
            "net_amount": row["amount"],
            "account": row["account"],
            "department": row["department"]
        })

    return voucher_id, [voucher_data]


def load_voucher_text(voucher_id):
    folder_path = 'context/supplier_postings_2022-01-01_-_2022-08-31/ocr/'
    file_path = os.path.join(folder_path, f'{voucher_id}.txt')

    if not os.path.exists(file_path):
        return None  # or raise FileNotFoundError

    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()
    




# ✅ Step 4: Define prompt function
def extract_invoice_details(predicted_vat_lines, invoice_text, supplier_data, example_voucher_question, example_voucher_answer, example_voucher_invoice, has_old_voucher):

    if has_old_voucher:
        prompt = f"""
                You are a Norwegian accountant following Norwegian accounting standards.
                - I have an invoice and the VAT lines for that invoice. For each VAT lines, I want you to:
                    - Pick the correct account code from the chart of accounts.
                    - Pick the correct department from the department list.
                - Keep the VAT lines. They're correct.
                - If there is supplier context, please adhere to it
                - Always use double quotes. No single quotes.

                ### **Supplier:**  
                {json.dumps(supplier_data, indent=2)}

                ### **Chart of accounts:**
                {accounts_df}

                ### **Department list:**
                {departments_df}

                ### **Invoice Text:**  
                {invoice_text}

                ### **Return the result as RAW JSON:**  
                - Do NOT format the JSON in markdown.  
                - Do NOT use backticks.  
                - Keep the VAT lines, and add the accounting code and department.
                - Return raw JSON directly, like this: 
                {predicted_vat_lines}


                Below is an old invoice and the correct return value for it. Use it to understand how to solve the task above.

                ### **The old invoice:**
                {example_voucher_invoice}

                ### **The VAT lines for the old invoice**:
                {example_voucher_question}
                
                ### The return value for the old invoice:
                {example_voucher_answer}
            """
    else:
        prompt = f"""
                You are a Norwegian accountant following Norwegian accounting standards.
                - I have an invoice and the VAT lines for that invoice. For each VAT lines, I want you to:
                    - Pick the correct account code from the chart of accounts.
                    - Pick the correct department from the department list.
                - Keep the VAT lines. They're correct.
                - If there is supplier context, please adhere to it
                - Always use double quotes. No single quotes.

                ### **Supplier:**  
                {json.dumps(supplier_data, indent=2)}

                ### **Chart of accounts:**
                {accounts_df}

                ### **Department list:**
                {departments_df}

                ### **Invoice Text:**  
                {invoice_text}

                ### **Return the result as RAW JSON:**  
                - Do NOT format the JSON in markdown.  
                - Do NOT use backticks.  
                - Keep the VAT lines, and add the accounting code and department.
                - Return raw JSON directly, like this: 
                {predicted_vat_lines}
            """
        

    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(
        prompt,
        generation_config={"temperature": 1}
    )

    if not response.candidates:
        print("⚠️ No response from Gemini.")
        return []

    json_text = response.candidates[0].content.parts[0].text.strip()

    m = re.search(r"```(?:json)?\s*(.*?)\s*```", json_text, flags=re.S)
    if m:
        json_text = m.group(1).strip()

    try:
        parsed = json.loads(json_text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(json_text)
        except Exception as exc:
            print(f"❌ Could not parse as JSON or Python literal: {exc}\n{json_text}")
            return []                                # ← nothing usable

    if isinstance(parsed, dict):
        parsed = [parsed]
    elif not isinstance(parsed, list):
        print(f"⚠️ Unexpected result type ({type(parsed).__name__}): {parsed}")
        return []                                   # ← nothing usable

    result = [item for item in parsed if isinstance(item, dict)]
    return result          




# ✅ Step 5: Process each invoice
input_folder = f"runs/{run_name}/001 Output from OCR"
output_csv_path = f"runs/{run_name}/004 Booking of the voucher/account_department_lines.csv"
result_output = []
# iterate once for every distinct voucher number
for voucher_id in vat_line_predictions["voucher"].unique():
    # all prediction-rows that belong to this voucher
    voucher_rows = vat_line_predictions.loc[vat_line_predictions["voucher"] == voucher_id]

    # assume every row for a voucher has the same supplier_id → take the first
    supplier_id = voucher_rows["supplier_id"].iat[0]

    # lookup the supplier record
    supplier_data = (
        suppliers_with_id.loc[suppliers_with_id["id"] == supplier_id]
        .squeeze()              # convert 1-row DataFrame → Series
        .to_dict()
    )

    # read the OCR/plain-text file for this voucher
    file_path = os.path.join(input_folder, f"{voucher_id}.txt")
    if not os.path.exists(file_path):
        print(f"⚠️ Invoice text file not found for voucher {voucher_id}")
        continue

    with open(file_path, "r", encoding="utf-8") as fh:
        invoice_text = fh.read()

    # fetch example voucher data for few-shot prompting
    get_first_voucher_return        = get_first_voucher(supplier_id, filtered_postings, vat_codes_df)
    get_first_voucher_answer_return = get_first_voucher_answer(supplier_id, filtered_postings, vat_codes_df)

    if not get_first_voucher_return:
        print("No previous voucher found.")
        old_voucher_example_question = example_voucher_answer = example_voucher_invoice = "N/A"
        has_example = False
    else:
        old_voucher_id, old_voucher_example_question = get_first_voucher_return
        _,             example_voucher_answer        = get_first_voucher_answer_return
        example_voucher_invoice = load_voucher_text(old_voucher_id)
        has_example = True

    # build the VAT-line dictionaries for this voucher
    constructed_vat_lines = construct_vat_lines(voucher_id, vat_line_predictions)

    # extract structured invoice details
    invoice_details = extract_invoice_details(
        constructed_vat_lines,
        invoice_text,
        supplier_data,
        old_voucher_example_question,
        example_voucher_answer,
        example_voucher_invoice,
        has_example,
    )

    # attach voucher id and accumulate
    for item in invoice_details:
        item["voucher"] = voucher_id
        result_output.append(item)

# ✅ Step 6: Flatten vat_lines and save to CSV
flattened_result = []

for item in result_output:
    voucher = item.get('voucher')
    vat_lines = item.get('vat_lines', [])

    for vat_line in vat_lines:
        if isinstance(vat_line, dict):
            flattened_result.append({
                "voucher": voucher,
                "vatType": vat_line.get("vatType"),
                "net_amount": vat_line.get("net_amount"),
                "department": vat_line.get("department"),
                "account": vat_line.get("account")
            })

# Save to CSV
flattened_df = pd.DataFrame(flattened_result)
flattened_df.to_csv(output_csv_path, index=False, encoding='utf-8')

print(f"✅ Flattened results saved to {output_csv_path}")
