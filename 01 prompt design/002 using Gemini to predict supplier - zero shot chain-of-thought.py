import os
import pandas as pd
import json
import google.generativeai as genai

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Your Google credentials as a JSON file here"

# Configure Gemini API
genai.configure()

# Load the supplier list
supplier_df = pd.read_csv('context/suppliers.csv', encoding='ISO-8859-1')

# Prepare supplier context for Gemini prompt
supplier_context = "\n".join([f"{row['Supplier name']}, {row['Supplier number']}, {row['Organization number']}" 
                              for _, row in supplier_df.iterrows()])

run_name = "Your run name here"

# Paths
input_folder = "runs/" + run_name + "/001 Output from OCR"
output_csv_path = "runs/" + run_name + "/002 Supplier prediction/result.csv"

# Initialize result DataFrame
result_df = pd.DataFrame(columns=["invoice_number", "supplier_name", "supplier_number", "organization_number"])

def extract_supplier_from_gemini(invoice_text):
    prompt = f"""
        Choose the correct supplier number from the supplier list based on invoice text.
        Let's solve this step by step.

        Step 1: Read the invoice text and extract all names that could be suppliers.
        Step 2: Remove any names that are our own company: [REDACTED] and [REDACTED].
        Step 3: Try to find a perfect match for either the supplier name or organization number in the supplier list.
        Step 4: If there are several possible matches or no clear match, return empty values.
        Step 5: All suppliers in the supplier list have supplier numbers, make sure you find and return the supplier number of the identified supplier.
        Step 6: If you find a perfect match, return the JSON below with the supplier name, supplier number, organization number and reasoning. 
        Step 7: Remove any text outside of the JSON.


        Supplier List:
        {supplier_context}

        Invoice Text:
        {invoice_text}


        {{
        "supplier_name": "...",
        "supplier_number": "...",
        "organization_number": "...",
        "reasoning": "Step-by-step explanation of how the result was determined or why it was left empty."
        }}
"""

    
    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)
    # print(response)

    try:
        # Extract JSON from the response
        json_text = response.candidates[0].content.parts[0].text

        # Remove markdown formatting if present
        if json_text.startswith("```json"):
            json_text = json_text[7:]
        if json_text.endswith("```"):
            json_text = json_text[:-3]

        # Parse JSON response
        supplier_data = json.loads(json_text.strip())
        return supplier_data

    except Exception as e:
        print(f"Error extracting supplier data: {e}")
        return None

for txt_file in os.listdir(input_folder):
    if txt_file.endswith(".txt"):
        file_path = os.path.join(input_folder, txt_file)

        with open(file_path, "r", encoding="utf-8") as file:
            invoice_text = file.read()
        
        # Extract supplier info using Gemini
        supplier_data = extract_supplier_from_gemini(invoice_text)
        
        if supplier_data:
            # print(supplier_data['reasoning'])
            new_row = pd.DataFrame([{
                "invoice_number": txt_file.replace(".txt", ""),
                "supplier_name": supplier_data.get("supplier_name", ""),
                "supplier_number": supplier_data.get("supplier_number", ""),
                "organization_number": supplier_data.get("organization_number", "")
            }])

            result_df = pd.concat([result_df, new_row], ignore_index=True)

# Save the result to CSV
result_df.to_csv(output_csv_path, index=False)

print(f"Results saved to {output_csv_path}")
