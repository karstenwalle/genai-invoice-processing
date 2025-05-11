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
    - Ignore [REDACTED]. That's our company.
    - If there are several suppliers mentioned, return empty values.
    - If you can't find a perfect match for either the supplier name or the organization number, return empty values.
    - If you can't find the supplier in the supplier list, return empty values.
    - If there are several potential matches from the supplier list, return empty values.

    Empty values = the JSON below with no added content.

    Supplier List:
    {supplier_context}

    Invoice Text:
    {invoice_text}

    Return the result in JSON format:
    {{
      "supplier_name": "",
      "supplier_number": "",
      "organization_number": ""
    }}
    """
    
    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(
        prompt,
        generation_config={
            "temperature": 0.5
        }
    )

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
