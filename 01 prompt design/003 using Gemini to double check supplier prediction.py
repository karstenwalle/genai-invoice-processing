import os
import pandas as pd
import json
import google.generativeai as genai

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Your Google credentials as a JSON file here"

genai.configure()

# Load the supplier list
supplier_df = pd.read_csv('context/suppliers.csv', encoding='ISO-8859-1')

# Prepare supplier context for Gemini prompt
supplier_context = "\n".join([f"{row['Supplier name']}, {row['Supplier number']}, {row['Organization number']}" 
                              for _, row in supplier_df.iterrows()])

run_name = "Your run name here"



input_folder = "runs/" + run_name + "/001 Output from OCR"
results_csv_path = "runs/" + run_name + "/002 Supplier prediction/result.csv"
output_csv_path = "runs/" + run_name + "/003 Supplier sense-check/double_checked_results.csv"

# Load previous results
results_df = pd.read_csv(results_csv_path)

corrected_df = pd.DataFrame(columns=["invoice_number", "supplier_name", "supplier_number", "organization_number", "status"])

def double_check_with_gemini(invoice_text, previous_supplier_data):

    lines = invoice_text.strip().split('\n')
    header = '\n'.join(lines[:10]) if len(lines) > 10 else '\n'.join(lines)
    footer = '\n'.join(lines[-10:]) if len(lines) > 10 else ''
    body = '\n'.join(lines[10:-10]) if len(lines) > (10 + 10) else ''


    """Use Gemini to double-check extracted supplier details."""
    prompt = f"""
You are tasked with double-checking supplier data extracted from an invoice.

### **Instructions:**  
1. Focus on the sender information in the header or footer.  
2. If the sender is clearly stated in the header or footer, return "correct."  
3. If the sender appears only in the body, return "uncertain."  
4. If multiple company names are mentioned, return "uncertain."  
5. If you cannot confidently verify the extracted supplier, return "uncertain."  
6. Do NOT suggest a new supplier — only evaluate the existing prediction.  

### **Hierarchy of Importance:**  
- The company name in the header or footer takes priority over mentions in the body.  
- If the extracted supplier does not align with the header or footer, return "uncertain."  

### **Header:**  
{header}

### **Invoice Body:**  
{body}

### **Footer:**  
{footer}

### **Extracted Data:**  
{json.dumps(previous_supplier_data, indent=2)}

### **Return the result in JSON format:**  
{{
  "status": ""  // "correct" or "uncertain"
}}
"""
    
    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)

    try:
        # Extract JSON from the response
        json_text = response.candidates[0].content.parts[0].text

        # Remove markdown formatting if present
        if json_text.startswith("```json"):
            json_text = json_text[7:]
        if json_text.endswith("```"):
            json_text = json_text[:-3]

        # Parse JSON response
        corrected_data = json.loads(json_text.strip())
        return corrected_data

    except Exception as e:
        print(f"Error double-checking supplier data: {e}")
        return None

for _, row in results_df.iterrows():
    invoice_number = row['invoice_number']
    file_path = os.path.join(input_folder, f"{invoice_number}.txt")

    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            invoice_text = file.read()

        # Prepare the previous extracted data for Gemini check
        previous_supplier_data = {
            "supplier_name": row["supplier_name"],
            "supplier_number": row["supplier_number"],
            "organization_number": row["organization_number"]
        }

        # Double-check with Gemini
        corrected_data = double_check_with_gemini(invoice_text, previous_supplier_data)

        if corrected_data:
            # Save corrected data
            new_row = pd.DataFrame([{
                "invoice_number": invoice_number,
                "status": corrected_data.get("status", "uncertain")
            }])

            corrected_df = pd.concat([corrected_df, new_row], ignore_index=True)

corrected_df.to_csv(output_csv_path, index=False)

print(f"Double-checked results saved to {output_csv_path}")
