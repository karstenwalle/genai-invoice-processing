import os
from google.cloud import documentai_v1 as documentai
import google.generativeai as genai
import fitz

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Your Goolge credentials as a JSON file here"

# Google Cloud settings
PROJECT_ID = "Your Google Cloud project ID here"
LOCATION = "eu" 
PROCESSOR_ID = "Your Document AI processor ID here"  

document_ai_client = documentai.DocumentProcessorServiceClient(
    client_options={"api_endpoint": f"{LOCATION}-documentai.googleapis.com"}
)

run_name = "Your run name here"

genai.configure()

def extract_first_15_pages(input_path, output_path):
    """Extracts the first 15 pages of a PDF using PyMuPDF."""
    doc = fitz.open(input_path)
    new_doc = fitz.open()

    for i in range(min(15, len(doc))):
        new_doc.insert_pdf(doc, from_page=i, to_page=i)

    new_doc.save(output_path)
    new_doc.close()
    doc.close()

def process_invoice_ocr(pdf_path):
    """Processes a PDF invoice and extracts text using Google Document AI OCR."""
    try:
        temp_pdf = "temp_first_15_pages.pdf"
        extract_first_15_pages(pdf_path, temp_pdf)

        with open(temp_pdf, "rb") as file:
            pdf_bytes = file.read()

        request = documentai.ProcessRequest(
            name=f"projects/{PROJECT_ID}/locations/{LOCATION}/processors/{PROCESSOR_ID}",
            raw_document=documentai.RawDocument(content=pdf_bytes, mime_type="application/pdf")
        )

        result = document_ai_client.process_document(request=request)

        os.remove(temp_pdf)

        return result.document.text

    except Exception as e:
        print(f"Error processing file '{pdf_path}': {e}")
        return None


input_folder = "runs/" + run_name + "/000 Initial input"
output_text_folder = "runs/" + run_name + "/001 Output from OCR/"

os.makedirs(output_text_folder, exist_ok=True)

for pdf_file in os.listdir(input_folder):
    if pdf_file.endswith(".pdf"):
        pdf_path = os.path.join(input_folder, pdf_file)

        extracted_text = process_invoice_ocr(pdf_path)

        if extracted_text:
            output_text_path = os.path.join(output_text_folder, pdf_file.replace(".pdf", ".txt"))

            with open(output_text_path, "w", encoding="utf-8") as text_file:
                text_file.write(extracted_text)

            print(f"Processed OCR: {pdf_file} → {output_text_path}")

        else:
            print(f"Failed to process OCR: {pdf_file}")
