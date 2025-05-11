import os
import json
import ast
import re
from collections import defaultdict
from typing import List, Dict

import pandas as pd
import google.generativeai as genai

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Your Google credentials as a JSON file here"

genai.configure()

RUN_NAME = "Your run name here"
NUM_ATTEMPTS = 3
OUTPUT_CSV_PATH = f"runs/{RUN_NAME}/004 Booking of the voucher/account_department_lines.csv"


vat_line_predictions = pd.read_csv(
    f"runs/{RUN_NAME}/004 Booking of the voucher/finished vat_lines.csv"
)

# context tables
suppliers_with_id = pd.read_csv("context/suppliers_with_id.csv", encoding="ISO-8859-1")
supplier_postings = pd.read_csv(
    "context/supplier_postings_2022-01-01_-_2022-08-31.csv", encoding="ISO-8859-1"
)
accounts_df = pd.read_csv("context/accounts.csv", encoding="ISO-8859-1")
departments_df = pd.read_csv("context/departments.csv", encoding="ISO-8859-1")
vat_codes_df = pd.read_csv("context/vat_codes.csv", encoding="ISO-8859-1")
filtered_postings = pd.read_csv(
    "context/supplier_postings_2022-01-01_-_2022-08-31/filtered_supplier_postings.csv"
)

def construct_vat_lines(voucher_id: int, vat_lines_preds: pd.DataFrame):
    """Return VAT line skeletons (without account/department) for prompt."""
    rows = vat_lines_preds.loc[
        vat_lines_preds["voucher"] == voucher_id, ["vatType", "net_amount"]
    ]
    return (
        [
            {
                "vat_lines": [
                    {
                        "vatType": r.vatType,
                        "net_amount": r.net_amount,
                        "department": "",
                        "account": "",
                    }
                ]
            }
            for _, r in rows.iterrows()
        ]
        if not rows.empty
        else []
    )


def get_first_voucher(supplier_id, df_of_vouchers, vat_codes_df):
    """Return (voucher_id, [question‑style data]) or False if none found."""
    df = df_of_vouchers[df_of_vouchers["supplier"] == supplier_id]
    if df.empty:
        return False
    voucher_id = df.sort_values("date").iloc[0]["voucher"]
    rows = df[df["voucher"] == voucher_id]

    vmap = vat_codes_df.copy()
    vmap["VAT rate"] = vmap["VAT rate"].str.rstrip("% ").astype(float) / 100
    merged = rows.merge(vmap, left_on="vatType", right_on="VAT code", how="left")

    question = {
        "vat_lines": [
            {
                "vatType": r.vatType,
                "net_amount": r.amount,
                "account": "",
                "department": "",
            }
            for _, r in merged.iterrows()
        ]
    }
    return voucher_id, [question]


def get_first_voucher_answer(supplier_id, df_of_vouchers, vat_codes_df):
    """Return (voucher_id, [answer‑style data]) or False if none found."""
    df = df_of_vouchers[df_of_vouchers["supplier"] == supplier_id]
    if df.empty:
        return False
    voucher_id = df.sort_values("date").iloc[0]["voucher"]
    rows = df[df["voucher"] == voucher_id]

    vmap = vat_codes_df.copy()
    vmap["VAT rate"] = vmap["VAT rate"].str.rstrip("% ").astype(float) / 100
    merged = rows.merge(vmap, left_on="vatType", right_on="VAT code", how="left")

    answer = {
        "vat_lines": [
            {
                "vatType": r.vatType,
                "net_amount": r.amount,
                "account": r.account,
                "department": r.department,
            }
            for _, r in merged.iterrows()
        ]
    }
    return voucher_id, [answer]


def load_voucher_text(voucher_id):
    path = f"context/supplier_postings_2022-01-01_-_2022-08-31/ocr/{voucher_id}.txt"
    return open(path, encoding="utf-8").read() if os.path.exists(path) else None


def extract_invoice_details(
    predicted_vat_lines, 
    invoice_text: str,
    supplier_data: Dict,
    example_voucher_question,
    example_voucher_answer,
    example_voucher_invoice,
    has_old_voucher: bool,
):
    """Ask Gemini to fill in account / department for the VAT lines."""

    common_part = f"""
You are a Norwegian accountant following Norwegian accounting standards.
- I have an invoice and the VAT lines for that invoice. For each VAT line, pick the correct **account code** and **department**.
- Keep the VAT lines. They are correct.
- If there is supplier context, adhere to it.
- Always use double quotes – never single quotes.

### Supplier
{json.dumps(supplier_data, indent=2)}

### Chart of accounts
{accounts_df}

### Departments
{departments_df}

### Invoice text
{invoice_text}

### Return RAW JSON (no markdown, no backticks) ***exactly*** in this format:
{predicted_vat_lines}
"""

    if has_old_voucher:
        prompt = (
            common_part
            + f"""

Below is an **old invoice** from the same supplier. Use it as an example.

### Old invoice text
{example_voucher_invoice}

### VAT lines for the old invoice
{example_voucher_question}

### Correct return value for the old invoice
{example_voucher_answer}
"""
        )
    else:
        prompt = common_part

    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt, generation_config={"temperature": 1})

    if not response.candidates:
        return []

    txt = response.candidates[0].content.parts[0].text.strip()
    code_block = re.search(r"```(?:json)?\s*(.*?)\s*```", txt, re.S)
    if code_block:
        txt = code_block.group(1).strip()

    try:
        parsed = json.loads(txt)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(txt)
        except Exception:
            return []

    if isinstance(parsed, dict):
        parsed = [parsed]
    return [p for p in parsed if isinstance(p, dict)]



def consensus_runs(runs: List[List[Dict]], voucher_id: int) -> List[Dict]:
    """Return consensus VAT‑lines for *one* voucher across several runs."""

    if not runs or any(len(r) == 0 for r in runs):
        return []  # at least one run failed to parse

    base = runs[0][0]
    out_item = {"voucher": voucher_id, "vat_lines": []}

    lines_by_type = defaultdict(list)
    for run in runs:
        parent = run[0]
        for line in parent.get("vat_lines", []):
            lines_by_type[line["vatType"]].append(line)

    for vat_type, same_type_lines in lines_by_type.items():
        if len(same_type_lines) < len(runs):
            # at least one run missing this VAT type, skip
            continue

        # pull values in the *same order* as the runs list
        accounts = [l.get("account", "") for l in same_type_lines]
        depts = [l.get("department", "") for l in same_type_lines]
        net_amounts = [l.get("net_amount") for l in same_type_lines]
        # net_amount should be identical across runs – pick first

        account = accounts[0] if all(a == accounts[0] for a in accounts) else ""
        department = depts[0] if all(d == depts[0] for d in depts) else ""

        out_item["vat_lines"].append(
            {
                "vatType": vat_type,
                "net_amount": net_amounts[0],
                "account": account,
                "department": department,
            }
        )

    return [out_item]


INPUT_OCR_FOLDER = f"runs/{RUN_NAME}/001 Output from OCR"
result_output = []

for voucher_id in vat_line_predictions["voucher"].unique():
    voucher_rows = vat_line_predictions[vat_line_predictions["voucher"] == voucher_id]
    supplier_id = voucher_rows["supplier_id"].iat[0]

    supplier_data = (
        suppliers_with_id[suppliers_with_id["id"] == supplier_id].squeeze().to_dict()
    )

    ocr_path = os.path.join(INPUT_OCR_FOLDER, f"{voucher_id}.txt")
    if not os.path.exists(ocr_path):
        print(f"Missing OCR for voucher {voucher_id}")
        continue

    invoice_text = open(ocr_path, encoding="utf-8").read()

    # Few‑shot examples (if any)
    first_q = first_a = first_txt = "N/A"
    has_example = False

    ex_q_res = get_first_voucher(supplier_id, filtered_postings, vat_codes_df)
    ex_a_res = get_first_voucher_answer(supplier_id, filtered_postings, vat_codes_df)
    if ex_q_res and ex_a_res:
        old_id, first_q = ex_q_res
        _, first_a = ex_a_res
        first_txt = load_voucher_text(old_id) or "N/A"
        has_example = True

    # Skeleton VAT lines for the current voucher
    skeleton_lines = construct_vat_lines(voucher_id, vat_line_predictions)

    # Run the LLM several times
    all_runs = []
    for _ in range(NUM_ATTEMPTS):
        all_runs.append(
            extract_invoice_details(
                skeleton_lines,
                invoice_text,
                supplier_data,
                first_q,
                first_a,
                first_txt,
                has_example,
            )
        )

    # Consensus
    consensus = consensus_runs(all_runs, voucher_id)
    result_output.extend(consensus)


flattened = []
for item in result_output:
    v_id = item.get("voucher")
    for line in item.get("vat_lines", []):
        flattened.append(
            {
                "voucher": v_id,
                "vatType": line.get("vatType"),
                "net_amount": line.get("net_amount"),
                "department": line.get("department"),
                "account": line.get("account"),
            }
        )

pd.DataFrame(flattened).to_csv(OUTPUT_CSV_PATH, index=False, encoding="utf-8")
print(f"Consensus results saved to {OUTPUT_CSV_PATH}")
