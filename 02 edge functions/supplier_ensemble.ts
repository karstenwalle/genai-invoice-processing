import { createClient } from "npm:@supabase/supabase-js@2.49.4";

const { SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GEMINI_API_KEY } = Deno.env.toObject();
if (!GEMINI_API_KEY) throw new Error("Missing GEMINI_API_KEY secret");
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false
  }
});
const GEMINI_MODEL = "gemini-2.0-flash"; // switch model here if Google renames it
const GEMINI_URL = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`;
function suppliersCSV(rows) {
  return rows.map((r)=>`${r.supplier_name ?? ""}, ${r.supplier_number ?? ""}, ${r.organization_number ?? ""}`).join("\n");
}
function buildPrompt(invoiceText, supplierContext) {
  return `Choose the correct supplier number from the supplier list based on invoice text.\n- Ignore [REDACTED]. That's our company.\n- If there are several suppliers mentioned, return empty values.\n- If you can't find a perfect match for either the supplier name or the organization number, return empty values.\n- If you can't find the supplier in the supplier list, return empty values.\n- If there are several potential matches from the supplier list, return empty values.\n\nEmpty values = the JSON below with no added content.\n\nSupplier List:\n${supplierContext}\n\nInvoice Text:\n${invoiceText}\n\nReturn the result in JSON format:\n{\n  "supplier_name": "",\n  "supplier_number": "",\n  "organization_number": ""\n}`;
}
async function geminiChat(prompt) {
  const body = {
    contents: [
      {
        parts: [
          {
            text: prompt
          }
        ]
      }
    ],
    generationConfig: {
      temperature: 1
    }
  };
  const res = await fetch(GEMINI_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });
  if (!res.ok) throw new Error(`Gemini API ${res.status}: ${await res.text()}`);
  const j = await res.json();
  const text = j.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
  return text.trim();
}
function safeJSON(text) {
  let trimmed = text;
  if (trimmed.startsWith("```")) trimmed = trimmed.slice(trimmed.indexOf("\n") + 1);
  if (trimmed.endsWith("```")) trimmed = trimmed.slice(0, trimmed.lastIndexOf("```"));
  try {
    return JSON.parse(trimmed);
  } catch  {
    return {
      supplier_name: "",
      supplier_number: "",
      organization_number: ""
    };
  }
}
function majority(arr) {
  const freq = {};
  for (const v of arr)freq[String(v)] = (freq[String(v)] ?? 0) + 1;
  let top = arr[0];
  let max = 0;
  for (const [k, c] of Object.entries(freq))if (c > max) {
    max = c;
    top = k;
  }
  return {
    value: top,
    count: max
  };
}
Deno.serve(async (req)=>{
  if (req.method !== "POST") return new Response("Method Not Allowed", {
    status: 405
  });
  try {
    // 1. pending supplier_prediction rows
    const { data: queueRows, error: qErr } = await supabase.from("queue").select("*").eq("action_type", "supplier_prediction").eq("status", "pending");
    if (qErr) throw qErr;
    if (!queueRows?.length) return new Response(JSON.stringify({
      message: "Nothing to process"
    }), {
      headers: {
        "Content-Type": "application/json"
      }
    });
    // invoices
    const invoiceIds = [
      ...new Set(queueRows.map((r)=>r.invoice_id))
    ];
    const { data: invoices, error: invErr } = await supabase.from("invoices").select("id, invoice_text").in("id", invoiceIds);
    if (invErr) throw invErr;
    const invoiceMap = new Map(invoices.map((i)=>[
        i.id,
        i
      ]));
    // suppliers list
    const { data: suppliers, error: supErr } = await supabase.from("suppliers").select("id, supplier_number, supplier_name, organization_number");
    if (supErr) throw supErr;
    const numberToId = {};
    suppliers.forEach((s)=>{
      if (s.supplier_number) numberToId[s.supplier_number] = s.id;
    });
    const supplierCtx = suppliersCSV(suppliers);
    const results = [];
    for (const row of queueRows){
      const inv = invoiceMap.get(row.invoice_id);
      if (!inv?.invoice_text) {
        await supabase.from("queue").update({
          status: "error",
          error_message: "Missing invoice_text",
          action_finished: Date.now()
        }).eq("id", row.id);
        continue;
      }
      try {
        // 5 calls to Gemini
        const prompt = buildPrompt(inv.invoice_text, supplierCtx);
        const calls = Array.from({
          length: 5
        }).map(()=>{
          return geminiChat(prompt);
        });
        const replies = await Promise.all(calls);
        const parsed = replies.map(safeJSON);
        const numbers = parsed.map((p)=>p.supplier_number ?? "");
        const { value: majorityNumber, count } = majority(numbers);
        if (majorityNumber && count === 5 && numberToId[majorityNumber]) {
          const supplierId = numberToId[majorityNumber];
          await supabase.from("invoices").update({
            supplier: supplierId,
            supplier_predicted: 1,
            supplier_prediction_prompt: prompt
          }).eq("id", inv.id);
          await supabase.from("queue").insert({
            invoice_id: inv.id,
            action_type: "vat_prediction",
            status: "pending",
            created_at: new Date().toISOString()
          });
          await supabase.from("queue").update({
            status: "done",
            action_finished: Date.now()
          }).eq("id", row.id);
          results.push({
            queue_id: row.id,
            status: "done",
            supplier_id: supplierId
          });
        } else {
          await supabase.from("invoices").update({
            supplier_predicted: 3
          }).eq("id", inv.id);
          await supabase.from("queue").update({
            status: "error",
            error_message: "No consensus",
            action_finished: Date.now()
          }).eq("id", row.id);
          results.push({
            queue_id: row.id,
            status: "no_consensus"
          });
        }
      } catch (e) {
        await supabase.from("queue").update({
          status: "error",
          error_message: String(e.message).slice(0, 2000),
          action_finished: Date.now()
        }).eq("id", row.id);
        results.push({
          queue_id: row.id,
          status: "error"
        });
      }
    }
    return new Response(JSON.stringify({
      processed: results
    }), {
      headers: {
        "Content-Type": "application/json"
      }
    });
  } catch (err) {
    return new Response(JSON.stringify({
      error: err.message
    }), {
      status: 500,
      headers: {
        "Content-Type": "application/json"
      }
    });
  }
});
