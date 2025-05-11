import { createClient } from "npm:@supabase/supabase-js@2.49.4";
const { SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GEMINI_API_KEY } = Deno.env.toObject();
if (!GEMINI_API_KEY) throw new Error("Missing GEMINI_API_KEY");
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false
  }
});
const GEMINI_MODEL = "gemini-2.0-flash";
const GEMINI_URL = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`;
/***** helpers *****/ const toCSV = (rows, cols)=>rows.map((r)=>cols.map((c)=>r[c] ?? "").join(", ")).join("\n");
async function gemini(prompt) {
  const res = await fetch(GEMINI_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
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
    })
  });
  if (!res.ok) throw new Error(`Gemini ${res.status}: ${await res.text()}`);
  const j = await res.json();
  return (j.candidates?.[0]?.content?.parts?.[0]?.text ?? "").trim();
}
const stripJson = (t)=>{
  let s = t.trim();
  if (s.startsWith("```")) s = s.slice(s.indexOf("\n") + 1);
  if (s.endsWith("```")) s = s.slice(0, s.lastIndexOf("```"));
  return s;
};
function buildPrompt(invoiceText, vatCSV, supplierCtx, oldInv, oldReturn) {
  let p = `Please find the sum payable and group the attached invoice by VAT type.\n- Payable amount is gross (incl. VAT).\n- Net amounts per VAT line exclude VAT.\n- Use only VAT types provided.\n- Negative amounts for credit notes.\n- If outside Norway: VAT type 22 for food items, 21 otherwise (0% VAT).\n\nSupplier:\n${supplierCtx ?? ""}\n\nVAT types (id, rate, description):\n${vatCSV}\n\nInvoice text:\n${invoiceText}\n`;
  if (oldInv && oldReturn) {
    p += `\nBelow is an old invoice from the same supplier and the correct JSON output. Use it as guidance.\nOld invoice:\n${oldInv}\nOld return:\n${oldReturn}\n`;
  }
  p += `\nReturn raw JSON only (no markdown) exactly like:\n[{"date":"","general description":"","payable_gross_amount":0,"vat_lines":[{"vatType":0,"net_amount":0}]}]`;
  return p;
}
function validate(output, rateMap) {
  if (!Array.isArray(output) || !output.length) return false;
  const head = output[0];
  let sum = 0;
  for (const l of head.vat_lines ?? []){
    const rate = rateMap.get(Number(l.vatType));
    if (rate === undefined) return false;
    const net = Number(l.net_amount);
    const gross = net + net * rate;
    sum += gross;
  }
  return Math.abs(sum - Number(head.payable_gross_amount)) <= 0.02;
}
function groupByVat(lines) {
  const map = {};
  for (const l of lines)map[l.vat_type] = (map[l.vat_type] ?? 0) + Number(l.net_amount);
  return Object.entries(map).map(([vat, net])=>({
      vatType: Number(vat),
      net_amount: net
    }));
}
/***** main *****/ Deno.serve(async (req)=>{
  if (req.method !== "POST") return new Response("Method Not Allowed", {
    status: 405
  });
  const { data: queue } = await supabase.from("queue").select("*").eq("action_type", "vat_prediction").eq("status", "pending");
  if (!queue?.length) return new Response("{}", {
    headers: {
      "Content-Type": "application/json"
    }
  });
  const { data: vatTypes } = await supabase.from("vat_types").select("id, vat_rate, description");
  const vatCSV = toCSV(vatTypes ?? [], [
    "id",
    "vat_rate",
    "description"
  ]);
  const rateMap = new Map((vatTypes ?? []).map((v)=>[
      v.id,
      Number(v.vat_rate)
    ]));
  const results = [];
  for (const q of queue){
    const { data: inv } = await supabase.from("invoices").select("id, supplier, invoice_text").eq("id", q.invoice_id).single();
    if (!inv?.invoice_text) {
      await supabase.from("queue").update({
        status: "failed",
        error_message: "missing text",
        action_finished: Date.now()
      }).eq("id", q.id);
      await supabase.from("invoices").update({
        vat_lines_predicted: 3
      }).eq("id", inv?.id ?? "");
      continue;
    }
    const supplierCtx = inv.supplier ? (await supabase.from("suppliers").select("*").eq("id", inv.supplier).single()).data : null;
    const supplierStr = supplierCtx ? JSON.stringify(supplierCtx, null, 2) : null;
    // fetch old booked invoice
    let oldInvTxt, oldReturnJson;
    if (inv.supplier) {
      const { data: booked } = await supabase.from("invoices").select("id, invoice_text").eq("supplier", inv.supplier).eq("is_booked", 1).limit(1);
      if (booked?.length) {
        oldInvTxt = booked[0].invoice_text;
        const { data: bookedLines } = await supabase.from("invoice_lines").select("vat_type, net_amount").eq("invoice", booked[0].id);
        if (bookedLines?.length) {
          const grouped = groupByVat(bookedLines);
          const payable = grouped.reduce((s, l)=>s + l.net_amount * (1 + (rateMap.get(l.vatType) ?? 0)), 0);
          oldReturnJson = JSON.stringify([
            {
              date: "",
              "general description": "",
              payable_gross_amount: +payable.toFixed(2),
              vat_lines: grouped
            }
          ], null, 2);
        }
      }
    }
    const prompt = buildPrompt(inv.invoice_text, vatCSV, supplierStr, oldInvTxt, oldReturnJson);
    try {
      const raw = await gemini(prompt);
      const parsed = JSON.parse(stripJson(raw));
      const ok = validate(parsed, rateMap);
      if (!ok) throw new Error("validation");
      const voucher = parsed[0];
      const gross = Number(voucher.payable_gross_amount);
      await supabase.from("invoice_lines").delete().eq("invoice", inv.id); // reset
      for (const l of voucher.vat_lines){
        const rate = rateMap.get(Number(l.vatType)) ?? 0;
        const net = Number(l.net_amount);
        const vat = +(net * rate).toFixed(2);
        await supabase.from("invoice_lines").insert({
          invoice: inv.id,
          vat_type: l.vatType,
          net_amount: net,
          vat_amount: vat
        });
      }
      await supabase.from("invoices").update({
        amount: gross,
        vat_lines_predicted: 1,
        vat_prediction_prompt: prompt
      }).eq("id", inv.id);
      await supabase.from("queue").update({
        status: "done",
        action_finished: Date.now()
      }).eq("id", q.id);
      await supabase.from("queue").insert({
        invoice_id: inv.id,
        action_type: "account_prediction",
        status: "pending",
        created_at: new Date().toISOString()
      });
      results.push({
        id: q.id,
        status: "done"
      });
    } catch (err) {
      await supabase.from("invoices").update({
        vat_lines_predicted: 3,
        vat_prediction_prompt: prompt
      }).eq("id", inv.id);
      await supabase.from("queue").update({
        status: "failed",
        error_message: String(err instanceof Error ? err.message : err).slice(0, 2000),
        action_finished: Date.now()
      }).eq("id", q.id);
      results.push({
        id: q.id,
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
});
