import { createClient } from "npm:@supabase/supabase-js@2.49.4";
const { SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GEMINI_API_KEY } = Deno.env.toObject();
if (!GEMINI_API_KEY) throw new Error("Missing GEMINI_API_KEY");
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false
  }
});
const GEMINI_URL = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${GEMINI_API_KEY}`;
const NUM_RUNS = 3;
const toCSV = (rows, cols)=>rows.map((r)=>cols.map((c)=>r[c] ?? "").join(", ")).join("\n");
const stripJson = (t)=>{
  let s = t.trim();
  if (s.startsWith("```")) s = s.slice(s.indexOf("\n") + 1);
  if (s.endsWith("```")) s = s.slice(0, s.lastIndexOf("```"));
  return s;
};
const majority = (vals)=>{
  const freq = {};
  for (const v of vals)freq[v] = (freq[v] ?? 0) + 1;
  const [val, count] = Object.entries(freq).sort((a, b)=>b[1] - a[1])[0];
  return count >= 2 ? val : "";
};
const mapAccount = (v, chart)=>{
  const s = v.trim();
  if (/^[0-9a-fA-F-]{36}$/.test(s)) return s;
  const byCode = chart.find((c)=>String(c.code) === s);
  if (byCode) return byCode.id;
  const byName = chart.find((c)=>c.name?.toLowerCase() === s.toLowerCase());
  return byName?.id ?? null;
};
const mapDept = (v, depts)=>{
  const s = v.trim();
  if (/^[0-9a-fA-F-]{36}$/.test(s)) return s;
  const byId = depts.find((d)=>String(d.id) === s);
  if (byId) return byId.id;
  const byName = depts.find((d)=>d.name?.toLowerCase() === s.toLowerCase());
  return byName?.id ?? null;
};
const buildPrompt = (invoiceText, supplierCtx, skeleton, chartCSV, deptCSV, exText, exQ, exA)=>{
  let p = `You are a Norwegian accountant. For each VAT line, choose the correct account code and department.\n\nSupplier:\n${supplierCtx ?? ""}\n\nChart of accounts (number, name):\n${chartCSV}\n\nDepartments (id, name):\n${deptCSV}\n\nInvoice text:\n${invoiceText}\n\nReturn raw JSON only (no markdown):\n${JSON.stringify(skeleton, null, 2)}\n`;
  if (exText && exQ && exA) {
    p += `\n### Example\nOld invoice text:\n${exText}\nOld VAT lines:\n${exQ}\nCorrect return:\n${exA}`;
  }
  return p;
};
async function gemini(prompt) {
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
  const r = await fetch(GEMINI_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error(`Gemini ${r.status}`);
  const j = await r.json();
  return (j.candidates?.[0]?.content?.parts?.[0]?.text ?? "").trim();
}
/***** main *****/ Deno.serve(async (req)=>{
  if (req.method !== "POST") return new Response("Method Not Allowed", {
    status: 405
  });
  const { data: queue } = await supabase.from("queue").select("*").eq("action_type", "account_prediction").eq("status", "pending");
  if (!queue?.length) return new Response("{}", {
    headers: {
      "Content-Type": "application/json"
    }
  });
  const out = [];
  for (const row of queue){
    const { data: inv } = await supabase.from("invoices").select("id, invoice_text, supplier, organization_id").eq("id", row.invoice_id).single();
    if (!inv) continue;
    const { data: chart } = await supabase.from("chart_of_accounts").select("id, account_code, account_name").eq("organization_id", inv.organization_id);
    const { data: depts } = await supabase.from("departments").select("id, department_name, department_number").eq("organization_id", inv.organization_id);
    const chartCSV = toCSV(chart ?? [], [
      "id",
      "account_code",
      "account_name"
    ]);
    const deptCSV = toCSV(depts ?? [], [
      "id",
      "department_number",
      "department_name"
    ]);
    const supplierCtx = inv.supplier ? (await supabase.from("suppliers").select("*").eq("id", inv.supplier).single()).data : null;
    const supplierStr = supplierCtx ? JSON.stringify(supplierCtx, null, 2) : null;
    const { data: lines } = await supabase.from("invoice_lines").select("vat_type, net_amount").eq("invoice", inv.id);
    const skeleton = [
      {
        vat_lines: (lines ?? []).map((l)=>({
            vatType: l.vat_type,
            net_amount: l.net_amount,
            account: "",
            department: ""
          }))
      }
    ];
    // booked one‑shot
    let exText, exQ, exA;
    if (inv.supplier) {
      const b = await supabase.from("invoices").select("id, invoice_text").eq("supplier", inv.supplier).eq("is_booked", 1).limit(1);
      console.log("b");
      console.log(b);
      if (b.data?.length) {
        exText = b.data[0].invoice_text;
        const bl = await supabase.from("invoice_lines").select("vat_type, net_amount, account, department").eq("invoice", b.data[0].id);
        if (bl.data?.length) {
          exQ = JSON.stringify([
            {
              vat_lines: bl.data.map((l)=>({
                  vatType: l.vat_type,
                  net_amount: l.net_amount,
                  account: "",
                  department: ""
                }))
            }
          ], null, 2);
          exA = JSON.stringify([
            {
              vat_lines: bl.data.map((l)=>({
                  vatType: l.vat_type,
                  net_amount: l.net_amount,
                  account: l.account,
                  department: l.department
                }))
            }
          ], null, 2);
        }
      }
    }
    const prompt = buildPrompt(inv.invoice_text, supplierStr, skeleton, chartCSV, deptCSV, exText, exQ, exA);
    console.log(exText);
    console.log(exQ);
    console.log(exA);
    console.log(prompt);
    try {
      const runs = [];
      for(let i = 0; i < NUM_RUNS; i++)runs.push(JSON.parse(stripJson(await gemini(prompt))));
      const merged = (()=>{
        if (runs.length !== 3) return null;
        const base = runs[0]?.[0];
        if (!base) return null;
        const m = JSON.parse(JSON.stringify(base));
        for(let i = 0; i < m.vat_lines.length; i++){
          m.vat_lines[i].account = majority(runs.map((r)=>r[0].vat_lines[i].account ?? ""));
          m.vat_lines[i].department = majority(runs.map((r)=>r[0].vat_lines[i].department ?? ""));
        }
        return m;
      })();
      if (!merged) throw new Error("majority fail");
      for (const l of merged.vat_lines){
        const accId = l.account ? mapAccount(String(l.account), chart ?? []) : null;
        const deptId = l.department ? mapDept(String(l.department), depts ?? []) : null;
        await supabase.from("invoice_lines").update({
          account: accId,
          department: deptId
        }).eq("invoice", inv.id).eq("vat_type", l.vatType).eq("net_amount", l.net_amount);
      }
      await supabase.from("invoices").update({
        account_predicted: 1,
        department_predicted: 1,
        account_prediction_prompt: prompt
      }).eq("id", inv.id);
      await supabase.from("queue").update({
        status: "done",
        action_finished: Date.now()
      }).eq("id", row.id);
      out.push({
        id: row.id,
        status: "done"
      });
    } catch (e) {
      await supabase.from("invoices").update({
        account_predicted: 3,
        department_predicted: 3
      }).eq("id", inv.id);
      await supabase.from("queue").update({
        status: "error",
        error_message: String(e.message).slice(0, 2000),
        action_finished: Date.now()
      }).eq("id", row.id);
      out.push({
        id: row.id,
        status: "error"
      });
    }
  }
  return new Response(JSON.stringify(out), {
    headers: {
      "Content-Type": "application/json"
    }
  });
});
