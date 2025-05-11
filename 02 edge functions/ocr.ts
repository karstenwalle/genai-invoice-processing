import { createClient } from "npm:@supabase/supabase-js@2.49.4";
import { PDFDocument } from "https://esm.sh/pdf-lib";
import { SignJWT, importPKCS8 } from "https://deno.land/x/jose@v6.0.10/index.ts";
import { encode as encodeBase64 } from "https://deno.land/std@0.177.0/encoding/base64.ts";
const BUCKET = "invoices";
const TOKEN_URL = "https://oauth2.googleapis.com/token";
const { SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GCP_SA_JSON, GCP_PROJECT_ID, GCP_LOCATION, GCP_PROCESSOR_ID } = Deno.env.toObject();
if (!GCP_SA_JSON) throw new Error("Missing GCP_SA_JSON secret");
const serviceAccount = JSON.parse(GCP_SA_JSON);
if (typeof serviceAccount.private_key !== "string" || typeof serviceAccount.client_email !== "string") {
  throw new Error("GCP_SA_JSON must include private_key and client_email");
}
serviceAccount.private_key = serviceAccount.private_key.replace(/\\r?\\n/g, "\n").trim();
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false
  }
});
async function createAccessToken() {
  const key = await importPKCS8(serviceAccount.private_key, "RS256");
  const now = Math.floor(Date.now() / 1000);
  const jwt = await new SignJWT({
    scope: "https://www.googleapis.com/auth/cloud-platform"
  }).setProtectedHeader({
    alg: "RS256",
    typ: "JWT"
  }).setIssuedAt(now).setExpirationTime(now + 3600).setIssuer(serviceAccount.client_email).setSubject(serviceAccount.client_email).setAudience(TOKEN_URL).sign(key);
  const res = await fetch(TOKEN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion: jwt
    }).toString()
  });
  if (!res.ok) throw new Error(`OAuth token exchange failed: ${res.status} ${await res.text()}`);
  const { access_token } = await res.json();
  if (!access_token) throw new Error("No access_token returned by Google");
  return access_token;
}
const DOC_AI_URL = `https://${GCP_LOCATION}-documentai.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/${GCP_LOCATION}/processors/${GCP_PROCESSOR_ID}:process`;
function toBase64(bytes) {
  return encodeBase64(bytes);
}
Deno.serve(async (req)=>{
  if (req.method === "OPTIONS") {
    return new Response("ok", {
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*, Authorization, Content-Type",
        "Access-Control-Allow-Methods": "POST, OPTIONS"
      }
    });
  }
  if (req.method !== "POST") {
    return new Response(JSON.stringify({
      error: "Method not allowed"
    }), {
      status: 405,
      headers: {
        "Content-Type": "application/json"
      }
    });
  }
  try {
    const { data: queue, error: qErr } = await supabase.from("queue").select("*").eq("action_type", "ocr").eq("status", "pending").limit(10);
    if (qErr) throw qErr;
    if (!queue?.length) return new Response(JSON.stringify({
      message: "Nothing to process"
    }), {
      headers: {
        "Content-Type": "application/json"
      }
    });
    const invoiceIds = [
      ...new Set(queue.map((q)=>q.invoice_id).filter(Boolean))
    ];
    const { data: invoices, error: invErr } = await supabase.from("invoices").select("*").in("id", invoiceIds);
    if (invErr) throw invErr;
    const invoiceMap = new Map(invoices.map((i)=>[
        i.id,
        i
      ]));
    const accessToken = await createAccessToken();
    const results = [];
    for (const row of queue){
      const inv = invoiceMap.get(row.invoice_id);
      if (!inv?.file_path) {
        await supabase.from("queue").update({
          status: "error",
          error_message: "Missing invoice or file_path",
          action_finished: Date.now()
        }).eq("id", row.id);
        results.push({
          queue_id: row.id,
          status: "skipped"
        });
        continue;
      }
      try {
        const { data: file, error: dlErr } = await supabase.storage.from(BUCKET).download(inv.file_path);
        console.log("row.invoice_id");
        console.log(row.invoice_id);
        if (dlErr) throw dlErr;
        console.log("1");
        const pdfBuf = await file.arrayBuffer();
        console.log("2");
        console.log("pdfBuf byteLength:", pdfBuf.byteLength);
        console.log("file type:", file.type);
        let fullPdf;
        try {
          fullPdf = await PDFDocument.load(pdfBuf, {
            ignoreEncryption: true
          });
        } catch (e) {
          console.error("Failed to load PDF:", e);
          throw new Error("The downloaded PDF is corrupted or invalid.");
        }
        console.log("3");
        const newPdf = await PDFDocument.create();
        console.log("4");
        let pageCount;
        try {
          pageCount = Math.min(5, fullPdf.getPageCount());
        } catch (e) {
          console.error("Failed to get page count:", e);
          throw new Error("PDF is loaded but invalid page structure.");
        }
        if (pageCount === 0) {
          throw new Error("Original PDF has no pages!");
        }
        console.log("6");
        const pages = await newPdf.copyPages(fullPdf, [
          ...Array(pageCount).keys()
        ]);
        console.log("7");
        pages.forEach((page)=>newPdf.addPage(page));
        console.log("8");
        const pdfBytes = await newPdf.save();
        console.log("9");
        if (!pdfBytes.length) {
          throw new Error("Generated PDF is empty!");
        } else {
          console.log("gen pdf is good");
        }
        console.log("10");
        const gaRes = await fetch(DOC_AI_URL, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            rawDocument: {
              content: toBase64(pdfBytes),
              mimeType: "application/pdf"
            }
          })
        });
        if (!gaRes.ok) throw new Error(`DocAI: ${gaRes.status} ${await gaRes.text()}`);
        const { document } = await gaRes.json();
        const text = document?.text ?? "";
        await supabase.from("invoices").update({
          invoice_text: text
        }).eq("id", inv.id);
        // enqueue next step before marking current row done
        await supabase.from("queue").insert({
          invoice_id: inv.id,
          action_type: "supplier_prediction",
          status: "pending",
          created_at: new Date().toISOString()
        });
        await supabase.from("queue").update({
          status: "completed",
          action_finished: Date.now()
        }).eq("id", row.id);
        results.push({
          queue_id: row.id,
          status: "completed",
          chars: text.length
        });
      } catch (err) {
        await supabase.from("queue").update({
          status: "error",
          error_message: String(err.message).slice(0, 2000),
          action_finished: Date.now()
        }).eq("id", row.id);
        results.push({
          queue_id: row.id,
          status: "error",
          message: err.message
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
