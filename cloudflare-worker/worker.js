/**
 * Cloudflare Worker: EVCC & Solar-Log Dashboard Telemetry Storage (R2)
 *
 * Bindings:
 *   - R2 Bucket Binding: DATA_BUCKET
 *   - Secret Env Var:    WRITE_TOKEN
 */

const ALLOWED_ORIGINS = new Set(["https://bbbdc.github.io"]);
const MAX_PAYLOAD_BYTES = 2 * 1024 * 1024; // 2 MiB
const DATA_KEY = "data.json";

function isAllowedOrigin(origin) {
  if (!origin) return false;
  if (ALLOWED_ORIGINS.has(origin)) return true;
  try {
    const url = new URL(origin);
    return (
      url.protocol === "http:" &&
      (url.hostname === "localhost" || url.hostname === "127.0.0.1")
    );
  } catch {
    return false;
  }
}

function corsHeaders(request) {
  const origin = request.headers.get("Origin");
  const allowOrigin = isAllowedOrigin(origin)
    ? origin
    : "https://bbbdc.github.io";

  return {
    "Access-Control-Allow-Origin": allowOrigin,
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
    Vary: "Origin",
  };
}

function jsonResponse(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
      "X-Content-Type-Options": "nosniff",
      ...extraHeaders,
    },
  });
}

async function timingSafeTokenEqual(provided, expected) {
  if (typeof provided !== "string" || typeof expected !== "string" || !expected) {
    return false;
  }
  const encoder = new TextEncoder();
  const [hashA, hashB] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(provided)),
    crypto.subtle.digest("SHA-256", encoder.encode(expected)),
  ]);
  const bytesA = new Uint8Array(hashA);
  const bytesB = new Uint8Array(hashB);
  let diff = 0;
  for (let i = 0; i < bytesA.length; i++) {
    diff |= bytesA[i] ^ bytesB[i];
  }
  return diff === 0;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method.toUpperCase();
    const cors = corsHeaders(request);

    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }

    if (path === "/health") {
      if (method !== "GET" && method !== "HEAD") {
        return jsonResponse({ error: "method_not_allowed" }, 405, cors);
      }
      if (!env.DATA_BUCKET) {
        return jsonResponse(
          { ok: false, error: "DATA_BUCKET binding missing" },
          500,
          cors
        );
      }
      const head = await env.DATA_BUCKET.head(DATA_KEY);
      const body = {
        ok: true,
        has_data: Boolean(head),
        uploaded_at:
          head?.customMetadata?.updatedAt || head?.uploaded?.toISOString() || null,
        size: head?.size ?? 0,
        token_configured: Boolean(env.WRITE_TOKEN),
      };
      if (method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "Content-Type": "application/json; charset=utf-8",
            "Cache-Control": "no-store",
            ...cors,
          },
        });
      }
      return jsonResponse(body, 200, cors);
    }

    if (path === "/" || path === "/data.json") {
      if (method === "GET" || method === "HEAD") {
        if (!env.DATA_BUCKET) {
          return jsonResponse(
            { error: "DATA_BUCKET binding missing" },
            500,
            cors
          );
        }
        const obj = await env.DATA_BUCKET.get(DATA_KEY);
        if (!obj) {
          return jsonResponse({ error: "no_data_yet" }, 404, cors);
        }
        const updatedAt =
          obj.customMetadata?.updatedAt || obj.uploaded.toISOString();
        return new Response(method === "HEAD" ? null : obj.body, {
          status: 200,
          headers: {
            "Content-Type": "application/json; charset=utf-8",
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            Pragma: "no-cache",
            ETag: obj.httpEtag,
            "X-Updated-At": updatedAt,
            "X-Content-Type-Options": "nosniff",
            ...cors,
          },
        });
      }

      if (method === "PUT" && path === "/data.json") {
        if (!env.DATA_BUCKET || !env.WRITE_TOKEN) {
          return jsonResponse({ error: "server_misconfigured" }, 500);
        }

        const authHeader = request.headers.get("Authorization") || "";
        const match = authHeader.match(/^Bearer\s+(.+)$/i);
        const token = match ? match[1].trim() : "";
        if (!(await timingSafeTokenEqual(token, env.WRITE_TOKEN))) {
          return jsonResponse({ error: "unauthorized" }, 401);
        }

        const body = await request.text();
        if (!body) {
          return jsonResponse({ error: "empty_body" }, 400);
        }
        const byteLength = new TextEncoder().encode(body).byteLength;
        if (byteLength > MAX_PAYLOAD_BYTES) {
          return jsonResponse({ error: "payload_too_large" }, 413);
        }

        let parsed;
        try {
          parsed = JSON.parse(body);
        } catch {
          return jsonResponse({ error: "invalid_json" }, 400);
        }
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          return jsonResponse({ error: "invalid_json" }, 400);
        }

        const updatedAt = new Date().toISOString();
        await env.DATA_BUCKET.put(DATA_KEY, body, {
          httpMetadata: {
            contentType: "application/json; charset=utf-8",
            cacheControl: "no-store",
          },
          customMetadata: {
            updatedAt,
          },
        });

        return jsonResponse(
          { ok: true, bytes: byteLength, updated_at: updatedAt },
          200
        );
      }

      return jsonResponse({ error: "method_not_allowed" }, 405, cors);
    }

    return jsonResponse({ error: "not_found" }, 404, cors);
  },
};
