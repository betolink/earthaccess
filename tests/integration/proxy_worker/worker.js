// Minimal path-prefix CORS proxy for testing earthaccess.set_proxy().
//
// The target URL is encoded as the request path, e.g.:
//
//   GET http://127.0.0.1:8787/https://cmr.earthdata.nasa.gov/search/granules.umm_json
//
// is forwarded to https://cmr.earthdata.nasa.gov/search/granules.umm_json, and
// permissive CORS headers are added to the response so the same worker can be
// deployed to unblock browser-based (JupyterLite/Pyodide) clients.
//
// NOTE: this is an open proxy by design for local testing. Set ALLOWED_HOSTS in
// wrangler.toml (comma-separated) to restrict which upstream hosts are allowed.

const DEFAULT_ALLOWED_HOSTS = [
  "cmr.earthdata.nasa.gov",
  "cmr.uat.earthdata.nasa.gov",
  "urs.earthdata.nasa.gov",
  "uat.urs.earthdata.nasa.gov",
  "status.earthdata.nasa.gov",
  "status.uat.earthdata.nasa.gov",
];

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, HEAD, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "*",
  "Access-Control-Expose-Headers": "*",
  "Access-Control-Max-Age": "86400",
};

// Headers that must not be forwarded upstream.
const HOP_BY_HOP = new Set([
  "connection",
  "content-length",
  "host",
  "accept-encoding",
]);

function allowedHosts(env) {
  const raw = env && env.ALLOWED_HOSTS;
  if (!raw) return DEFAULT_ALLOWED_HOSTS;
  return raw
    .split(",")
    .map((host) => host.trim())
    .filter(Boolean);
}

function corsResponse(body, status) {
  return new Response(body, { status, headers: CORS_HEADERS });
}

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const { pathname, search } = new URL(request.url);
    const target = pathname.slice(1) + search;

    if (!/^https?:\/\//i.test(target)) {
      return corsResponse("Missing target URL in path", 400);
    }

    let targetUrl;
    try {
      targetUrl = new URL(target);
    } catch {
      return corsResponse("Invalid target URL", 400);
    }

    const hosts = allowedHosts(env);
    if (hosts.length > 0 && !hosts.includes(targetUrl.hostname)) {
      return corsResponse(`Host not allowed: ${targetUrl.hostname}`, 403);
    }

    const headers = new Headers();
    for (const [key, value] of request.headers) {
      if (!HOP_BY_HOP.has(key.toLowerCase())) headers.set(key, value);
    }

    const hasBody = !["GET", "HEAD"].includes(request.method);
    const init = {
      method: request.method,
      headers,
      redirect: "follow",
      body: hasBody ? await request.arrayBuffer() : undefined,
    };

    let response;
    try {
      response = await fetch(targetUrl.toString(), init);
    } catch (err) {
      return corsResponse(`Proxy error: ${err}`, 502);
    }

    const responseHeaders = new Headers(response.headers);
    for (const [key, value] of Object.entries(CORS_HEADERS)) {
      responseHeaders.set(key, value);
    }

    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: responseHeaders,
    });
  },
};
