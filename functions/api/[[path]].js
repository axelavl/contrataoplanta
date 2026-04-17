// Cloudflare Pages Function: proxy same-origin de /api/* al backend FastAPI.
//
// Motivo:
// El sitio se publica también en hosts estáticos (estadoemplea.pages.dev,
// contrataoplanta.pages.dev). Allí no hay nginx que haga reverse-proxy, así
// que cualquier fetch a `/api/...` terminaría sirviendo HTML 404 y la cadena
// de fallbacks absolutos dispara "Failed to fetch" por CORS o DNS.
// Esta función convierte el llamado en same-origin y reenvía al backend.

const UPSTREAM = 'https://contrataoplanta.cl';

// Headers inyectados por Cloudflare o por el host estático que no deben viajar
// al upstream: al reenviarlos rompen el routing o la terminación TLS.
const STRIP_REQUEST_HEADERS = [
  'host',
  'cf-connecting-ip',
  'cf-ray',
  'cf-visitor',
  'cf-ipcountry',
  'cf-ew-via',
  'x-forwarded-for',
  'x-forwarded-host',
  'x-forwarded-proto',
  'x-real-ip',
];

export async function onRequest({ request }) {
  const incoming = new URL(request.url);
  const target = UPSTREAM + incoming.pathname + incoming.search;

  const headers = new Headers(request.headers);
  for (const name of STRIP_REQUEST_HEADERS) headers.delete(name);

  const init = {
    method: request.method,
    headers,
    redirect: 'follow',
  };
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    init.body = request.body;
  }

  try {
    const upstream = await fetch(target, init);
    // El runtime de Cloudflare puede descomprimir el cuerpo aguas arriba; en
    // ese caso reenviar `content-encoding`/`content-length` hace que el browser
    // intente descomprimir bytes ya expandidos y aborte la respuesta.
    const respHeaders = new Headers(upstream.headers);
    respHeaders.delete('content-encoding');
    respHeaders.delete('content-length');
    return new Response(upstream.body, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: respHeaders,
    });
  } catch (err) {
    const detail = err && err.message ? err.message : String(err);
    return new Response(
      JSON.stringify({
        error: 'backend_unreachable',
        upstream: target,
        detail,
      }),
      {
        status: 502,
        headers: { 'content-type': 'application/json; charset=utf-8' },
      },
    );
  }
}
