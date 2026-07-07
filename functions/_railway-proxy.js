// Proxy reutilizable hacia el backend en Railway.
//
// Por qué existe:
// ---------------
// Cloudflare Pages NO soporta proxy (status 200) en `_redirects` hacia
// orígenes EXTERNOS — esas reglas solo aplican a rutas internas del propio
// sitio. Para que `/oferta/:id` (SSR con meta og:* dinámicos) y las imágenes
// OG (`/api/og/*`) se sirvan desde el dominio de Pages sin redirección
// visible, hay que reenviarlas explícitamente con una Pages Function.
//
// Los archivos/carpetas con prefijo "_" se excluyen del enrutamiento de
// Pages Functions, así que este módulo solo se importa, no se expone.
export const RAILWAY_BACKEND = 'https://contrataoplanta-production.up.railway.app';

export async function proxyARailway(request) {
  const inUrl = new URL(request.url);
  const targetUrl = RAILWAY_BACKEND + inUrl.pathname + inUrl.search;

  // Conservamos método, headers (User-Agent del crawler, Accept, etc.) y body.
  // Quitamos `host` para que el runtime use el del destino (Railway).
  const headers = new Headers(request.headers);
  headers.delete('host');
  // Marca "vengo del dominio de marca": el backend agrega X-Robots-Tag
  // noindex a todo lo que no llegue por contrataoplanta.cl (para desindexar
  // el host técnico de Railway), y esta cabecera lo exime — el contenido
  // proxeado se sirve bajo el dominio canónico y SÍ debe indexarse.
  headers.set('x-canonical-proxy', '1');

  const init = {
    method: request.method,
    headers,
    redirect: 'manual', // que el cliente decida sobre 3xx, no el edge
  };
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    init.body = request.body;
  }

  let resp;
  try {
    resp = await fetch(targetUrl, init);
  } catch (err) {
    return new Response('Backend no disponible (Railway).', {
      status: 502,
      headers: { 'content-type': 'text/plain; charset=utf-8' },
    });
  }

  // Reemitimos status + headers + body. Se quita X-Robots-Tag por si el
  // backend igual lo agregó (defensa en profundidad): bajo el dominio de
  // marca este contenido SÍ debe indexarse.
  const outHeaders = new Headers(resp.headers);
  outHeaders.delete('x-robots-tag');
  return new Response(resp.body, {
    status: resp.status,
    statusText: resp.statusText,
    headers: outHeaders,
  });
}
