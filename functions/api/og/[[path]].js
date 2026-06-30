// Proxy de /api/og/* hacia Railway (imágenes OG dinámicas: 1200x630 / 1080x1080).
//
// Se usa cuando el HTML SSR de una oferta referencia la imagen OG con una ruta
// relativa al dominio de Pages. Status 200 (sin redirección) para que el PNG se
// vea como del mismo origen y no lo bloqueen referrer/atributo download.
//
// `[[path]]` captura, p.ej., /api/og/123.png?format=horizontal.
import { proxyARailway } from '../../_railway-proxy.js';

export const onRequest = (context) => proxyARailway(context.request);
