// Proxy de /oferta/* hacia el SSR de Railway.
//
// Los crawlers de WhatsApp/LinkedIn/Facebook/X/Slack hacen GET a
// `https://estadoemplea.pages.dev/oferta/:id` (el enlace que compartimos) y
// leen og:image / og:title / og:description. Railway renderiza esa página con
// los meta dinámicos por oferta; aquí la reenviamos para que el unfurl muestre
// la tarjeta correcta en vez del fallback genérico.
//
// `[[path]]` captura /oferta/123 y /oferta/123-slug-de-la-oferta.
import { proxyARailway } from '../_railway-proxy.js';

export const onRequest = (context) => proxyARailway(context.request);
