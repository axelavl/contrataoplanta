// Proxy de /empleos/* hacia el SSR de Railway.
//
// Las landings SEO por región/sector/institución (/empleos/region/{slug},
// /empleos/sector/{slug}, /empleos/institucion/{id}-{slug}) las renderiza
// Railway (api/routers/web.py). Cloudflare Pages no las tiene como archivos
// estáticos y no puede proxear a un origen externo vía _redirects, así que sin
// esta Function devolvía 404 y los enlaces del footer/landing quedaban rotos.
//
// `[[path]]` captura región/sector/institución y cualquier sub-ruta.
import { proxyARailway } from '../_railway-proxy.js';

export const onRequest = (context) => proxyARailway(context.request);
