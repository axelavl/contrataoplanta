# Logos institucionales reales

Esta carpeta guarda **logos oficiales** de instituciones, servidos como
`/logos/<dominio>.png`. Son la primera opción de la cadena de logos del sitio
(ver `web/shared-shell.js` → `sourcesFor`).

**No se usan logos dibujados a mano.** Si una institución no tiene un PNG aquí,
el sitio intenta su favicon oficial (DuckDuckGo → Google → apple-touch-icon) y,
si tampoco hay, muestra el ícono genérico del sector. Así sólo se ven logos
reales.

## Cómo agregar un logo real

1. Nombra el archivo con el **dominio** de la institución y extensión `.png`:
   - `carabineros.cl.png`
   - `armada.cl.png`
   - `pdichile.cl.png`
   El dominio es el que resuelve el sitio para esa institución (su sitio web
   oficial, sin `www.`).
2. Usa un PNG cuadrado, fondo transparente o blanco, idealmente 128×128 px o más.
3. Colócalo en esta carpeta y despliega. Tiene prioridad automática sobre el
   favicon.

No hace falta tocar código: `getInstIcon` (en `web/app.js`) ya usa
`/logos/<dominio>.png` como fuente primaria.
