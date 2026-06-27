/* ──────────────────────────────────────────────────────────────────────────
   ads-config.js · Configuración central de publicidad (AdSense)
   ──────────────────────────────────────────────────────────────────────────
   Cómo activar:
     1. Crea tu cuenta en Google AdSense y obtén tu "client" (ca-pub-XXXX...).
     2. Crea unidades de anuncio y copia el "slot id" (número) de cada una.
     3. Rellena `client` y `slots`, y pon `enabled: true`.
     4. Asegúrate de que el CSP en `web/_headers` permita los dominios de Google
        (ver MONETIZACION_ADS_RRSS.md). Sin eso, el navegador bloquea AdSense.

   Mientras `enabled` sea false, NO se carga ningún script de terceros y los
   espacios quedan como placeholder (no se rompe nada).
   ────────────────────────────────────────────────────────────────────────── */
window.ADS_CONFIG = {
  enabled: true,                        // ← activo: carga el loader de AdSense
  client: 'ca-pub-9396873058904461',    // ← tu publisher ID de AdSense
  // Mapea cada ubicación del sitio a un "slot id" de AdSense:
  slots: {
    resultados: '',                     // banner bajo resultados (index)
    sidebar: '',                        // caja lateral (index)
    contenido: ''                       // dentro de páginas de contenido
  },
  // Mostrar un recuadro tenue "Espacio publicitario" cuando está desactivado.
  // Útil en desarrollo; en producción conviene dejarlo en false.
  mostrarPlaceholder: false
};
