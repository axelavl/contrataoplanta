/* ═══════════════════════════════════════════════════════════════
   web/boot.js — bootstrap que debe ejecutarse ANTES del primer paint.

   Dos tareas:
   1. Aplicar el tema (claro/oscuro) guardado en localStorage o el
      preferido por el sistema. Sin esto hay un "flash" de tema claro
      antes de que el usuario con tema oscuro vea el suyo.
   2. Setear `html.js-nav` para que las reglas CSS que asumen JS
      activo (p.ej. ocultar los bloques SSR cuando hay cliente vivo)
      se apliquen desde el primer render.

   Se carga con `<script src="boot.js"></script>` sin `defer` ni
   `async`, bloqueando el parser un momento. El archivo es minúsculo
   (<1 KB), el costo es despreciable.

   Antes estaba en 2 `<script>` inline en el `<head>` — CSP enforce
   con `script-src 'self'` los bloquearía.
   =================================================================== */
(function () {
  try {
    var stored = localStorage.getItem('theme');
    var prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    document.documentElement.setAttribute(
      'data-theme',
      stored || (prefersDark ? 'dark' : 'light')
    );
  } catch (e) { /* localStorage deshabilitado, no bloquear */ }
  document.documentElement.classList.add('js-nav');
})();
