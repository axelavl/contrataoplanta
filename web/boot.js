/* ═══════════════════════════════════════════════════════════════
   web/boot.js — bootstrap que debe ejecutarse ANTES del primer paint.

   Dos tareas:
   1. Aplicar el tema (claro/oscuro). El DEFAULT del sitio es CLARO: sólo
      se usa oscuro si el usuario lo eligió explícitamente (guardado en
      localStorage). NO se sigue la preferencia del sistema operativo, para
      que la primera visita siempre vea el tema claro de marca. Aplicarlo
      antes del paint evita el "flash" para quien tiene guardado el oscuro.
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
    // Default CLARO: sólo oscuro si el usuario lo guardó explícitamente.
    var stored = localStorage.getItem('theme');
    document.documentElement.setAttribute('data-theme', stored || 'light');
  } catch (e) {
    // localStorage deshabilitado: caemos al claro por defecto.
    document.documentElement.setAttribute('data-theme', 'light');
  }
  document.documentElement.classList.add('js-nav');
})();
