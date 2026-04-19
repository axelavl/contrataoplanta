/* share-mejoras.js
   - Cambia el botón "Instagram" para que ya NO abra el generador de imagen.
     En su lugar copia el mensaje (título + URL) al portapapeles con toast
     "Link copiado, pégalo en tu IG".
   - Añade un botón de Email a la fila de compartir (orden: WA · LinkedIn
     · Email · IG · Copiar enlace).
   - El botón Copiar ahora copia "Título — Institución" + url.
*/
(function () {
  'use strict';

  function getOfertaFromModal() {
    var titulo = document.getElementById('modal-cargo');
    var inst = document.getElementById('modal-institucion');
    var linkInput = document.getElementById('share-link-input');
    var tituloTexto =
      (titulo && (titulo.dataset.fullTitle || titulo.textContent)) || 'Oferta laboral';
    var instTexto = (inst && inst.textContent) || 'Sector público';
    var url = (linkInput && linkInput.value) || window.location.href;
    return {
      titulo: tituloTexto.trim(),
      institucion: instTexto.trim(),
      url: url.trim()
    };
  }

  function buildShareText(o) {
    return o.titulo + ' — ' + o.institucion + ' · ' + o.url;
  }

  function showToast(message) {
    var toast = document.getElementById('share-toast');
    if (!toast) {
      toast = document.createElement('div');
      toast.id = 'share-toast';
      toast.className = 'share-toast';
      document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.classList.add('is-visible');
    clearTimeout(window.__shareToastTimer);
    window.__shareToastTimer = setTimeout(function () {
      toast.classList.remove('is-visible');
    }, 2500);
  }

  async function copyToClipboard(text) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (e) {
      try {
        var ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.left = '-9999px';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        return true;
      } catch (e2) {
        return false;
      }
    }
  }

  function addEmailButton(group) {
    if (group.querySelector('.share-btn--email')) return;
    var btn = document.createElement('a');
    btn.className = 'share-btn share-btn--email';
    btn.id = 'share-email';
    btn.title = 'Enviar por correo';
    btn.setAttribute('aria-label', 'Compartir por email');
    btn.target = '_blank';
    btn.rel = 'noopener';
    btn.href = '#';
    btn.innerHTML =
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
      '<path d="M4 4 h16 c1.1 0 2 .9 2 2 v12 c0 1.1-.9 2-2 2 H4 c-1.1 0-2-.9-2-2 V6 c0-1.1.9-2 2-2 z"/>' +
      '<polyline points="22,6 12,13 2,6"/>' +
      '</svg>';
    btn.addEventListener('click', function (e) {
      var o = getOfertaFromModal();
      var subject = encodeURIComponent('Oferta laboral: ' + o.titulo);
      var body = encodeURIComponent(o.titulo + '\n' + o.institucion + '\n\n' + o.url);
      btn.href = 'mailto:?subject=' + subject + '&body=' + body;
    });
    // Insertarlo después de LinkedIn
    var li = group.querySelector('.share-btn--li');
    if (li && li.nextSibling) {
      group.insertBefore(btn, li.nextSibling);
    } else {
      group.appendChild(btn);
    }
  }

  function patchIGButton(btn) {
    if (!btn || btn.dataset.igPatched === '1') return;
    btn.dataset.igPatched = '1';
    btn.setAttribute('title', 'Copiar enlace (pégalo en tu IG)');
    btn.setAttribute('aria-label', 'Copiar enlace para pegar en Instagram');
    // Remover onclick previo y añadir nuestro handler
    var newBtn = btn.cloneNode(true);
    btn.parentNode.replaceChild(newBtn, btn);
    newBtn.addEventListener('click', async function (e) {
      e.preventDefault();
      e.stopPropagation();
      var o = getOfertaFromModal();
      var ok = await copyToClipboard(buildShareText(o));
      showToast(ok ? '🔗 Link copiado — pégalo en tu IG' : 'No se pudo copiar');
    }, true);
  }

  function patchCopyButton(btn) {
    if (!btn || btn.dataset.copyPatched === '1') return;
    btn.dataset.copyPatched = '1';
    var newBtn = btn.cloneNode(true);
    btn.parentNode.replaceChild(newBtn, btn);
    newBtn.addEventListener('click', async function (e) {
      e.preventDefault();
      e.stopPropagation();
      var o = getOfertaFromModal();
      var ok = await copyToClipboard(buildShareText(o));
      showToast(ok ? '✓ Mensaje copiado (título + enlace)' : 'No se pudo copiar');
    }, true);
  }

  function reorderShareGroup(group) {
    // Orden final: WhatsApp · LinkedIn · Email · Instagram · Copiar
    var order = ['share-btn--wa', 'share-btn--li', 'share-btn--email', 'share-btn--ig', 'share-btn--copy'];
    order.forEach(function (cls) {
      var el = group.querySelector('.' + cls);
      if (el) group.appendChild(el);
    });
  }

  function enhance() {
    var group = document.querySelector('.modal-share-group');
    if (!group) return;
    addEmailButton(group);
    patchIGButton(group.querySelector('.share-btn--ig'));
    patchCopyButton(group.querySelector('.share-btn--copy'));
    reorderShareGroup(group);
  }

  function init() {
    enhance();
    // Re-aplicar cuando el modal se abre (su contenido cambia)
    var modal = document.getElementById('modal');
    if (modal) {
      var obs = new MutationObserver(function () {
        clearTimeout(window.__shareEnhanceTimer);
        window.__shareEnhanceTimer = setTimeout(enhance, 100);
      });
      obs.observe(modal, { childList: true, subtree: true, attributes: true, attributeFilter: ['class', 'open'] });
    }
  }

  document.addEventListener('shell:ready', init);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    setTimeout(init, 300);
  }
})();
