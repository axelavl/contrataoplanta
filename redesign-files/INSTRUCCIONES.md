# Instrucciones para agente de código

Tengo 7 archivos listos en esta carpeta (`redesign-files/`) que debes aplicar al repo `contrataoplanta`. Son mejoras del rediseño visual: truncado inteligente de títulos, generación de iconos SVG por sector cuando no hay logo, rediseño del menú compartir, y varios fixes de UX/dark mode.

## Pasos a ejecutar

Trabaja en la rama `redesign/v3-home`. Asume que estás en la raíz del repo (`C:\proyectos\empleoestado` o equivalente).

### 1. Crear 4 archivos nuevos

Copiar estos archivos desde `redesign-files/` a `web/`:

- `redesign-files/title-truncate.js` → `web/title-truncate.js`
- `redesign-files/share-mejoras.js` → `web/share-mejoras.js`
- `redesign-files/plazo-colors.js` → `web/plazo-colors.js`
- `redesign-files/logo-fallback.js` → `web/logo-fallback.js`

En PowerShell:
```powershell
Copy-Item redesign-files/title-truncate.js web/title-truncate.js -Force
Copy-Item redesign-files/share-mejoras.js web/share-mejoras.js -Force
Copy-Item redesign-files/plazo-colors.js web/plazo-colors.js -Force
Copy-Item redesign-files/logo-fallback.js web/logo-fallback.js -Force
```

### 2. Reemplazar 3 archivos existentes

- `redesign-files/shared-shell.js` → `web/shared-shell.js` (reemplazar completo)
- `redesign-files/redesign-overrides.css` → `web/styles/redesign-overrides.css` (reemplazar completo)
- `redesign-files/favicon.svg` → `web/favicon.svg` (reemplazar completo)

En PowerShell:
```powershell
Copy-Item redesign-files/shared-shell.js web/shared-shell.js -Force
Copy-Item redesign-files/redesign-overrides.css web/styles/redesign-overrides.css -Force
Copy-Item redesign-files/favicon.svg web/favicon.svg -Force
```

### 3. Verificar que la CI local pasa

```powershell
python scripts/qa/check_accessibility_basics.py
```

Debería decir: `OK: checks de accesibilidad mínima completados.`

### 4. Git commit y push

```powershell
git add web/title-truncate.js web/share-mejoras.js web/plazo-colors.js web/logo-fallback.js web/shared-shell.js web/styles/redesign-overrides.css web/favicon.svg
git status
git commit -m "feat(home): titulos con truncado inteligente + share rediseñado + logos inline"
git push origin redesign/v3-home
```

### 5. Limpiar la carpeta auxiliar

Una vez pusheado:
```powershell
Remove-Item redesign-files -Recurse -Force
```

Y asegurarse de que no se hayan staged archivos no deseados:
```powershell
git status
```

Si hay archivos modificados que no deberían estar (por line endings, etc.), NO los agregues al commit. Solo los 7 de arriba.

### 6. Abrir PR

URL para abrir el PR en el navegador:
```
https://github.com/axelavl/contrataoplanta/compare/main...redesign/v3-home
```

Título sugerido: `feat(home): titulos con truncado inteligente + share rediseñado + logos inline`

Descripción sugerida (pegar en el PR):

```markdown
## Cambios

### 1. Títulos con truncado inteligente (`title-truncate.js`)
Detecta separadores naturales (` - `, ` — `, `(`, `. `, `, `, `: `) y corta el título en el primero que aparezca entre posición 20-120. Fallback: máx 90 chars al último espacio + `…`. El título completo queda en `data-full-title` y tooltip. Aplica al modal-cargo y a `.oferta-cargo` en cards, con MutationObserver para re-aplicar al filtrar o abrir otra oferta.

### 2. Logos institucionales garantizados (`shared-shell.js` + `logo-fallback.js`)
El override de `window.imgFavFallback` se registra inline al TOP de `shared-shell.js` (defer, antes de DOMContentLoaded) para que esté activo desde el primer error de imagen. Cascada:
1. Clearbit (128px)
2. DuckDuckGo icons
3. Google favicons (128px)
4. `/apple-touch-icon.png` del dominio oficial
5. `/favicon.ico` del dominio oficial
6. SVG genérico por sector (municipal, salud, universidad, ministerio, judicial, FFAA, regional, empresa)

### 3. Menú compartir rediseñado (`share-mejoras.js`)
- Se quita el generador de imagen (modal feo)
- Botón IG ahora copia enlace con toast "🔗 Link copiado — pégalo en tu IG"
- Botón Copiar ahora copia "Título — Institución · URL" (antes solo URL)
- Nuevo botón Email con `mailto:` pre-llenado
- Orden: WhatsApp · LinkedIn · Email · Instagram · Copiar
- Todos alineados en círculos 40×40 con hover por canal (verde WA, azul LI, etc.)

### 4. Plazos con colores progresivos (`plazo-colors.js`)
- 0-2 días: rojo + border-left grueso + peso bold
- 3-5 días: ámbar
- 6+ días: verde sage
- Cerradas: gris con opacidad

### 5. CSS: modal más ancho, toast, line-clamp, etc.
- `max-width: 820px` para el modal (antes 680)
- Toast flotante para feedback de copiar
- Cards con `line-clamp: 2`
- Logo container con fondo navy-tint

### 6. Favicon actualizado
Logo "o" itálica dorada (coherente con la marca actual).

## CI
`check_accessibility_basics.py` pasa OK localmente.

## No cambia
- Buscador, filtros, comunas selector, autocompletado
- API, scrapers
- Lógica de favoritos
```

Clic en **Create pull request** → esperar CI verde → Merge.

---

## Archivos en esta carpeta

| Archivo | Tamaño | Descripción |
|---|---|---|
| `title-truncate.js` | 3.3 KB | Truncado inteligente de títulos |
| `share-mejoras.js` | 5.9 KB | Rediseño menú compartir + email + toast |
| `plazo-colors.js` | 2.5 KB | Colores progresivos de plazo |
| `logo-fallback.js` | 4.7 KB | Logos con 5 fuentes + SVG genérico |
| `shared-shell.js` | 7.9 KB | Shell con logo-fallback inline |
| `redesign-overrides.css` | 57 KB | Todos los estilos del rediseño |
| `favicon.svg` | 444 B | Favicon nuevo |

Total: ~82 KB.
