# Validación de datos estructurados (JSON-LD)

Este documento define cómo evitar marcado inválido en `web/index.html` y cómo revisar resultados en **Rich Results Test**.

## Bloques JSON-LD implementados

- `Organization`: identidad principal del sitio.
- `WebSite` + `SearchAction`: búsqueda interna en `/?q=...`.
- `JobPosting` (dinámico): se inyecta por oferta **solo si pasa validación mínima**.

## Reglas para `JobPosting` (inyección condicional)

Se **omite** el `JobPosting` si falta cualquiera de estos campos mínimos:

1. `cargo` → `title`.
2. `institucion` → `hiringOrganization.name`.
3. `fecha_cierre` válida en formato `YYYY-MM-DD` → `validThrough`.
4. `ubicación` mínima (`ciudad` o `region`) → `jobLocation.address`.
5. `url_oferta` absoluta `http/https` → `url`.

Si no cumple, se registra un `console.debug` para facilitar revisión manual.

## Campos opcionales recomendados

Cuando existan, se agregan:

- `fecha_publicacion` válida → `datePosted` (si no existe, se usa `fecha_cierre` como fallback técnico).
- `renta_bruta_min` > 0 → `baseSalary`.
- `jornada` → `workHours`.
- `tipo_contrato` → `employmentType`.
- `descripcion` → `description`.

## Revisión manual (Rich Results Test)

1. Levantar el front y abrir una página de resultados con ofertas reales.
2. Copiar la URL pública o, para revisión local, copiar el HTML renderizado.
3. Probar en: <https://search.google.com/test/rich-results>.
4. Confirmar:
   - que `Organization` y `WebSite` se detecten sin errores;
   - que los `JobPosting` aparezcan solo en ofertas con datos mínimos válidos;
   - que no existan errores críticos de campos obligatorios.

## Checklist rápido de release

- [ ] Verificar en DevTools que no haya `JobPosting` con campos vacíos.
- [ ] Ejecutar Rich Results Test sobre una URL con resultados reales.
- [ ] Corregir mapping si cambia el contrato de la API (`cargo`, `institucion`, `fecha_cierre`, `ciudad/region`, `url_oferta`).
