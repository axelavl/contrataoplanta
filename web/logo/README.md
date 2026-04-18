# Logo · contrata o planta

Paquete de variantes en SVG listas para producción.

## Archivos

| Archivo | Uso recomendado |
|---|---|
| `logo-primary.svg` | Mark principal (256×256) · redes, splash, branding general |
| `logo-favicon.svg` | Favicon 32×32 optimizado para tamaños pequeños |
| `logo-app-icon.svg` | App icon 1024×1024 para iOS/Android (sin rounding propio — el OS aplica máscara) |
| `logo-horizontal.svg` | Lockup con wordmark + tagline para headers, email signatures, facturas |
| `logo-mono-navy.svg` | Monocromo navy para fondos claros (impresión sin color, stamps) |
| `logo-mono-gold.svg` | Monocromo dorado para fondos oscuros |

## Tokens del mark

- **Azul fondo**: `#254BA0` (`--navy`)
- **Dorado trazo + punto**: `#F2C26A` (`--gold-brilliant`)
- **Skew itálico**: `-10°`
- **Radius del cuadrado (256)**: 64 (25%)
- **Radius del cuadrado (32)**: 6 (~19%, más proporcional en tamaños pequeños)

## Reglas de uso

1. **Área de respeto** mínimo: espacio libre alrededor = 25% del tamaño del mark.
2. **Tamaño mínimo de uso**: 20×20 px.
3. **No distorsionar**, no cambiar los colores fuera de las variantes provistas.
4. Sobre fondos oscuros, usar `logo-mono-gold.svg` o la variante con fondo navy sólido.
5. Para impresión en una sola tinta, usar las variantes monocromáticas.

## HTML para favicon

```html
<link rel="icon" type="image/svg+xml" href="/logo/logo-favicon.svg">
<link rel="apple-touch-icon" href="/logo/logo-app-icon.svg">
```
