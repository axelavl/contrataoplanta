# Lista de ofertas a cerrar — consolidada y verificada

Generado 2026-06-21 cruzando la API de producción con las fuentes de origen en vivo (navegador). **239 IDs únicos a cerrar.** No se ejecutó nada sobre la base; el SQL al final es para que lo revises tú.

Esta versión corrige el lote 1 anterior: 4 ofertas que estaban en el grupo "URL muerta" resultaron ser concursos **vigentes** de Pucón (el validador da falso "muerto" en URLs WordPress con `#ancla`). Quedaron fuera del cierre.

## Composición de las 239

| Grupo | n | Método de verificación |
|---|---|---|
| A. Integra fuera del feed | 67 | Cruce contra el feed Aira en vivo (82 activas); las que ya no están se despublicaron |
| B. URL muerta + sin fecha (corregido) | 120 | `url_oferta_valida=false` y sin plazo, menos 4 falsos positivos de Pucón |
| C. Pucón histórico | 44 | Cruce por hash de ancla contra la sección "En Curso" del sitio (solo 8 vigentes) |
| D. Cobquecura no-empleos | 21 | El sitio muestra que son avisos de elecciones de organizaciones (Ley 19.418), no ofertas |
| **Total único (deduplicado)** | **239** | |

## Hallazgos clave de esta pasada

**Pucón (id 562).** La URL de origen es una sola página-listado (`?page_id=89`) con 8 concursos "En Curso" y 80 "Anteriores". El scraper ingirió el archivo histórico completo. De las 50 ofertas en la base, **6 son vigentes** (hash en "En Curso") y **44 son históricas**. Los hashes de ancla son estables y cruzan exactamente entre sitio y base, por eso el match es confiable.

**Cobquecura (id de inst.).** Las 21 ofertas (`/oficio-conductor-*`) **no son empleos**: son oficios administrativos que convocan a *elecciones de organizaciones comunitarias* (juntas de vecinos, centros de padres) bajo la Ley 19.418, fechados junio 2026. El clasificador las tomó como empleo porque "conductor" matcheó como cargo. Su página de concursos real, además, solo tiene histórico (lo más nuevo es "Concurso Público 2025"). → Cerrar las 21 y excluir "oficio conductor / elecciones de organizaciones" en el clasificador.

## NO cerrar — Pucón vigentes (6 IDs)

```
1060, 1061, 1062, 1063, 1064, 1065
```
Estos son los concursos "En Curso" de Pucón. Cuatro (1060, 1061, 1063, 1065) están marcados con URL muerta por error — conviene revalidar su `url_oferta_valida` en vez de cerrarlos. Lo correcto es que el scraper les capture la fecha desde el PDF de bases.

## IDs a cerrar (239)

```
246,248,250,265,407,408,409,458,479,481,492,513,526,527,529,532,533,534,536,538,539,542,543,544,546,548,551,553,554,557,558,560,563,564,651,727,733,742,748,751,768,778,786,797,827,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,892,897,898,899,900,969,973,981,1020,1068,1069,1070,1072,1073,1077,1079,1081,1083,1085,1086,1088,1089,1090,1092,1096,1099,1102,1104,1106,1107,1109,1111,1112,1114,1116,1118,1120,1121,1133,1134,1136,1137,1147,1151,1156,1253,1260,1263,1265,1268,1270,1272,1276,1301,1316,1317,1330,1331,1333,1356,1357,1360,1369,1373,1375,1376,1380,1388,1419,1420,1421,1428,1456,1459,1460,1461,1462,1463,1484,1486,1491,1492,1514,4416,4417,4418,4421,4422,4424,4429,4436,4439,4469,4484,5256,5458,5460,5461,5743,5745,7214,7360,7669,18519,18522,18528,18529,18531,18532,18534,18537,18539,18541,18542,18545,18546,18548,18550,18562,18563,18566,18567,18568,18569,18570,18571,18572,18573,18574,18575,18576,18577,18578,18579,18580,18581,18582,18583,18584,18586,18587,18589,18590,18593,18599,18602,18603,18604,18605,18606,18607,18608,18609,18610,18612,18613,18614,18615,18626,18627,18628,18629,18630,19222,19224,22145,22147,22148,22429,22430
```

## SQL sugerido (revisar antes de ejecutar)

Cierre reversible (no borra). Recomiendo revalidar las URLs justo antes, y NO incluir los 6 IDs de Pucón vigentes (ya están excluidos de esta lista).

```sql
UPDATE ofertas SET activa = FALSE WHERE id IN (
246,248,250,265,407,408,409,458,479,481,492,513,526,527,529,532,533,534,536,538,539,542,543,544,546,548,551,553,554,557,558,560,563,564,651,727,733,742,748,751,768,778,786,797,827,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,892,897,898,899,900,969,973,981,1020,1068,1069,1070,1072,1073,1077,1079,1081,1083,1085,1086,1088,1089,1090,1092,1096,1099,1102,1104,1106,1107,1109,1111,1112,1114,1116,1118,1120,1121,1133,1134,1136,1137,1147,1151,1156,1253,1260,1263,1265,1268,1270,1272,1276,1301,1316,1317,1330,1331,1333,1356,1357,1360,1369,1373,1375,1376,1380,1388,1419,1420,1421,1428,1456,1459,1460,1461,1462,1463,1484,1486,1491,1492,1514,4416,4417,4418,4421,4422,4424,4429,4436,4439,4469,4484,5256,5458,5460,5461,5743,5745,7214,7360,7669,18519,18522,18528,18529,18531,18532,18534,18537,18539,18541,18542,18545,18546,18548,18550,18562,18563,18566,18567,18568,18569,18570,18571,18572,18573,18574,18575,18576,18577,18578,18579,18580,18581,18582,18583,18584,18586,18587,18589,18590,18593,18599,18602,18603,18604,18605,18606,18607,18608,18609,18610,18612,18613,18614,18615,18626,18627,18628,18629,18630,19222,19224,22145,22147,22148,22429,22430
);
```

## Pendiente (siguiente lote)

- **Los Álamos (9)** y la **cola larga de ~65 municipios** (1-4 ofertas c/u, ~110 ofertas live restantes): cada sitio es distinto (anclas vs páginas separadas), así que el cruce es manual por sitio. Mejor ROI: arreglar el scraper en vez de revisar 65 sitios a mano.
- **Backfill de fechas (no cerrar):** Integra 82 vigentes, CODELCO 7, Tribunal Constitucional 9 — la fecha existe en origen.

## Correcciones de scraper/clasificador que esto implica

1. **WordPress municipal:** no ingerir la sección "Anteriores"/archivo; respetar "En Curso", o caducar por antigüedad. (Pucón es el caso testigo.)
2. **Clasificador:** excluir avisos de elecciones de organizaciones / "oficio conductor" Ley 19.418 (Cobquecura). No son empleos.
3. **Validador de URLs:** las URLs WordPress con `#ancla` dan falso "muerto"; revisar la lógica para no marcar como caídos concursos vigentes.
