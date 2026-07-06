"""Búsqueda robusta: raíces sin tildes, con variación de género (o/a) y match
también en requisitos/descripción, no sólo en el título.

Regresión: buscar un TÍTULO como "administrador público" devolvía muy pocos
resultados porque el término suele estar en los requisitos del aviso, no en el
cargo, y la búsqueda sólo miraba cargo + institución.
"""
from api.services.sql import (
    _ACCENT_FROM,
    _ACCENT_TO,
    _query_roots,
    build_cargo_relevance,
    build_ofertas_filters,
)


def _fold(s: str) -> str:
    return (s or "").translate(str.maketrans(_ACCENT_FROM, _ACCENT_TO)).lower()


def _matchea(q: str, *campos: str) -> bool:
    """Replica la semántica del WHERE: TODAS las raíces del query deben aparecer
    (folded, substring) en el campo combinado."""
    combinado = _fold(" ".join(campos))
    roots = _query_roots(q)
    return all(r in combinado for r in roots) if roots else _fold(q) in combinado


def _tier(q: str, cargo: str) -> int:
    """Replica build_cargo_relevance: 0 frase en cargo, 1 todas las raíces en
    cargo, 2 matcheó fuera del cargo."""
    fc = _fold(cargo)
    if _fold(q) in fc:
        return 0
    roots = _query_roots(q)
    if roots and all(r in fc for r in roots):
        return 1
    return 2


# ── Raíces / variación de género ─────────────────────────────────────────────
def test_roots_administrador_publico():
    assert _query_roots("administrador público") == ["administr", "publi"]


def test_variacion_genero_o_a():
    # La raíz "administr" cubre administrador/administradora/administración.
    for variante in ("Administrador Público", "Administradora Pública",
                     "Administración Pública"):
        assert _matchea("administrador publico", "Cargo genérico", "", variante)


def test_tilde_insensible():
    assert _matchea("administración", "Administracion Municipal")
    assert _matchea("administracion", "Administración Municipal")


# ── El título pedido en REQUISITOS ahora matchea ─────────────────────────────
def test_titulo_en_requisitos_matchea():
    cargo = "Profesional de Apoyo Social"
    requisitos = "Se requiere título de Administrador Público o Ingeniero Comercial."
    assert _matchea("administrador publico", cargo, "", "", requisitos, "")


def test_titulo_en_descripcion_matchea():
    cargo = "Encargado de Finanzas"
    descripcion = "Profesional del área, idealmente Administrador Público o Contador Auditor."
    assert _matchea("administrador publico", cargo, "", "", "", descripcion)


def test_no_matchea_si_falta_una_raiz():
    # "administrador" aparece pero "publico" no → no debe matchear (AND de raíces).
    cargo = "Administrador de Bodega"
    requisitos = "Manejo de inventario y logística."
    assert not _matchea("administrador publico", cargo, "", "", requisitos, "")


# ── El WHERE realmente incluye los campos nuevos ─────────────────────────────
def test_where_incluye_requisitos_y_descripcion():
    where, params = build_ofertas_filters(q="administrador publico")
    assert "o.requisitos" in where
    assert "o.requisitos_texto" in where   # AMBAS columnas de requisitos
    assert "o.descripcion" in where
    assert "o.area_profesional" in where
    assert where.count("%s") == len(params)


def test_titulo_en_requisitos_texto_matchea():
    # El título vive en `requisitos_texto` mientras `requisitos` trae otra cosa:
    # con COALESCE se perdía; concatenando ambas ahora matchea.
    cargo = "Profesional de Apoyo"
    requisitos = "Disponibilidad inmediata."
    requisitos_texto = "Título profesional de Administrador Público o afín."
    assert _matchea("administrador publico", cargo, "", "", requisitos, requisitos_texto, "")


# ── Ranking: título exacto primero, requisitos después ───────────────────────
def test_ranking_gradiente():
    q = "administrador publico"
    assert _tier(q, "Administrador Público Regional") == 0     # frase en el cargo
    assert _tier(q, "Administrador de Sistemas Público") == 1  # ambas raíces, no frase
    assert _tier(q, "Profesional de Apoyo") == 2               # matcheó por requisitos


def test_cargo_relevance_balancea_params():
    rel, params = build_cargo_relevance("administrador publico")
    assert rel.count("%s") == len(params)
    # 1 (frase) + 2 (raíces) = 3 parámetros.
    assert len(params) == 3


# ── Excluir empleospublicos: el "%" del LIKE va en parámetro, no en el SQL ────
# Regresión del HTTP 500 al aplicar "Sin empleospublicos": el patrón LIKE estaba
# embebido en la cadena ('%empleospublicos.cl%'). psycopg2 usa "%" como marcador
# de placeholders, así que ese "%" literal rompía execute() con
# "not enough arguments for format string".
def _sin_porcentaje_suelto(sql: str) -> bool:
    """True si TODO '%' en el SQL forma parte de un placeholder '%s'."""
    import re
    return not re.search(r"%(?!s)", sql)


def test_excluir_empleos_publicos_parametriza_el_like():
    where, params = build_ofertas_filters(excluir_empleos_publicos=True)
    assert "empleospublicos" not in where          # el dominio NO va embebido
    assert "%empleospublicos.cl%" in params        # va como parámetro
    assert where.count("%s") == len(params)         # balanceado
    assert _sin_porcentaje_suelto(where)            # sin "%" suelto que rompa psycopg2


def test_ningun_filtro_deja_porcentaje_suelto_en_el_sql():
    # Combinación amplia de filtros: ningún '%' literal debe quedar en el SQL.
    where, params = build_ofertas_filters(
        q="administrador",
        region="Metropolitana",
        sector="Salud",
        tipo="Contrata,Planta",
        excluir_empleos_publicos=True,
        solo_con_correo=True,
    )
    assert where.count("%s") == len(params)
    assert _sin_porcentaje_suelto(where)
