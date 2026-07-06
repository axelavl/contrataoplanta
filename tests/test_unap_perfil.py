"""UNAP: asociación del PDF 'Perfil' a cada cargo.

Antes se asignaba por índice en una lista plana; un bloque saltado o un perfil
extra desalineaba los índices y adjuntaba el descriptor de OTRA oferta. Ahora se
asocia por código (robusto) y el fallback posicional sólo aplica si hay 1:1.
"""
import scrapers.universidades_portal as U


def _items(*codigos):
    return [{"codigo": c, "perfil": None} for c in codigos]


def test_perfil_por_codigo_aunque_el_orden_difiera():
    items = _items("USA1201", "USA1305")
    perfiles = ["/docs_perfiles/perfil_USA1305.pdf", "/docs_perfiles/perfil_USA1201.pdf"]
    U._asignar_perfiles_unap(items, perfiles)
    assert items[0]["perfil"].endswith("USA1201.pdf")
    assert items[1]["perfil"].endswith("USA1305.pdf")


def test_no_adjunta_perfil_equivocado_si_cantidades_difieren():
    # 3 cargos, 1 perfil sin código coincidente → nadie recibe un PDF ajeno.
    items = _items("USA1", "USA2", "USA3")
    U._asignar_perfiles_unap(items, ["/x/otro.pdf"])
    assert all(it["perfil"] is None for it in items)


def test_fallback_posicional_solo_si_uno_a_uno():
    items = _items(None, None)
    U._asignar_perfiles_unap(items, ["/a.pdf", "/b.pdf"])
    assert items[0]["perfil"] == "/a.pdf"
    assert items[1]["perfil"] == "/b.pdf"
