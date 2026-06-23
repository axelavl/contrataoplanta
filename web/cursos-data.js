/* ──────────────────────────────────────────────────────────────────────────
   cursos-data.js · Directorio de cursos y especializaciones para el sector público
   ──────────────────────────────────────────────────────────────────────────
   Modelo de monetización: empresas de capacitación pagan por aparecer aquí.
   Niveles:
     - destacado  → tarjeta arriba, con sello "Destacado" (mayor precio).
     - estandar   → listado normal.
     - slot libre → "Anúnciate aquí" (espacio en venta).

   Las CATEGORÍAS están alineadas con las áreas que más demanda el sector
   público (ver panel-mercado.html), para que el aviso le llegue a la persona
   correcta en el momento que busca empleo en esa área.

   IMPORTANTE: los listados de abajo son EJEMPLOS de demostración (marcados con
   `demo:true`) para mostrar cómo se ve un aviso. No representan empresas reales
   ni implican relación comercial. Reemplazar por avisadores reales al vender.
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  // Categorías de especialización (slug → etiqueta + áreas de demanda que cubren).
  var categorias = [
    { slug: 'admin-publica',   etiqueta: 'Administración y gestión pública' },
    { slug: 'finanzas',        etiqueta: 'Finanzas y presupuesto público (SIGFE)' },
    { slug: 'compras',         etiqueta: 'Compras públicas (ChileCompra · Ley 19.886)' },
    { slug: 'derecho',         etiqueta: 'Derecho administrativo y probidad' },
    { slug: 'rrhh',            etiqueta: 'Recursos humanos del Estado' },
    { slug: 'salud',           etiqueta: 'Salud pública' },
    { slug: 'educacion',       etiqueta: 'Educación y párvulos' },
    { slug: 'ti',              etiqueta: 'TI y transformación digital del Estado' },
    { slug: 'prevencion',      etiqueta: 'Prevención de riesgos' },
    { slug: 'atencion',        etiqueta: 'Atención ciudadana' }
  ];

  // Avisos (ejemplos de demostración). Estructura de un aviso real:
  //   { id, titulo, proveedor, categoria, modalidad, duracion, nivel, url, descripcion, demo }
  var avisos = [
    {
      id: 'demo-1',
      titulo: 'Diplomado en Gestión Pública',
      proveedor: 'Institución de ejemplo',
      categoria: 'admin-publica',
      modalidad: 'Online',
      duracion: '120 h',
      nivel: 'destacado',
      url: '#',
      descripcion: 'Formación integral en administración del Estado, modernización y gestión de personas.',
      demo: true
    },
    {
      id: 'demo-2',
      titulo: 'Curso ChileCompra / Mercado Público (Ley 19.886)',
      proveedor: 'Institución de ejemplo',
      categoria: 'compras',
      modalidad: 'Online en vivo',
      duracion: '24 h',
      nivel: 'destacado',
      url: '#',
      descripcion: 'Operación de la plataforma de compras públicas, bases de licitación y normativa aplicable.',
      demo: true
    },
    {
      id: 'demo-3',
      titulo: 'Especialización en SIGFE y presupuesto público',
      proveedor: 'Institución de ejemplo',
      categoria: 'finanzas',
      modalidad: 'Online',
      duracion: '40 h',
      nivel: 'estandar',
      url: '#',
      descripcion: 'Ejecución presupuestaria, contabilidad gubernamental y reportería en SIGFE.',
      demo: true
    },
    {
      id: 'demo-4',
      titulo: 'Derecho administrativo y probidad (Ley 19.880 · 20.880)',
      proveedor: 'Institución de ejemplo',
      categoria: 'derecho',
      modalidad: 'Online',
      duracion: '30 h',
      nivel: 'estandar',
      url: '#',
      descripcion: 'Procedimiento administrativo, actos, plazos, probidad y transparencia.',
      demo: true
    },
    {
      id: 'demo-5',
      titulo: 'Transformación digital del Estado',
      proveedor: 'Institución de ejemplo',
      categoria: 'ti',
      modalidad: 'Online',
      duracion: '36 h',
      nivel: 'estandar',
      url: '#',
      descripcion: 'Gobierno digital, interoperabilidad y gestión de proyectos tecnológicos en el sector público.',
      demo: true
    }
  ];

  // Mapa: valor de `area_profesional` que entrega la API → categoría de curso.
  // Permite deep-link desde el panel de mercado (?area=salud) al directorio.
  var mapaAreas = {
    'salud': 'salud',
    'educacion': 'educacion',
    'educación': 'educacion',
    'administracion': 'admin-publica',
    'administración': 'admin-publica',
    'finanzas': 'finanzas',
    'contabilidad': 'finanzas',
    'juridica': 'derecho',
    'jurídica': 'derecho',
    'legal': 'derecho',
    'informatica': 'ti',
    'informática': 'ti',
    'tecnologia': 'ti',
    'tecnología': 'ti',
    'recursos humanos': 'rrhh',
    'prevencion de riesgos': 'prevencion',
    'prevención de riesgos': 'prevencion'
  };

  // Planes de aviso (precios REFERENCIALES en CLP/mes — ajustar al vender).
  // Pensados como punto de partida para negociar, no como tarifa publicada.
  var planes = [
    {
      id: 'destacado',
      nombre: 'Destacado',
      precioRef: 89000,
      periodo: 'mes',
      incluye: [
        'Posición superior con sello “Destacado”',
        'Aparece en todas las categorías afines',
        'Enlace seguible (sin nofollow) negociable',
        'Métrica mensual de clics'
      ]
    },
    {
      id: 'estandar',
      nombre: 'Estándar',
      precioRef: 39000,
      periodo: 'mes',
      incluye: [
        'Listado en su categoría',
        'Descripción, modalidad y duración',
        'Métrica mensual de clics'
      ]
    },
    {
      id: 'performance',
      nombre: 'Por desempeño',
      precioRef: null,
      periodo: 'CPC / CPL',
      incluye: [
        'Pago por clic o por lead generado',
        'Ideal para campañas puntuales',
        'Sin costo fijo mensual'
      ]
    }
  ];

  var contacto = {
    email: 'contacto@estadoemplea.cl', // ← reemplazar por correo real
    asuntoBase: 'Quiero anunciar un curso'
  };

  window.CURSOS_DATA = {
    categorias: categorias,
    avisos: avisos,
    mapaAreas: mapaAreas,
    planes: planes,
    contacto: contacto
  };
})();
