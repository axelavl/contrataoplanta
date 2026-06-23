/* ──────────────────────────────────────────────────────────────────────────
   cursos-data.js · Directorio de cursos y especializaciones para el sector público
   ──────────────────────────────────────────────────────────────────────────
   Contiene DOS tipos de entrada:
     1. Cursos GRATUITOS oficiales del Estado (gratuito:true). Capacitación real
        y sin costo de organismos públicos (AcademiaCEA/Contraloría, ChileCompra,
        Servicio Civil, SUBDERE). Las URLs apuntan a la plataforma del organismo;
        casi todas requieren Clave Única.
     2. Avisos pagados (modelo de monetización): empresas de capacitación que
        pagan por aparecer. Niveles: destacado (sello, arriba) / estandar.

   Las CATEGORÍAS están alineadas con las áreas que más demanda el sector
   público (ver panel-mercado.html).

   NOTA: para administrar este listado desde el panel admin se requiere mover
   estos datos a una tabla en la DB + endpoints CRUD (ver docs). Mientras tanto,
   esta es la fuente de verdad y se edita acá.
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

  // Estructura de un aviso:
  //   { id, titulo, proveedor, categoria, modalidad, duracion, nivel, url,
  //     descripcion, gratuito, demo }
  var avisos = [
    // ── AcademiaCEA · Contraloría General de la República (CGR) ──────────────
    // Aula virtual gratuita, 100% online, requiere Clave Única.
    {
      id: 'cea-induccion-estado',
      titulo: 'Inducción General a la Administración del Estado',
      proveedor: 'AcademiaCEA · Contraloría (CGR)',
      categoria: 'admin-publica',
      modalidad: 'Online · Clave Única',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://www.ceacgr.cl/aulavirtual/',
      descripcion: 'Curso base sobre el Estado, sus instituciones, principios y el rol del funcionario público. Punto de partida ideal para quien recién ingresa.',
      gratuito: true,
      demo: false
    },
    {
      id: 'cea-estatuto-administrativo',
      titulo: 'Estatuto Administrativo',
      proveedor: 'AcademiaCEA · Contraloría (CGR)',
      categoria: 'rrhh',
      modalidad: 'Online · Clave Única',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://www.ceacgr.cl/aulavirtual/',
      descripcion: 'Derechos, deberes, carrera funcionaria, calidades jurídicas (planta, contrata) y régimen disciplinario del personal del Estado.',
      gratuito: true,
      demo: false
    },
    {
      id: 'cea-derecho-administrativo',
      titulo: 'Derecho Administrativo',
      proveedor: 'AcademiaCEA · Contraloría (CGR)',
      categoria: 'derecho',
      modalidad: 'Online · Clave Única',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://www.ceacgr.cl/aulavirtual/',
      descripcion: 'Acto administrativo, procedimiento (Ley 19.880), plazos, notificaciones y control de la legalidad.',
      gratuito: true,
      demo: false
    },
    {
      id: 'cea-etica-transparencia-lobby',
      titulo: 'Ética, Probidad, Transparencia y Lobby',
      proveedor: 'AcademiaCEA · Contraloría (CGR)',
      categoria: 'derecho',
      modalidad: 'Online · Clave Única',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://www.ceacgr.cl/aulavirtual/',
      descripcion: 'Probidad administrativa, conflictos de interés, transparencia activa y Ley del Lobby. Anticorrupción en el ejercicio del cargo.',
      gratuito: true,
      demo: false
    },
    {
      id: 'cea-ley-karin',
      titulo: 'Ley Karin: prevención del acoso laboral y sexual',
      proveedor: 'AcademiaCEA · Contraloría (CGR)',
      categoria: 'rrhh',
      modalidad: 'Online · Clave Única',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://www.ceacgr.cl/aulavirtual/',
      descripcion: 'Marco de la Ley Karin, prevención del acoso y la violencia en el trabajo, y procedimientos de denuncia en el Estado.',
      gratuito: true,
      demo: false
    },
    {
      id: 'cea-ciberseguridad',
      titulo: 'Ciberseguridad en el sector público',
      proveedor: 'AcademiaCEA · Contraloría (CGR)',
      categoria: 'ti',
      modalidad: 'Online · Clave Única',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://www.ceacgr.cl/aulavirtual/',
      descripcion: 'Buenas prácticas de seguridad de la información, gestión de riesgos y protección de datos en instituciones del Estado.',
      gratuito: true,
      demo: false
    },
    {
      id: 'cea-administracion-financiera',
      titulo: 'Administración Financiera del Estado',
      proveedor: 'AcademiaCEA · Contraloría (CGR)',
      categoria: 'finanzas',
      modalidad: 'Online · Clave Única',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://www.ceacgr.cl/aulavirtual/',
      descripcion: 'Presupuesto público, contabilidad gubernamental (NICSP) y rendición de cuentas en el sector público.',
      gratuito: true,
      demo: false
    },
    {
      id: 'cea-induccion-municipal',
      titulo: 'Inducción para el Sector Municipal',
      proveedor: 'AcademiaCEA · Contraloría (CGR)',
      categoria: 'admin-publica',
      modalidad: 'Online · Clave Única',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://www.ceacgr.cl/aulavirtual/',
      descripcion: 'Funcionamiento municipal, competencias del municipio y particularidades del régimen aplicable a su personal.',
      gratuito: true,
      demo: false
    },

    // ── ChileCompra · Mercado Público ───────────────────────────────────────
    // Certificación de competencias 100% online y gratuita, con Clave Única.
    {
      id: 'chilecompra-cert-basico',
      titulo: 'Certificación en Compras Públicas — Nivel Básico',
      proveedor: 'ChileCompra · Mercado Público',
      categoria: 'compras',
      modalidad: 'Online · Clave Única',
      duracion: '36 h',
      nivel: 'estandar',
      url: 'https://www.chilecompra.cl/certificacion/',
      descripcion: 'Normativa (Ley 19.886), uso de la plataforma Mercado Público y ética en las compras. Para operar tareas diarias de abastecimiento.',
      gratuito: true,
      demo: false
    },
    {
      id: 'chilecompra-cert-intermedio',
      titulo: 'Certificación en Compras Públicas — Nivel Intermedio',
      proveedor: 'ChileCompra · Mercado Público',
      categoria: 'compras',
      modalidad: 'Online · Clave Única',
      duracion: '44 h',
      nivel: 'estandar',
      url: 'https://www.chilecompra.cl/certificacion/',
      descripcion: 'Evaluación de ofertas y gestión de contratos, para supervisores y gestores de contratos del Estado.',
      gratuito: true,
      demo: false
    },
    {
      id: 'chilecompra-cert-avanzado',
      titulo: 'Certificación en Compras Públicas — Nivel Avanzado',
      proveedor: 'ChileCompra · Mercado Público',
      categoria: 'compras',
      modalidad: 'Online · Clave Única',
      duracion: '59 h',
      nivel: 'estandar',
      url: 'https://www.chilecompra.cl/certificacion/',
      descripcion: 'Abastecimiento estratégico y análisis de datos para compras de alta complejidad. Para abogados, auditores y administradores.',
      gratuito: true,
      demo: false
    },
    {
      id: 'chilecompra-capacitacion',
      titulo: 'Cursos gratuitos para compradores y proveedores del Estado',
      proveedor: 'ChileCompra · Mercado Público',
      categoria: 'compras',
      modalidad: 'Online · Clave Única',
      duracion: 'Todo el año',
      nivel: 'estandar',
      url: 'https://capacitacion.chilecompra.cl/',
      descripcion: 'Catálogo permanente de cursos y charlas online y gratuitas sobre la operación de Mercado Público.',
      gratuito: true,
      demo: false
    },

    // ── Campus · Dirección Nacional del Servicio Civil ──────────────────────
    {
      id: 'serviciocivil-gestion-personas',
      titulo: 'Gestión y Desarrollo de Personas en el Estado',
      proveedor: 'Campus · Servicio Civil',
      categoria: 'rrhh',
      modalidad: 'Online',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://campus.serviciocivil.cl/',
      descripcion: 'Formación transversal para servidores públicos en gestión y desarrollo de personas, con mirada de Estado.',
      gratuito: true,
      demo: false
    },
    {
      id: 'serviciocivil-inclusion',
      titulo: 'Inclusión laboral y gestión de la diversidad',
      proveedor: 'Campus · Servicio Civil',
      categoria: 'rrhh',
      modalidad: 'Online',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://campus.serviciocivil.cl/',
      descripcion: 'Inclusión de personas con discapacidad y gestión inclusiva de la diversidad en los servicios públicos.',
      gratuito: true,
      demo: false
    },
    {
      id: 'serviciocivil-capacitacion',
      titulo: 'Fundamentos para la Gestión de la Capacitación',
      proveedor: 'Campus · Servicio Civil',
      categoria: 'rrhh',
      modalidad: 'Online',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://campus.serviciocivil.cl/',
      descripcion: 'Cómo planificar, ejecutar y evaluar la capacitación en un servicio público.',
      gratuito: true,
      demo: false
    },

    // ── Academia de Capacitación Municipal y Regional · SUBDERE ─────────────
    {
      id: 'subdere-municipal-regional',
      titulo: 'Formación para funcionarios/as municipales y regionales',
      proveedor: 'Academia SUBDERE',
      categoria: 'admin-publica',
      modalidad: 'Online',
      duracion: '20–40 h',
      nivel: 'estandar',
      url: 'https://academia.subdere.gov.cl/',
      descripcion: 'Diplomados y cursos gratuitos para personal de municipios y gobiernos regionales, dictados por universidades acreditadas.',
      gratuito: true,
      demo: false
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
