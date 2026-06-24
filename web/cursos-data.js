/* ──────────────────────────────────────────────────────────────────────────
   cursos-data.js · Directorio de cursos y especializaciones para el sector público
   ──────────────────────────────────────────────────────────────────────────
   Es el FALLBACK del directorio: si la API (`GET /api/cursos`) no responde,
   `cursos.js` usa esta lista. La fuente de verdad en producción es la tabla
   `cursos` (gestionable desde el panel admin).

   Entradas:
     1. Cursos GRATUITOS oficiales del Estado (gratuito:true). Capacitación real
        de organismos públicos. Casi todas requieren Clave Única, y el enlace
        apunta al catálogo del organismo (los cursos puntuales se abren tras
        iniciar sesión).
     2. Avisos pagados (monetización): empresas que pagan por aparecer.
        Niveles: destacado (sello, arriba) / estandar.
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

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
    { slug: 'atencion',        etiqueta: 'Atención ciudadana' },
    { slug: 'otros',           etiqueta: 'Otros' }
  ];

  //   { id, titulo, proveedor, categoria, modalidad, duracion, nivel, url,
  //     descripcion, gratuito, demo }
  var avisos = [
    // ── AcademiaCEA · Contraloría General de la República (CGR) ──────────────
    {
      id: 'cea-induccion-estado',
      titulo: 'Inducción General a la Administración del Estado',
      proveedor: 'AcademiaCEA · Contraloría (CGR)',
      categoria: 'admin-publica',
      modalidad: 'Online · Clave Única',
      duracion: 'A tu ritmo',
      nivel: 'estandar',
      url: 'https://www.ceacgr.cl/aulavirtual/course/index.php',
      descripcion: 'Si recién entras al Estado, parte por acá: cómo se organiza la administración pública y qué se espera de un funcionario. Es el curso con el que la propia Contraloría pone a todos en la misma página.',
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
      url: 'https://www.ceacgr.cl/aulavirtual/course/index.php',
      descripcion: 'El reglamento que rige tu día a día en el Estado: ingreso, deberes y derechos, permisos, calificaciones y la carrera funcionaria. Casi obligado si eres planta o contrata.',
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
      url: 'https://www.ceacgr.cl/aulavirtual/course/index.php',
      descripcion: 'Cómo actúa legalmente el Estado: el acto administrativo, los plazos de la Ley 19.880, las notificaciones y el control de Contraloría. Te ahorra tropezar con la forma.',
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
      url: 'https://www.ceacgr.cl/aulavirtual/course/index.php',
      descripcion: 'Dónde está la raya: probidad, conflictos de interés, qué se publica por transparencia y cómo se registran las reuniones de lobby. La base para cuidar tu cargo.',
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
      url: 'https://www.ceacgr.cl/aulavirtual/course/index.php',
      descripcion: 'Qué cambió con la Ley Karin y qué hacer frente al acoso o la violencia en el trabajo público: prevención, canales de denuncia y el rol de cada quien.',
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
      url: 'https://www.ceacgr.cl/aulavirtual/course/index.php',
      descripcion: 'Para no ser el eslabón débil: contraseñas, phishing, manejo de datos y resguardo de la información en un servicio público. Sin tecnicismos innecesarios.',
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
      url: 'https://www.ceacgr.cl/aulavirtual/course/index.php',
      descripcion: 'Cómo se mueve la plata del Estado: presupuesto, contabilidad gubernamental (NICSP) y rendición de cuentas. Para quien trabaja en finanzas o quiere entenderlas de una vez.',
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
      url: 'https://www.ceacgr.cl/aulavirtual/course/index.php',
      descripcion: 'La versión municipal de la inducción: cómo funciona un municipio, qué puede y qué no, y las reglas propias del personal municipal.',
      gratuito: true,
      demo: false
    },

    // ── ChileCompra · Mercado Público ───────────────────────────────────────
    {
      id: 'chilecompra-cert-basico',
      titulo: 'Certificación en Compras Públicas — Nivel Básico',
      proveedor: 'ChileCompra · Mercado Público',
      categoria: 'compras',
      modalidad: 'Online · Clave Única',
      duracion: '36 h',
      nivel: 'estandar',
      url: 'https://www.chilecompra.cl/certificacion/',
      descripcion: 'El primer escalón para trabajar con Mercado Público: la Ley 19.886, cómo se usa la plataforma y la ética en las compras. Si recién partes en abastecimiento, este es el tuyo.',
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
      descripcion: 'Para quien ya opera y quiere subir de nivel: evaluación de ofertas y administración de contratos. Pensado para supervisores y gestores de contrato.',
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
      descripcion: 'El nivel experto: abastecimiento estratégico y análisis de datos para compras complejas. Apunta a abogados, auditores y administradores de contrato.',
      gratuito: true,
      demo: false
    },
    {
      id: 'chilecompra-capacitacion',
      titulo: 'Cursos y charlas gratis sobre Mercado Público',
      proveedor: 'ChileCompra · Mercado Público',
      categoria: 'compras',
      modalidad: 'Online · Clave Única',
      duracion: 'Todo el año',
      nivel: 'estandar',
      url: 'https://capacitacion.chilecompra.cl/',
      descripcion: 'Aparte de la certificación, ChileCompra dicta charlas y cursos cortos online durante todo el año sobre cómo comprar y vender al Estado. Conviene mirarlo seguido.',
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
      descripcion: 'Gestión de personas con mirada de Estado: liderazgo, clima y desarrollo de equipos en el sector público. Transversal, sirvas donde sirvas.',
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
      descripcion: 'Cómo hacer del servicio público un lugar más inclusivo: discapacidad, diversidad y los ajustes que la ley exige. Muy útil para jefaturas y RR.HH.',
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
      descripcion: 'El cómo detrás de la capacitación: detectar necesidades, armar el plan y evaluar si de verdad sirvió. Para quienes gestionan formación en su servicio.',
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
      url: 'https://academia.subdere.gov.cl/?page_id=1880',
      descripcion: 'Diplomados y cursos gratis para funcionarios de municipios y gobiernos regionales, dictados por universidades acreditadas. Las becas salen por convocatoria; conviene estar atento al calendario del año.',
      gratuito: true,
      demo: false
    },

    // ── Salud pública (MINSAL · ISP) ────────────────────────────────────────
    {
      id: 'minsal-siad',
      titulo: 'Cursos a distancia del Ministerio de Salud (SIAD)',
      proveedor: 'MINSAL · Subsecretaría de Salud Pública',
      categoria: 'salud',
      modalidad: 'Online · Cuenta institucional',
      duracion: 'Varía por curso',
      nivel: 'estandar',
      url: 'https://siadsps.minsal.cl/course/',
      descripcion: 'La plataforma de formación a distancia del MINSAL para los equipos de salud: esterilización y desinfección, salud mental en emergencias y desastres, interculturalidad con población migrante y más. Acceso con tu cuenta institucional.',
      gratuito: true,
      demo: false
    },
    {
      id: 'isp-elearning',
      titulo: 'Cursos e-learning del Instituto de Salud Pública (ISP)',
      proveedor: 'Instituto de Salud Pública · ISP',
      categoria: 'salud',
      modalidad: 'Online',
      duracion: 'Varía por curso',
      nivel: 'estandar',
      url: 'https://www.ispch.gob.cl/productos-y-servicios/cursos-e-learning/',
      descripcion: 'Catálogo de cursos en línea de la autoridad sanitaria, en temas de laboratorio, salud ambiental y ocupacional, vigilancia y uso seguro de productos. Formación técnica para quienes trabajan en salud pública.',
      gratuito: true,
      demo: false
    },
    {
      id: 'minsal-salud-digital',
      titulo: 'Capacitaciones en Salud Digital para funcionarios',
      proveedor: 'MINSAL · Departamento de Salud Digital',
      categoria: 'salud',
      modalidad: 'Online',
      duracion: 'Todo el año',
      nivel: 'estandar',
      url: 'https://portalsaluddigital.minsal.cl/difusion-y-documentos-de-interes/capacitaciones-funcionarios/',
      descripcion: 'Capacitaciones para funcionarios del sector sobre las herramientas y sistemas de salud digital del Estado: registros clínicos, interoperabilidad y buenas prácticas en el uso de datos de salud.',
      gratuito: true,
      demo: false
    }
  ];

  // Mapa: valor de `area_profesional` que entrega la API → categoría de curso.
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
