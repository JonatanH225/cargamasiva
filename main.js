import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';

let db, conn;
let mesesCargados = new Set();
let usuarioActivo = null;
let todosLosUsuarios = {}; 
let lineaTiempo = []; 
let cargando = false;

let idCliente = 1; // ✅ CAMBIADO A LET

/**
 * 1. INICIALIZACIÓN
 */
async function init() {
    // 1. DECLARAR EL INICIO AQUÍ (Fuera o al puro principio del try)
    const tInicioInit = performance.now(); 

    try {
        updateStatus("⚡ Inicializando Motor...");
        const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
        const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
        const workerCode = `importScripts("${bundle.mainWorker}");`;
        const workerBlob = new Blob([workerCode], { type: 'text/javascript' });
        const workerUrl = URL.createObjectURL(workerBlob);
        
        const worker = new Worker(workerUrl);
        db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
        conn = await db.connect();

        // Carga de archivo maestro
        updateStatus("⚡ Cargando Maestro de Vehículos...");
        const urlMaestro = new URL(`./${idCliente}/config/maestro_vehiculos.parquet`, window.location.href).href;
        await db.registerFileURL('maestro.parquet', urlMaestro, duckdb.DuckDBDataProtocol.HTTP, false);
        await conn.query(`CREATE VIEW IF NOT EXISTS maestro AS SELECT * FROM read_parquet('maestro.parquet')`);
        
        await cargarDiccionarioUsuarios();
        await autodescubrirMeses(); 

        // Registro de Eventos
        document.getElementById('btn-login').addEventListener('click', manejarLogin);
        document.getElementById('btn-filtrar').addEventListener('click', procesarFiltroFecha);
        document.getElementById('btn-exportar').addEventListener('click', exportarExcel);

        // Listener para que al cambiar flota se filtren las placas del select
        document.getElementById('select-flota').addEventListener('change', actualizarPlacasPorFlota);
        document.getElementById('btn-limpiar').addEventListener('click', limpiarFiltros);

        updateStatus("🔑 Esperando Ingreso...");
        updateLog("ℹ️ Ingrese su ID de usuario para comenzar.");

        // 2. CÁLCULO DEL TIEMPO (Ahora sí existe la variable tInicioInit)
        const tFinInit = performance.now();
        const tiempoCarga = ((tFinInit - tInicioInit) / 1000).toFixed(2);
        updateLog(`🚀 Motor e índices listos en **${tiempoCarga}s**`);

    } catch (error) {
        console.error(error);
        updateStatus("❌ Error en inicialización.");
    }
}

function limpiarFiltros() {
    // 1. Limpiar Textareas
    const pMasivas = document.getElementById('input-placas-multiples');
    const fMasivas = document.getElementById('input-flotas-multiples');
    if (pMasivas) pMasivas.value = '';
    if (fMasivas) fMasivas.value = '';

    // 2. Resetear Selectores a "TODAS"
    const selectFlota = document.getElementById('select-flota');
    const selectPlaca = document.getElementById('select-placa');
    if (selectFlota) selectFlota.value = 'TODAS';
    if (selectPlaca) {
        selectPlaca.value = 'TODAS';
        // Actualiza el listado de placas para que coincida con "Todas las Flotas"
        actualizarPlacasPorFlota(); 
    }

    // 3. Limpiar Tabla y todos los Contadores
    const cuerpo = document.getElementById('cuerpo-tabla');
    if (cuerpo) cuerpo.innerHTML = '';
    
    // Reseteo visual de los indicadores
    const idsContadores = ['count-bloque', 'count', 'count-unidades'];
    idsContadores.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.innerText = '0';
    });

    updateLog("🧹 Filtros, tabla y contadores limpiados.");
}

/**
 * Función genérica para parsear textareas (Placas o Flotas)
 * Soporta comas, espacios o saltos de línea
 */
function obtenerValoresDesdeTextArea(id) {
    const el = document.getElementById(id);
    if (!el) return [];
    const texto = el.value.trim();
    if (!texto) return [];
    
    return texto.split(/[\s,\n]+/)
                .map(v => v.trim().toUpperCase())
                .filter(v => v.length > 0);
}

/**
 * Mantenemos la función original para no romper dependencias, 
 * pero ahora usa la genérica internamente.
 */
function obtenerPlacasDesdeTextArea() {
    return obtenerValoresDesdeTextArea('input-placas-multiples');
}

/**
 * Actualiza el selector de placas según la flota que se elija
 */
async function actualizarPlacasPorFlota() {
    if (!usuarioActivo) return;
    
    const flotaSeleccionada = document.getElementById('select-flota').value;
    const selectPlaca = document.getElementById('select-placa');
    
    try {
        let filtroSeguridad = obtenerFiltroSeguridad();
        let filtroLimpio = filtroSeguridad.replace(/d\./g, "");

        let sql = `SELECT DISTINCT placa FROM maestro WHERE ${filtroLimpio}`;
        if (flotaSeleccionada !== "TODAS") {
            sql += ` AND flota = '${flotaSeleccionada}'`;
        }
        sql += ` ORDER BY placa ASC`;

        const res = await conn.query(sql);
        const placas = res.toArray();

        selectPlaca.innerHTML = '<option value="TODAS">-- Todas las Placas --</option>';
        placas.forEach(p => {
            const opt = document.createElement('option');
            opt.value = p.placa;
            opt.innerText = p.placa;
            selectPlaca.appendChild(opt);
        });

        updateLog(`🔍 Selector de placas actualizado para: ${flotaSeleccionada}`);
    } catch (e) {
        console.error("Error actualizando placas:", e);
    }
}

/**
 * Llenado inicial de flotas y placas al iniciar sesión
 */
async function llenarSelectoresUI() {
    if (!usuarioActivo) return;
    try {
        const selectFlota = document.getElementById('select-flota');
        let filtroSeguridad = obtenerFiltroSeguridad();
        let filtroLimpio = filtroSeguridad.replace(/d\./g, "");

        const resFlotas = await conn.query(`
            SELECT DISTINCT flota FROM maestro 
            WHERE ${filtroLimpio} AND flota IS NOT NULL 
            ORDER BY flota ASC
        `);
        
        const flotas = resFlotas.toArray();
        selectFlota.innerHTML = '<option value="TODAS">-- Todas las Flotas --</option>';
        flotas.forEach(f => {
            const opt = document.createElement('option');
            opt.value = f.flota;
            opt.innerText = f.flota;
            selectFlota.appendChild(opt);
        });

        await actualizarPlacasPorFlota();
        
    } catch (e) {
        console.error("Error al llenar selectores:", e);
    }
}

/**
 * 2. MANEJO DE LOGIN (Versión corregida)
 */
async function manejarLogin() {
    try {
        if (cargando) return;
        
        const inputId = document.getElementById('id-usuario-input').value.trim();
        if (!inputId) {
            alert("Por favor, ingrese un ID.");
            return;
        }

        console.log("Intentando login para:", inputId); // Ver en consola

        const usuarioEncontrado = todosLosUsuarios[inputId];
        
        if (usuarioEncontrado) {
            // Seteamos las variables globales
            usuarioActivo = usuarioEncontrado;
            idCliente = usuarioActivo.id_cliente; 
            
            // Limpiamos memoria del motor para el nuevo usuario
            if (typeof mesesCargados !== 'undefined') {
                mesesCargados.clear();
            }

            // Actualizamos la interfaz
            const elNombre = document.getElementById('nombre-sesion');
            if (elNombre) elNombre.innerText = usuarioActivo.nombre;

            updateLog(`🚀 Sesión iniciada: ${usuarioActivo.nombre}`);

            // IMPORTANTE: Primero llenamos selectores, luego cargamos datos
            await llenarSelectoresUI(); 
            await cargarUltimoMesPredeterminado();

        } else {
            alert("ID de usuario no reconocido.");
        }
    } catch (error) {
        console.error("ERROR CRÍTICO EN LOGIN:", error);
        updateLog("❌ Error en el proceso de login. Revisa la consola.");
    }
}

async function cargarUltimoMesPredeterminado() {
    if (lineaTiempo.length === 0) return updateLog("⚠️ No hay datos.");
    const [anio, mes] = lineaTiempo[0].split('_');
    document.getElementById('fecha-inicio').value = `${anio}-${mes}-01`;
    document.getElementById('fecha-fin').value = `${anio}-${mes}-31`;
    await procesarFiltroFecha();
}

/**
 * 3. PROCESAMIENTO Y CONSULTAS
 */
async function procesarFiltroFecha() {
    if (cargando || !usuarioActivo) return;
    cargando = true;
    const loader = document.getElementById('loader');
    if (loader) loader.style.display = 'block';

    const fInicio = document.getElementById('fecha-inicio').value;
    const fFin = document.getElementById('fecha-fin').value;

    try {
        const mesesRequeridos = obtenerMesesEnRango(fInicio, fFin);
        for (const mesAnio of mesesRequeridos) {
            if (lineaTiempo.includes(mesAnio)) {
                const fileName = `file_${mesAnio}.parquet`;
                if (!mesesCargados.has(fileName)) {
                    const url = new URL(`./1/data/${mesAnio}/consolidado_${mesAnio}.parquet`, window.location.href).href;
                    await db.registerFileURL(fileName, url, duckdb.DuckDBDataProtocol.HTTP, false);
                    mesesCargados.add(fileName);
                    updateLog(`📦 Montado: ${mesAnio}`);
                }
            }
        }
        await ejecutarConsultaFiltrada(fInicio, fFin);
    } catch (e) {
        console.error(e);
        updateLog("❌ Error de procesamiento.");
    } finally {
        cargando = false;
        updateStatus("✅ Motor listo");
        if (loader) loader.style.display = 'none';
    }
}



function rutaAbsoluta(path) {
    return new URL(path, window.location.href).href;
}

async function ejecutarConsultaFiltrada(fInicio, fFin) {
    const t0 = performance.now();
    
    // 1. Obtener meses necesarios
    const mesesNecesarios = obtenerMesesEnRango(fInicio, fFin);
    const archivosValidos = [];

    // 2. Validar con URL Absoluta
    for (const nombreMes of mesesNecesarios) {
        const rutaRelativa = `./${idCliente}/data/${nombreMes}/consolidado_${nombreMes}.parquet`;
        const urlFinal = rutaAbsoluta(rutaRelativa);
        
        try {
            const resp = await fetch(urlFinal, { method: 'HEAD' });
            if (resp.ok) {
                archivosValidos.push(urlFinal);
            }
        } catch (e) {
            console.warn(`No disponible: ${nombreMes}`);
        }
    }

    if (archivosValidos.length === 0) {
        updateLog("⚠️ No se encontraron archivos para el periodo seleccionado.");
        return;
    }

    const listaSQL = archivosValidos.map(f => `'${f}'`).join(', ');

    // --- BLOQUE DE SEGURIDAD ---
    // Obtenemos la cadena "placa IN ('ABC', 'DEF')" o "1=1"
    const filtroSeguridad = obtenerFiltroSeguridad();

    try {
        // Definimos la base de la consulta para no repetir código
        // IMPORTANTE: Aquí se aplica el filtro de fechas Y el de seguridad
        const sqlBase = `
            FROM read_parquet([${listaSQL}], union_by_name=true)
            WHERE CAST(fecha AS DATE) BETWEEN '${fInicio}' AND '${fFin}'
            AND ${filtroSeguridad}
        `;

        // 3. Ejecutar conteos (Totales)
        const resTotales = await conn.query(`
            SELECT 
                COUNT(*)::INTEGER as total_filas, 
                COUNT(DISTINCT placa)::INTEGER as total_unidades
            ${sqlBase}
        `);
        
        const resumen = resTotales.toArray()[0] || { total_filas: 0, total_unidades: 0 };

        // 4. Ejecutar Tabla (Datos detallados)
        const resTabla = await conn.query(`
            SELECT 
                strftime(fecha, '%Y-%m-%d') as fecha_real, placa, flota,
                ROUND(distancia, 2) as km, ROUND(combustible, 2) as gal,
                ROUND(combustible_idle, 2) as idle, aceleraciones as acel,
                frenadas as fren, giros, excesos_velocidad as exc,
                maxima_velocidad as v_max, ROUND(velocidad_promedio, 2) as v_prom,
                trayectos_realizados as tray
            ${sqlBase}
            ORDER BY fecha DESC LIMIT 2000
        `);

        const datos = resTabla.toArray();

        // --- ACTUALIZAR UI ---
        const safeSetText = (id, value) => {
            const el = document.getElementById(id);
            if (el) el.innerText = value.toLocaleString();
        };

        // Ahora estos valores serán de 10 unidades para el Analista Junior
        safeSetText('count-unidades', resumen.total_unidades);
        safeSetText('count-bloque', datos.length);
        safeSetText('count', resumen.total_filas);
        safeSetText('count-total', resumen.total_filas);
        safeSetText('count-celdas', resumen.total_filas * 13);

        mostrarTabla(datos);
        
        const duracion = ((performance.now() - t0) / 1000).toFixed(3);
        updateLog(`⏱️ Analizados **${resumen.total_filas.toLocaleString()}** registros en **${duracion}s**`);

    } catch (e) {
        console.error("Error en DuckDB:", e);
        updateLog("❌ Error procesando datos. Revisa la consola.");
    }
}

/**
 * 4. EXPORTACIÓN A EXCEL
 */
async function exportarExcel() {
    if (cargando || !usuarioActivo) return;
    updateLog("⏳ Generando reporte Excel...");
    
    const fInicio = document.getElementById('fecha-inicio').value;
    const fFin = document.getElementById('fecha-fin').value;
    const listaArchivos = Array.from(mesesCargados).map(f => `'${f}'`).join(', ');
    
    const filtroSeguridad = obtenerFiltroSeguridad();
    const placasTextArea = obtenerValoresDesdeTextArea('input-placas-multiples');
    const flotasTextArea = obtenerValoresDesdeTextArea('input-flotas-multiples');
    const flotaSel = document.getElementById('select-flota').value;
    const placaSel = document.getElementById('select-placa').value;

    let filtroUI = "1=1";
if (placasTextArea.length > 0) {
    filtroUI = `d.placa IN (${placasTextArea.map(p => `'${p}'`).join(',')})`;
} else if (flotasTextArea.length > 0) {
    filtroUI = `m.flota IN (${flotasTextArea.map(f => `'${f}'`).join(',')})`;
} else {
    if (flotaSel !== "TODAS") filtroUI += ` AND m.flota = '${flotaSel}'`;
    if (placaSel !== "TODAS") filtroUI += ` AND d.placa = '${placaSel}'`;
}

    const sqlExport = `
        SELECT 
            strftime(d.fecha, '%Y-%m-%d') as Fecha, d.placa as Placa, m.flota as Flota,
            ROUND(SUM(d.distancia), 2) as KM, ROUND(SUM(d.combustible), 2) as Galones,
            MAX(d.maxima_velocidad) as V_Max, ROUND(AVG(d.velocidad_promedio), 2) as V_Prom, SUM(d.excesos_velocidad) as Excesos
        FROM read_parquet([${listaArchivos}], union_by_name=true) AS d
        INNER JOIN maestro AS m ON d.placa = m.placa
        WHERE (${filtroSeguridad}) AND (${filtroUI})
          AND CAST(d.fecha AS DATE) BETWEEN '${fInicio}' AND '${fFin}'
        GROUP BY ALL ORDER BY Fecha ASC
    `;

    try {
        const res = await conn.query(sqlExport);
        const datos = res.toArray().map(r => r.toJSON());
        const worksheet = XLSX.utils.json_to_sheet(datos);
        const workbook = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(workbook, worksheet, "Reporte");
        XLSX.writeFile(workbook, `Reporte_Analitico.xlsx`);
        updateLog("✅ Excel generado.");
    } catch (e) {
        console.error(e);
        updateLog("❌ Error al exportar Excel.");
    }
}

async function detectarArchivoCorrupto(urls) {
    updateLog("🛠️ Verificando integridad de archivos...");
    for (const url of urls) {
        try {
            await conn.query(`SELECT COUNT(*) FROM read_parquet('${url}')`);
        } catch (e) {
            updateLog(`❌ ARCHIVO CORRUPTO DETECTADO: ${url}`);
            console.error(`El archivo ${url} está mal formado o es muy grande para el servidor actual.`);
        }
    }
}

/**
 * FUNCIONES AUXILIARES (HELPERS)
 */
function obtenerFiltroSeguridad() {
    // 1. Bloqueo total si no hay sesión
    if (!usuarioActivo) return "1=0";

    // 2. Optimización para Administrador (8,000 placas)
    // Si tiene más de la mitad de la flota, dejamos que vea todo sin procesar la lista larga
    if (usuarioActivo.placas_autorizadas.length > 4000) {
        return "1=1"; 
    }

    // 3. Filtro específico para Analistas (Junior, Senior, etc.)
    // Limpiamos espacios y envolvemos en comillas simples
    const placas = usuarioActivo.placas_autorizadas
        .map(p => `'${p.trim()}'`)
        .join(', ');

    // Retornamos el filtro simple (sin prefijo d. a menos que uses alias en el FROM)
    return `placa IN (${placas})`;
}

async function cargarDiccionarioUsuarios() {
    const url = new URL(`./${idCliente}/usuarios/permisos.json`, window.location.href).href;
    const respuesta = await fetch(url);
    todosLosUsuarios = await respuesta.json();
}

async function autodescubrirMeses() {
    let fechaBusqueda = new Date(); 
    const limitePasado = new Date(2023, 0, 1); 
    lineaTiempo = [];
    while (fechaBusqueda >= limitePasado) {
        const anio = fechaBusqueda.getFullYear();
        const mes = String(fechaBusqueda.getMonth() + 1).padStart(2, '0');
        const nombreMes = `${anio}_${mes}`;
        const url = new URL(`./1/data/${nombreMes}/consolidado_${nombreMes}.parquet`, window.location.href).href;
        try {
            const response = await fetch(url, { method: 'HEAD' });
            if (response.ok) lineaTiempo.push(nombreMes);
        } catch (e) {}
        fechaBusqueda.setMonth(fechaBusqueda.getMonth() - 1);
    }
}

function obtenerMesesEnRango(inicio, fin) {
    const start = new Date(inicio);
    const end = new Date(fin);
    const meses = [];
    let actual = new Date(start.getFullYear(), start.getMonth(), 1);
    while (actual <= end) {
        meses.push(`${actual.getFullYear()}_${String(actual.getMonth() + 1).padStart(2, '0')}`);
        actual.setMonth(actual.getMonth() + 1);
    }
    return meses;
}

function mostrarTabla(datos) {
    const cuerpo = document.getElementById('cuerpo-tabla');
    cuerpo.innerHTML = '';
    const fragmento = document.createDocumentFragment();
    
    datos.forEach(fila => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td style="color:#1976d2; font-weight:bold">${fila.fecha_real}</td>
            <td>${fila.placa}</td>
            <td>${fila.flota}</td>
            <td>${Number(fila.km).toLocaleString()}</td>
            <td>${Number(fila.gal).toLocaleString()}</td>
            <td>${Number(fila.idle).toLocaleString()}</td>
            <td>${fila.acel}</td>
            <td>${fila.fren}</td>
            <td>${fila.giros}</td>
            <td>${fila.exc}</td>
            <td>${fila.v_max}</td>
            <td>${fila.v_prom}</td>
            <td>${fila.tray}</td>
        `;
        fragmento.appendChild(tr);
    });
    cuerpo.appendChild(fragmento);
}

function updateStatus(txt) { 
    const st = document.getElementById('status');
    if (st) {
        st.innerText = txt; 
        st.className = txt.includes('✅') ? 'ready' : 'loading';
    }
}

function updateLog(txt) { 
    const log = document.getElementById('log');
    if (log) {
        const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        log.innerHTML += `<div><span style="color:#888">[${time}]</span> ${txt}</div>`;
        log.scrollTop = log.scrollHeight;
    }
}

init();