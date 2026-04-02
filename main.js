import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';

let db, conn;
let mesesCargados = new Set();
let usuarioActivo = null;
let todosLosUsuarios = {}; 
let lineaTiempo = []; 
let cargando = false;

const idCliente = 1;

/**
 * 1. INICIALIZACIÓN
 */
async function init() {
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

        updateStatus("🔑 Esperando Ingreso...");
        updateLog("ℹ️ Ingrese su ID de usuario para comenzar.");

        document.getElementById('btn-limpiar').addEventListener('click', limpiarFiltros);

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
        // Si tienes la lógica de actualizar placas al cambiar flota, la llamamos
        actualizarPlacasPorFlota(); 
    }

    // 3. Limpiar Tabla y Contadores
    const cuerpo = document.getElementById('cuerpo-tabla');
    const contador = document.getElementById('count-bloque');
    if (cuerpo) cuerpo.innerHTML = '';
    if (contador) contador.innerText = '0';

    updateLog("🧹 Filtros y tabla limpiados.");
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
 * 2. MANEJO DE LOGIN
 */
async function manejarLogin() {
    if (cargando) return;
    const inputId = document.getElementById('id-usuario-input').value.trim();
    if (!inputId) return;

    const usuarioEncontrado = todosLosUsuarios[inputId];
    if (usuarioEncontrado) {
        usuarioActivo = usuarioEncontrado;
        document.getElementById('nombre-sesion').innerText = usuarioActivo.nombre;
        
        await llenarSelectoresUI(); 
        await cargarUltimoMesPredeterminado();
    } else {
        alert("ID no reconocido.");
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

async function ejecutarConsultaFiltrada(fInicio, fFin) {
    if (mesesCargados.size === 0) return;
    const listaArchivos = Array.from(mesesCargados).map(f => `'${f}'`).join(', ');
    
    const filtroSeguridad = obtenerFiltroSeguridad();
    const pMasivas = obtenerValoresDesdeTextArea('input-placas-multiples');
    const fMasivas = obtenerValoresDesdeTextArea('input-flotas-multiples');
    const fSel = document.getElementById('select-flota').value;
    const pSel = document.getElementById('select-placa').value;

    let filtroUI = "1=1";

    // PRIORIDAD 1: Placas Manuales
    if (pMasivas.length > 0) {
        const listaSql = pMasivas.map(p => `'${p}'`).join(', ');
        filtroUI = `d.placa IN (${listaSql})`;
        updateLog(`📝 Filtrando por ${pMasivas.length} placas manuales.`);
    } 
    // PRIORIDAD 2: Flotas Manuales (Aquí estaba el fallo)
    else if (fMasivas.length > 0) {
        const listaSql = fMasivas.map(f => `'${f}'`).join(', ');
        filtroUI = `m.flota IN (${listaSql})`;
        updateLog(`📝 Filtrando por ${fMasivas.length} flotas manuales.`);
    } 
    // PRIORIDAD 3: Selectores normales
    else {
        if (fSel !== "TODAS") filtroUI += ` AND m.flota = '${fSel}'`;
        if (pSel !== "TODAS") filtroUI += ` AND d.placa = '${pSel}'`;
    }

    const sql = `
        SELECT 
            strftime(d.fecha, '%Y-%m-%d') as fecha_real, 
            d.placa, 
            m.flota,
            ROUND(SUM(d.distancia), 2) as km,
            ROUND(SUM(d.combustible), 2) as gal,
            MAX(d.maxima_velocidad) as v_max,
            ROUND(AVG(d.velocidad_promedio), 2) as v_prom,
            SUM(d.excesos_velocidad) as exc
        FROM read_parquet([${listaArchivos}], union_by_name=true) AS d
        INNER JOIN maestro AS m ON d.placa = m.placa
        WHERE (${filtroSeguridad})
          AND (${filtroUI})
          AND CAST(d.fecha AS DATE) BETWEEN '${fInicio}' AND '${fFin}'
        GROUP BY ALL
        ORDER BY fecha_real DESC, km DESC
        LIMIT 2000
    `;

    try {
        const res = await conn.query(sql);
        const datos = res.toArray();
        document.getElementById('count-bloque').innerText = datos.length.toLocaleString();
        mostrarTabla(datos);
        updateLog(`📊 Vista: ${datos.length} filas.`);
    } catch (e) {
        console.error("Error SQL:", e);
        updateLog("❌ Error en la consulta SQL. Revisa los nombres de las flotas.");
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

/**
 * FUNCIONES AUXILIARES (HELPERS)
 */
function obtenerFiltroSeguridad() {
    if (!usuarioActivo) return "1=0";
    if (usuarioActivo.placas_autorizadas.length > 4000) return "1=1";
    const placas = usuarioActivo.placas_autorizadas.map(p => `'${p}'`).join(', ');
    return `d.placa IN (${placas})`;
}

async function cargarDiccionarioUsuarios() {
    const url = new URL(`./${idCliente}/usuarios/permisos.json`, window.location.href).href;
    const respuesta = await fetch(url);
    todosLosUsuarios = await respuesta.json();
}

async function autodescubrirMeses() {
    let fechaBusqueda = new Date(); 
    const limitePasado = new Date(2024, 0, 1); 
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
            <td>${fila.v_max}</td>
            <td>${fila.v_prom}</td>
            <td>${fila.exc}</td>
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