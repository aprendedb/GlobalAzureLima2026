-- ============================================================
-- DEMO 02: Estadísticas del Optimizador en Fabric Warehouse
-- ============================================================
-- COMPATIBILIDAD FABRIC:
--   sys.stats                    → soportado
--   sys.dm_db_stats_properties   → NO soportado
--   EXEC sp_updatestats          → NO soportado
--   UPDATE STATISTICS WITH FULLSCAN → soportado
-- ============================================================

-- ============================================================
-- 1. VER ESTADÍSTICAS EXISTENTES EN ESQUEMA GOLD
-- ============================================================
SELECT
    sc.name                                 AS esquema,
    t.name                                  AS tabla,
    s.name                                  AS estadistica,
    s.stats_id,
    s.auto_created,
    s.user_created,
    s.has_filter
FROM sys.stats s
JOIN sys.tables  t  ON s.object_id = t.object_id
JOIN sys.schemas sc ON t.schema_id = sc.schema_id
WHERE sc.name IN ('gold', 'staging')
ORDER BY tabla, estadistica;

-- ============================================================
-- 2. SOLO ESTADÍSTICAS CREADAS POR NOSOTROS
-- ============================================================
SELECT
    sc.name                                 AS esquema,
    t.name                                  AS tabla,
    s.name                                  AS estadistica,
    s.user_created,
    s.auto_created
FROM sys.stats s
JOIN sys.tables  t  ON s.object_id = t.object_id
JOIN sys.schemas sc ON t.schema_id = sc.schema_id
WHERE sc.name IN ('gold', 'staging')
  AND s.user_created = 1
ORDER BY tabla, estadistica;

-- ============================================================
-- 3. TABLAS SIN ESTADÍSTICAS DE USUARIO
-- ============================================================
SELECT
    sc.name + '.' + t.name                  AS tabla,
    'Sin estadisticas de usuario'           AS estado,
    'CREATE STATISTICS stat_' + t.name
        + ' ON ' + sc.name + '.' + t.name
        + ' (<<columna_join>>);'            AS accion_sugerida
FROM sys.tables t
JOIN sys.schemas sc     ON t.schema_id = sc.schema_id
LEFT JOIN sys.stats st  ON t.object_id = st.object_id
                       AND st.user_created = 1
WHERE sc.name IN ('gold', 'staging')
  AND st.stats_id IS NULL
GROUP BY sc.name, t.name
ORDER BY tabla;

-- ============================================================
-- 4. CREAR ESTADÍSTICAS EN COLUMNAS CRÍTICAS
-- Ejecutar si las tablas no tienen estadísticas de usuario
-- ============================================================

-- Columnas de JOIN en fact_ventas (las más críticas)
CREATE STATISTICS stat_fact_fecha_sk
ON gold.fact_ventas (fecha_sk);

CREATE STATISTICS stat_fact_cliente_sk
ON gold.fact_ventas (cliente_sk);

CREATE STATISTICS stat_fact_producto_sk
ON gold.fact_ventas (producto_sk);

CREATE STATISTICS stat_fact_tienda_sk
ON gold.fact_ventas (tienda_sk);

-- Columnas métricas usadas en agregaciones
CREATE STATISTICS stat_fact_monto_neto
ON gold.fact_ventas (monto_neto);

-- Columnas de filtro WHERE en dimensiones
CREATE STATISTICS stat_cli_segmento
ON gold.dim_cliente (segmento);

CREATE STATISTICS stat_cli_vigente
ON gold.dim_cliente (es_vigente);

-- Estadística compuesta: columnas usadas JUNTAS en WHERE
CREATE STATISTICS stat_cli_seg_vig
ON gold.dim_cliente (segmento, es_vigente);

CREATE STATISTICS stat_fecha_anio
ON gold.dim_fecha (anio);

PRINT 'Estadisticas creadas.';

-- ============================================================
-- 5. ACTUALIZAR ESTADÍSTICAS
-- FULLSCAN = máxima precisión (más lento pero mejor plan)
-- Sin FULLSCAN = muestreo automático (más rápido)
-- ============================================================
UPDATE STATISTICS gold.fact_ventas WITH FULLSCAN;
UPDATE STATISTICS gold.dim_cliente WITH FULLSCAN;
UPDATE STATISTICS gold.dim_producto WITH FULLSCAN;
UPDATE STATISTICS gold.dim_fecha WITH FULLSCAN;
UPDATE STATISTICS gold.dim_tienda WITH FULLSCAN;

PRINT 'Estadisticas actualizadas con FULLSCAN.';

-- ============================================================
-- 6. GENERAR SCRIPT DE UPDATE PARA TODAS LAS TABLAS GOLD
-- Copiar el resultado y ejecutarlo
-- ============================================================
SELECT
    'UPDATE STATISTICS ' + sc.name + '.' + t.name
        + ' WITH FULLSCAN;'                 AS script_update
FROM sys.tables t
JOIN sys.schemas sc ON t.schema_id = sc.schema_id
WHERE sc.name = 'gold'
ORDER BY t.name;
