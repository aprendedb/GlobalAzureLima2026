-- ============================================================
-- DEMO 01: Diagnóstico de Queries Lentas en Fabric Warehouse
-- ============================================================
-- COLUMNAS REALES CONFIRMADAS EN FABRIC:
--   exec_requests_history  → command, start_time, end_time,
--                            total_elapsed_time_ms, row_count,
--                            status, login_name, statement_type,
--                            allocated_cpu_time_ms,
--                            data_scanned_remote_storage_mb,
--                            data_scanned_memory_mb, result_cache_hit
--   frequently_run_queries → last_run_command, number_of_runs,
--                            avg_total_elapsed_time_ms,
--                            min_run_total_elapsed_time_ms,
--                            max_run_total_elapsed_time_ms,
--                            number_of_successful_runs,
--                            number_of_failed_runs,
--                            last_run_start_time, query_hash
-- ============================================================

-- ============================================================
-- 1. HISTORIAL RECIENTE — todas las ejecuciones
-- ============================================================
SELECT TOP 20
    start_time,
    total_elapsed_time_ms,
    row_count,
    statement_type,
    status,
    login_name,
    SUBSTRING(command, 1, 200)              AS query_preview
FROM queryinsights.exec_requests_history
ORDER BY start_time DESC;

-- ============================================================
-- 2. QUERIES MÁS LENTAS — últimas 24 horas
-- ============================================================
SELECT TOP 10
    start_time,
    total_elapsed_time_ms,
    row_count,
    statement_type,
    status,
    SUBSTRING(command, 1, 200)              AS query_preview
FROM queryinsights.exec_requests_history
WHERE total_elapsed_time_ms > 5000
  AND start_time >= DATEADD(HOUR, -24, GETDATE())
ORDER BY total_elapsed_time_ms DESC;

-- ============================================================
-- 3. MAYOR COSTO DE CPU Y DATOS ESCANEADOS
-- Métricas exclusivas de Fabric — no existen en SQL Server on-prem
-- ============================================================
SELECT TOP 10
    start_time,
    statement_type,
    total_elapsed_time_ms,
    allocated_cpu_time_ms,
    data_scanned_remote_storage_mb,
    data_scanned_memory_mb,
    row_count,
    SUBSTRING(command, 1, 150)              AS query_preview
FROM queryinsights.exec_requests_history
ORDER BY allocated_cpu_time_ms DESC;

-- ============================================================
-- 4. QUERIES FRECUENTES — mayor impacto acumulado
-- number_of_runs x avg_total_elapsed_time_ms = costo real
-- ============================================================
SELECT TOP 10
    number_of_runs,
    avg_total_elapsed_time_ms,
    CAST(number_of_runs * avg_total_elapsed_time_ms / 1000.0
         AS DECIMAL(12,2))                  AS costo_total_seg,
    min_run_total_elapsed_time_ms,
    max_run_total_elapsed_time_ms,
    number_of_successful_runs,
    number_of_failed_runs,
    last_run_start_time,
    SUBSTRING(last_run_command, 1, 200)     AS query_preview
FROM queryinsights.frequently_run_queries
ORDER BY costo_total_seg DESC;

-- ============================================================
-- 5. QUERIES CON MAYOR VARIABILIDAD
-- max muy distinto de min = query inestable, revisar plan
-- ============================================================
SELECT TOP 10
    number_of_runs,
    min_run_total_elapsed_time_ms,
    max_run_total_elapsed_time_ms,
    max_run_total_elapsed_time_ms
        - min_run_total_elapsed_time_ms     AS variabilidad_ms,
    avg_total_elapsed_time_ms,
    last_run_start_time,
    SUBSTRING(last_run_command, 1, 200)     AS query_preview
FROM queryinsights.frequently_run_queries
WHERE number_of_runs > 1
ORDER BY variabilidad_ms DESC;

-- ============================================================
-- 6. QUERIES ACTIVAS AHORA
-- ============================================================
SELECT
    session_id,
    total_elapsed_time / 1000               AS segundos_ejecutando,
    logical_reads,
    status,
    SUBSTRING(command, 1, 300)              AS query_preview
FROM sys.dm_exec_requests
WHERE status NOT IN ('background', 'sleeping')
  AND session_id > 50
ORDER BY total_elapsed_time DESC;

-- ============================================================
-- 7. VOLUMEN DE TABLAS (sin sys.allocation_units)
-- ============================================================
SELECT 'gold.fact_ventas'        AS tabla, COUNT(*) AS filas FROM gold.fact_ventas
UNION ALL
SELECT 'gold.dim_cliente',         COUNT(*) FROM gold.dim_cliente
UNION ALL
SELECT 'gold.dim_producto',        COUNT(*) FROM gold.dim_producto
UNION ALL
SELECT 'gold.dim_fecha',           COUNT(*) FROM gold.dim_fecha
UNION ALL
SELECT 'gold.dim_tienda',          COUNT(*) FROM gold.dim_tienda
UNION ALL
SELECT 'gold.agg_ventas_mensual',  COUNT(*) FROM gold.agg_ventas_mensual
UNION ALL
SELECT 'staging.ventas_raw',       COUNT(*) FROM staging.ventas_raw
ORDER BY filas DESC;
