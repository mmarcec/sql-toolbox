/*
SQL Toolbox - Community Edition
Module: Live Performance Analyzer (read-only)
Purpose: "What is hurting my database RIGHT NOW?"
*/

CREATE OR ALTER PROCEDURE SQLToolbox.LivePerformanceAnalyzer
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '===================================================';
    PRINT 'SQL TOOLBOX – LIVE PERFORMANCE ANALYZER';
    PRINT 'Database: ' + DB_NAME();
    PRINT 'Server: ' + @@SERVERNAME;
    PRINT 'Time: ' + CONVERT(varchar(19), GETDATE(), 120);
    PRINT '===================================================';

    ------------------------------------------------------------
    -- 1) BLOCKING (right now)
    ------------------------------------------------------------
    PRINT '';
    PRINT '1) BLOCKING SESSIONS (CURRENT)';
    PRINT '---------------------------------------------------';

    SELECT
        GETDATE() AS CaptureTime,
        DB_NAME(r.database_id) AS DatabaseName,
        r.session_id AS BlockedSessionID,
        r.blocking_session_id AS BlockingSessionID,
        r.wait_type,
        r.wait_time AS WaitTimeMs,
        r.wait_resource,
        r.status,
        r.command,
        r.cpu_time,
        r.total_elapsed_time,
        st.text AS BlockedQueryText
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
    WHERE r.blocking_session_id <> 0
    ORDER BY r.wait_time DESC;

    ------------------------------------------------------------
    -- 2) LONG RUNNING REQUESTS (current)
    ------------------------------------------------------------
    PRINT '';
    PRINT '2) LONG RUNNING REQUESTS (CURRENT, > 10s)';
    PRINT '---------------------------------------------------';

    SELECT TOP 20
        GETDATE() AS CaptureTime,
        DB_NAME(r.database_id) AS DatabaseName,
        r.session_id,
        r.status,
        r.command,
        r.cpu_time,
        r.total_elapsed_time AS ElapsedMs,
        r.reads,
        r.writes,
        r.logical_reads,
        st.text AS SqlText
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
    WHERE r.total_elapsed_time > 10000
      AND r.session_id <> @@SPID
    ORDER BY r.total_elapsed_time DESC;

    ------------------------------------------------------------
    -- 3) TOP CPU QUERIES (plan cache)
    ------------------------------------------------------------
    PRINT '';
    PRINT '3) TOP CPU QUERIES (PLAN CACHE)';
    PRINT '---------------------------------------------------';

    SELECT TOP 10
        CAST(qs.total_worker_time/1000.0 AS decimal(18,2)) AS TotalCpuMs,
        qs.execution_count,
        CAST((qs.total_worker_time*1.0/NULLIF(qs.execution_count,0))/1000.0 AS decimal(18,2)) AS AvgCpuMs,
        CAST((qs.total_elapsed_time*1.0/NULLIF(qs.execution_count,0))/1000.0 AS decimal(18,2)) AS AvgDurationMs,
        CAST((qs.total_logical_reads*1.0/NULLIF(qs.execution_count,0)) AS decimal(18,2)) AS AvgLogicalReads,
        SUBSTRING(
            st.text,
            (qs.statement_start_offset/2) + 1,
            ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) + 1
        ) AS QueryText
    FROM sys.dm_exec_query_stats qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
    ORDER BY qs.total_worker_time DESC;

    PRINT '';
    PRINT '===================================================';
    PRINT 'LIVE ANALYSIS COMPLETE';
    PRINT '===================================================';
END
GO
