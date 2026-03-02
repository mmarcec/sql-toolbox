/*
SQL Toolbox - Community Edition
Module: Instant Diagnostic (read-only)
*/

CREATE OR ALTER PROCEDURE SQLToolbox.InstantDiagnostic
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '===================================================';
    PRINT 'SQL TOOLBOX – INSTANT DATABASE DIAGNOSTIC';
    PRINT 'Database: ' + DB_NAME();
    PRINT 'Server: ' + @@SERVERNAME;
    PRINT 'Time: ' + CONVERT(varchar(19), GETDATE(), 120);
    PRINT '===================================================';

    ------------------------------------------------------------
    -- DATABASE INFO
    ------------------------------------------------------------
    PRINT '';
    PRINT 'DATABASE INFO';
    PRINT '---------------------------------------------------';

    SELECT
        d.name AS DatabaseName,
        d.compatibility_level,
        d.recovery_model_desc,
        d.page_verify_option_desc,
        d.state_desc,
        d.is_read_only
    FROM sys.databases d
    WHERE d.name = DB_NAME();

    ------------------------------------------------------------
    -- DATABASE FILES (SIZE)
    ------------------------------------------------------------
    PRINT '';
    PRINT 'DATABASE FILES';
    PRINT '---------------------------------------------------';

    SELECT
        df.name AS LogicalName,
        df.type_desc AS FileType,
        CAST(df.size/128.0 AS decimal(18,2)) AS SizeMB,
        CASE WHEN df.max_size = -1 THEN NULL ELSE CAST(df.max_size/128.0 AS decimal(18,2)) END AS MaxSizeMB,
        df.growth,
        df.is_percent_growth
    FROM sys.database_files df
    ORDER BY df.type_desc, df.name;

    ------------------------------------------------------------
    -- INDEX FRAGMENTATION (TOP)
    ------------------------------------------------------------
    PRINT '';
    PRINT 'INDEX FRAGMENTATION (TOP 10, rowstore only)';
    PRINT '---------------------------------------------------';

    SELECT TOP 10
        OBJECT_SCHEMA_NAME(ips.object_id) AS SchemaName,
        OBJECT_NAME(ips.object_id) AS TableName,
        i.name AS IndexName,
        CAST(ips.avg_fragmentation_in_percent AS decimal(9,2)) AS FragmentationPct,
        ips.page_count
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    JOIN sys.indexes i
        ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.page_count >= 1000
      AND i.type IN (1,2) -- exclude columnstore
      AND ips.avg_fragmentation_in_percent > 5
    ORDER BY ips.avg_fragmentation_in_percent DESC;

    ------------------------------------------------------------
    -- MISSING INDEXES (TOP)
    ------------------------------------------------------------
    PRINT '';
    PRINT 'MISSING INDEXES (TOP 10 by estimated impact)';
    PRINT '---------------------------------------------------';

    ;WITH mi AS
    (
        SELECT
            d.[statement] AS StatementText,
            d.equality_columns,
            d.inequality_columns,
            d.included_columns,
            s.avg_total_user_cost,
            s.avg_user_impact,
            s.user_seeks,
            s.user_scans,
            (s.user_seeks + s.user_scans) * s.avg_total_user_cost * (s.avg_user_impact/100.0) AS ImpactScore
        FROM sys.dm_db_missing_index_details d
        JOIN sys.dm_db_missing_index_groups g
            ON d.index_handle = g.index_handle
        JOIN sys.dm_db_missing_index_group_stats s
            ON g.index_group_handle = s.group_handle
        WHERE d.database_id = DB_ID()
    )
    SELECT TOP 10
        StatementText,
        equality_columns,
        inequality_columns,
        included_columns,
        CAST(avg_user_impact AS decimal(9,2)) AS AvgUserImpact,
        CAST(avg_total_user_cost AS decimal(18,2)) AS AvgTotalUserCost,
        user_seeks,
        user_scans,
        CAST(ImpactScore AS decimal(18,2)) AS ImpactScore
    FROM mi
    ORDER BY ImpactScore DESC;

    ------------------------------------------------------------
    -- TOP SLOW QUERIES (Avg Duration)
    ------------------------------------------------------------
    PRINT '';
    PRINT 'TOP SLOW QUERIES (TOP 10 by avg duration)';
    PRINT '---------------------------------------------------';

    SELECT TOP 10
        CAST(qs.total_elapsed_time * 1.0 / NULLIF(qs.execution_count,0) / 1000.0 AS decimal(18,2)) AS AvgDurationMs,
        CAST(qs.total_worker_time * 1.0 / NULLIF(qs.execution_count,0) / 1000.0 AS decimal(18,2)) AS AvgCpuMs,
        CAST(qs.total_logical_reads * 1.0 / NULLIF(qs.execution_count,0) AS decimal(18,2)) AS AvgLogicalReads,
        qs.execution_count,
        DB_NAME(st.dbid) AS DbName,
        SUBSTRING(
            st.text,
            (qs.statement_start_offset/2) + 1,
            ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) + 1
        ) AS QueryText
    FROM sys.dm_exec_query_stats qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
    ORDER BY (qs.total_elapsed_time * 1.0 / NULLIF(qs.execution_count,0)) DESC;

    ------------------------------------------------------------
    -- BLOCKING (CURRENT)
    ------------------------------------------------------------
    PRINT '';
    PRINT 'BLOCKING SESSIONS (CURRENT)';
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

    PRINT '';
    PRINT '===================================================';
    PRINT 'INSTANT DIAGNOSTIC COMPLETE';
    PRINT '===================================================';
END
GO
