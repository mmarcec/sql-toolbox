/*
SQL Toolbox - Community Edition
Module: WOW Performance Killers (read-only)
*/

CREATE OR ALTER PROCEDURE SQLToolbox.WOWPerformanceKillers
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '===================================================';
    PRINT 'SQL TOOLBOX – WOW PERFORMANCE KILLERS REPORT';
    PRINT 'Database: ' + DB_NAME();
    PRINT 'Server: ' + @@SERVERNAME;
    PRINT 'Time: ' + CONVERT(varchar(19), GETDATE(), 120);
    PRINT '===================================================';

    ------------------------------------------------------------
    -- 1) TABLE SCANS (user_scans > user_seeks)
    ------------------------------------------------------------
    PRINT '';
    PRINT '1) TABLE SCANS DETECTED';
    PRINT '---------------------------------------------------';

    SELECT TOP 20
        OBJECT_SCHEMA_NAME(s.object_id) AS SchemaName,
        OBJECT_NAME(s.object_id) AS TableName,
        i.name AS IndexName,
        s.user_scans,
        s.user_seeks,
        s.user_lookups,
        s.user_updates
    FROM sys.dm_db_index_usage_stats s
    JOIN sys.indexes i
        ON s.object_id = i.object_id AND s.index_id = i.index_id
    WHERE s.database_id = DB_ID()
      AND s.user_scans > s.user_seeks
    ORDER BY s.user_scans DESC;

    ------------------------------------------------------------
    -- 2) HIGH IMPACT MISSING INDEXES
    ------------------------------------------------------------
    PRINT '';
    PRINT '2) HIGH IMPACT MISSING INDEXES';
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
        user_seeks,
        user_scans,
        CAST(ImpactScore AS decimal(18,2)) AS ImpactScore
    FROM mi
    ORDER BY ImpactScore DESC;

    ------------------------------------------------------------
    -- 3) MOST EXPENSIVE QUERIES (avg duration)
    ------------------------------------------------------------
    PRINT '';
    PRINT '3) MOST EXPENSIVE QUERIES (TOP 10 by avg duration)';
    PRINT '---------------------------------------------------';

    SELECT TOP 10
        CAST(qs.total_elapsed_time * 1.0 / NULLIF(qs.execution_count,0) / 1000.0 AS decimal(18,2)) AS AvgDurationMs,
        CAST(qs.total_worker_time * 1.0 / NULLIF(qs.execution_count,0) / 1000.0 AS decimal(18,2)) AS AvgCpuMs,
        CAST(qs.total_logical_reads * 1.0 / NULLIF(qs.execution_count,0) AS decimal(18,2)) AS AvgLogicalReads,
        qs.execution_count,
        SUBSTRING(
            st.text,
            (qs.statement_start_offset/2) + 1,
            ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text) ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) + 1
        ) AS QueryText
    FROM sys.dm_exec_query_stats qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
    ORDER BY (qs.total_elapsed_time * 1.0 / NULLIF(qs.execution_count,0)) DESC;

    ------------------------------------------------------------
    -- 4) LARGEST TABLES (rows)
    ------------------------------------------------------------
    PRINT '';
    PRINT '4) LARGEST TABLES (TOP 10 by rows)';
    PRINT '---------------------------------------------------';

    SELECT TOP 10
        SCHEMA_NAME(t.schema_id) AS SchemaName,
        t.name AS TableName,
        SUM(p.rows) AS RowCounts
    FROM sys.tables t
    JOIN sys.partitions p
        ON t.object_id = p.object_id
    WHERE p.index_id IN (0,1)
    GROUP BY t.schema_id, t.name
    ORDER BY RowCounts DESC;

    ------------------------------------------------------------
    -- 5) LARGE TABLES WITHOUT NONCLUSTERED INDEXES
    ------------------------------------------------------------
    PRINT '';
    PRINT '5) LARGE TABLES WITHOUT NONCLUSTERED INDEXES (rows > 100k)';
    PRINT '---------------------------------------------------';

    SELECT
        SCHEMA_NAME(t.schema_id) AS SchemaName,
        t.name AS TableName,
        SUM(p.rows) AS RowCounts
    FROM sys.tables t
    JOIN sys.partitions p
        ON t.object_id = p.object_id
    WHERE p.index_id IN (0,1)
      AND NOT EXISTS
      (
          SELECT 1
          FROM sys.indexes i
          WHERE i.object_id = t.object_id
            AND i.type = 2
      )
    GROUP BY t.schema_id, t.name
    HAVING SUM(p.rows) > 100000
    ORDER BY RowCounts DESC;

    ------------------------------------------------------------
    -- 6) UNUSED NONCLUSTERED INDEXES (very conservative)
    ------------------------------------------------------------
    PRINT '';
    PRINT '6) UNUSED NONCLUSTERED INDEXES (conservative)';
    PRINT '---------------------------------------------------';

    SELECT TOP 50
        OBJECT_SCHEMA_NAME(i.object_id) AS SchemaName,
        OBJECT_NAME(i.object_id) AS TableName,
        i.name AS IndexName,
        ISNULL(us.user_seeks,0) AS user_seeks,
        ISNULL(us.user_scans,0) AS user_scans,
        ISNULL(us.user_lookups,0) AS user_lookups,
        ISNULL(us.user_updates,0) AS user_updates
    FROM sys.indexes i
    LEFT JOIN sys.dm_db_index_usage_stats us
        ON us.object_id = i.object_id
        AND us.index_id = i.index_id
        AND us.database_id = DB_ID()
    WHERE i.type = 2
      AND i.is_primary_key = 0
      AND i.is_unique = 0
      AND i.is_unique_constraint = 0
      AND ISNULL(us.user_seeks,0) = 0
      AND ISNULL(us.user_scans,0) = 0
      AND ISNULL(us.user_lookups,0) = 0
    ORDER BY user_updates DESC, TableName, IndexName;

    ------------------------------------------------------------
    -- 7) BLOCKING SESSIONS (current)
    ------------------------------------------------------------
    PRINT '';
    PRINT '7) BLOCKING SESSIONS (CURRENT)';
    PRINT '---------------------------------------------------';

    SELECT
        GETDATE() AS CaptureTime,
        DB_NAME(r.database_id) AS DatabaseName,
        r.session_id AS BlockedSessionID,
        r.blocking_session_id AS BlockingSessionID,
        r.wait_type,
        r.wait_time AS WaitTimeMs,
        r.wait_resource,
        st.text AS BlockedQueryText
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
    WHERE r.blocking_session_id <> 0
    ORDER BY r.wait_time DESC;

    PRINT '';
    PRINT '===================================================';
    PRINT 'WOW PERFORMANCE REPORT COMPLETE';
    PRINT '===================================================';
END
GO
