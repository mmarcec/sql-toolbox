/*
===========================================================
Source file: LivePerformanceAnalyzer.sql
Included in: Community + PRO (simple version in Community)

Purpose:
  Provide a quick live snapshot of:
    - Active requests
    - Top CPU queries from plan cache
===========================================================
*/

CREATE OR ALTER PROCEDURE SQLToolbox.LivePerformanceAnalyzer
    @TopN INT = 10
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '=== ACTIVE REQUESTS ===';

    SELECT TOP (@TopN)
        er.session_id,
        DB_NAME(er.database_id) AS database_name,
        er.status,
        er.command,
        er.cpu_time,
        er.total_elapsed_time,
        er.reads,
        er.writes,
        er.logical_reads,
        er.wait_type,
        er.wait_time,
        er.blocking_session_id,
        es.login_name,
        es.host_name,
        es.program_name,
        statement_text =
            SUBSTRING
            (
                st.text,
                (er.statement_start_offset / 2) + 1,
                (
                    (
                        CASE er.statement_end_offset
                            WHEN -1 THEN DATALENGTH(st.text)
                            ELSE er.statement_end_offset
                        END - er.statement_start_offset
                    ) / 2
                ) + 1
            )
    FROM sys.dm_exec_requests er
    INNER JOIN sys.dm_exec_sessions es
        ON er.session_id = es.session_id
    OUTER APPLY sys.dm_exec_sql_text(er.sql_handle) st
    WHERE er.session_id <> @@SPID
    ORDER BY er.cpu_time DESC, er.total_elapsed_time DESC;

    PRINT '';
    PRINT '=== TOP CPU QUERIES (PLAN CACHE) ===';

    ;WITH TopCpu AS
    (
        SELECT TOP (@TopN)
            database_name = DB_NAME(COALESCE(CAST(pa.value AS INT), st.dbid)),
            qs.execution_count,
            total_cpu_ms      = CAST(qs.total_worker_time / 1000.0 AS DECIMAL(18,2)),
            avg_cpu_ms        = CAST((qs.total_worker_time * 1.0 / NULLIF(qs.execution_count, 0)) / 1000.0 AS DECIMAL(18,2)),
            total_duration_ms = CAST(qs.total_elapsed_time / 1000.0 AS DECIMAL(18,2)),
            qs.total_logical_reads,
            qs.last_execution_time,
            query_text =
                LTRIM(RTRIM(
                    SUBSTRING
                    (
                        st.text,
                        (qs.statement_start_offset / 2) + 1,
                        (
                            (
                                CASE qs.statement_end_offset
                                    WHEN -1 THEN DATALENGTH(st.text)
                                    ELSE qs.statement_end_offset
                                END - qs.statement_start_offset
                            ) / 2
                        ) + 1
                    )
                ))
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        OUTER APPLY
        (
            SELECT TOP (1) pa.value
            FROM sys.dm_exec_plan_attributes(qs.plan_handle) pa
            WHERE pa.attribute = N'dbid'
        ) pa
        WHERE st.text IS NOT NULL
        ORDER BY qs.total_worker_time DESC
    )
    SELECT
        database_name,
        execution_count,
        total_cpu_ms,
        avg_cpu_ms,
        total_duration_ms,
        total_logical_reads,
        last_execution_time,
        query_text
    FROM TopCpu
    ORDER BY total_cpu_ms DESC;
END
GO