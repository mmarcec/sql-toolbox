/*
===========================================================
Source file: TopSlowQueries.sql
Included in: Community + PRO

Purpose:
  Show the highest-cost cached queries by CPU, duration,
  or reads.
===========================================================
*/

CREATE OR ALTER PROCEDURE SQLToolbox.TopSlowQueries
    @TopN INT = 10,
    @SortBy NVARCHAR(20) = N'CPU'   -- CPU | DURATION | READS
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH QueryStats AS
    (
        SELECT
            database_name = DB_NAME(COALESCE(CAST(pa.value AS INT), st.dbid)),
            qs.execution_count,
            total_cpu_ms       = CAST(qs.total_worker_time / 1000.0 AS DECIMAL(18,2)),
            avg_cpu_ms         = CAST((qs.total_worker_time * 1.0 / NULLIF(qs.execution_count, 0)) / 1000.0 AS DECIMAL(18,2)),
            total_duration_ms  = CAST(qs.total_elapsed_time / 1000.0 AS DECIMAL(18,2)),
            avg_duration_ms    = CAST((qs.total_elapsed_time * 1.0 / NULLIF(qs.execution_count, 0)) / 1000.0 AS DECIMAL(18,2)),
            qs.total_logical_reads,
            avg_logical_reads  = CAST(qs.total_logical_reads * 1.0 / NULLIF(qs.execution_count, 0) AS DECIMAL(18,2)),
            qs.total_logical_writes,
            avg_logical_writes = CAST(qs.total_logical_writes * 1.0 / NULLIF(qs.execution_count, 0) AS DECIMAL(18,2)),
            qs.creation_time,
            qs.last_execution_time,
            qs.plan_handle,
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
                )),
            insight =
                CASE
                    WHEN qs.total_logical_reads >= 1000000 THEN N'High IO - review indexes and access path'
                    WHEN qs.total_worker_time >= 1000000 THEN N'High CPU - review predicates, joins, scalar logic'
                    WHEN qs.total_elapsed_time >= 5000000 THEN N'Long duration - investigate waits, blocking, IO, or parallelism'
                    ELSE N'Review execution plan'
                END
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        OUTER APPLY
        (
            SELECT TOP (1) pa.value
            FROM sys.dm_exec_plan_attributes(qs.plan_handle) pa
            WHERE pa.attribute = N'dbid'
        ) pa
        WHERE st.text IS NOT NULL
    )
    SELECT TOP (@TopN)
        database_name,
        execution_count,
        total_cpu_ms,
        avg_cpu_ms,
        total_duration_ms,
        avg_duration_ms,
        total_logical_reads,
        avg_logical_reads,
        total_logical_writes,
        avg_logical_writes,
        creation_time,
        last_execution_time,
        insight,
        query_text,
        plan_handle
    FROM QueryStats
    ORDER BY
        CASE WHEN UPPER(@SortBy) = N'CPU'      THEN total_cpu_ms END DESC,
        CASE WHEN UPPER(@SortBy) = N'DURATION' THEN total_duration_ms END DESC,
        CASE WHEN UPPER(@SortBy) = N'READS'    THEN total_logical_reads END DESC,
        total_cpu_ms DESC;
END
GO