/*
===========================================================
SQL TOOLBOX COMMUNITY - INSTALLER
File: 10_SQLToolbox_Community_INSTALL.sql

Creates:
  - Database [SQLToolbox] (if missing)
  - Schema [SQLToolbox]
  - Procedures:
      SQLToolbox.WaitStatsSummary
      SQLToolbox.TopSlowQueries
      SQLToolbox.BlockingMonitor
      SQLToolbox.DeadlockDetector
      SQLToolbox.LivePerformanceAnalyzer

Purpose:
  - Lightweight community edition
  - Fast install
  - Useful diagnostic starting point
  - Designed to complement the PRO edition

Notes:
  - Read-only diagnostics only
  - Requires VIEW SERVER STATE for best results
  - No HTML reporting, automation, history, or RunAll orchestration
===========================================================
*/

SET NOCOUNT ON;
GO

-----------------------------------------------------------
-- 0) Create dedicated database
-----------------------------------------------------------
IF DB_ID(N'SQLToolbox') IS NULL
BEGIN
    PRINT 'Creating database [SQLToolbox]...';
    EXEC('CREATE DATABASE [SQLToolbox];');
END
ELSE
BEGIN
    PRINT 'Database [SQLToolbox] already exists.';
END
GO

USE [SQLToolbox];
GO

-----------------------------------------------------------
-- 1) Create schema
-----------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'SQLToolbox')
BEGIN
    EXEC('CREATE SCHEMA [SQLToolbox] AUTHORIZATION [dbo];');
    PRINT 'Created schema [SQLToolbox].';
END
ELSE
BEGIN
    PRINT 'Schema [SQLToolbox] already exists.';
END
GO

-----------------------------------------------------------
-- 2) Procedure: WaitStatsSummary
-----------------------------------------------------------
CREATE OR ALTER PROCEDURE SQLToolbox.WaitStatsSummary
    @TopN INT = 10
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH Waits AS
    (
        SELECT
            ws.wait_type,
            ws.waiting_tasks_count,
            ws.wait_time_ms,
            ws.signal_wait_time_ms,
            ws.max_wait_time_ms,
            wait_time_s          = CAST(ws.wait_time_ms / 1000.0 AS DECIMAL(18,2)),
            signal_wait_time_s   = CAST(ws.signal_wait_time_ms / 1000.0 AS DECIMAL(18,2)),
            resource_wait_time_s = CAST((ws.wait_time_ms - ws.signal_wait_time_ms) / 1000.0 AS DECIMAL(18,2)),
            pct                  = CAST(100.0 * ws.wait_time_ms / NULLIF(SUM(ws.wait_time_ms) OVER(), 0) AS DECIMAL(6,2))
        FROM sys.dm_os_wait_stats ws
        WHERE ws.wait_type NOT IN
        (
            N'BROKER_EVENTHANDLER',
            N'BROKER_RECEIVE_WAITFOR',
            N'BROKER_TASK_STOP',
            N'BROKER_TO_FLUSH',
            N'BROKER_TRANSMITTER',
            N'CHECKPOINT_QUEUE',
            N'CHKPT',
            N'CLR_AUTO_EVENT',
            N'CLR_MANUAL_EVENT',
            N'CLR_SEMAPHORE',
            N'DBMIRROR_DBM_EVENT',
            N'DBMIRROR_EVENTS_QUEUE',
            N'DBMIRROR_WORKER_QUEUE',
            N'DBMIRRORING_CMD',
            N'DIRTY_PAGE_POLL',
            N'DISPATCHER_QUEUE_SEMAPHORE',
            N'EXECSYNC',
            N'FSAGENT',
            N'FT_IFTS_SCHEDULER_IDLE_WAIT',
            N'FT_IFTSHC_MUTEX',
            N'HADR_CLUSAPI_CALL',
            N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
            N'HADR_LOGCAPTURE_WAIT',
            N'HADR_NOTIFICATION_DEQUEUE',
            N'HADR_TIMER_TASK',
            N'HADR_WORK_QUEUE',
            N'KSOURCE_WAKEUP',
            N'LAZYWRITER_SLEEP',
            N'LOGMGR_QUEUE',
            N'MEMORY_ALLOCATION_EXT',
            N'ONDEMAND_TASK_QUEUE',
            N'PARALLEL_REDO_DRAIN_WORKER',
            N'PARALLEL_REDO_LOG_CACHE',
            N'PARALLEL_REDO_TRAN_LIST',
            N'PARALLEL_REDO_WORKER_SYNC',
            N'PARALLEL_REDO_WORKER_WAIT_WORK',
            N'PREEMPTIVE_XE_GETTARGETSTATE',
            N'PWAIT_ALL_COMPONENTS_INITIALIZED',
            N'PWAIT_DIRECTLOGCONSUMER_GETNEXT',
            N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
            N'QDS_ASYNC_QUEUE',
            N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
            N'QDS_SHUTDOWN_QUEUE',
            N'REDO_THREAD_PENDING_WORK',
            N'REQUEST_FOR_DEADLOCK_SEARCH',
            N'RESOURCE_QUEUE',
            N'SERVER_IDLE_CHECK',
            N'SLEEP_BPOOL_FLUSH',
            N'SLEEP_DBSTARTUP',
            N'SLEEP_DCOMSTARTUP',
            N'SLEEP_MASTERDBREADY',
            N'SLEEP_MASTERMDREADY',
            N'SLEEP_MASTERUPGRADED',
            N'SLEEP_MSDBSTARTUP',
            N'SLEEP_SYSTEMTASK',
            N'SLEEP_TASK',
            N'SLEEP_TEMPDBSTARTUP',
            N'SNI_HTTP_ACCEPT',
            N'SOS_WORK_DISPATCHER',
            N'SP_SERVER_DIAGNOSTICS_SLEEP',
            N'SQLTRACE_BUFFER_FLUSH',
            N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
            N'SQLTRACE_WAIT_ENTRIES',
            N'WAIT_FOR_RESULTS',
            N'WAITFOR',
            N'WAITFOR_TASKSHUTDOWN',
            N'WAIT_XTP_RECOVERY',
            N'WAIT_XTP_HOST_WAIT',
            N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
            N'WAIT_XTP_CKPT_CLOSE',
            N'XE_DISPATCHER_JOIN',
            N'XE_DISPATCHER_WAIT',
            N'XE_TIMER_EVENT'
        )
          AND ws.waiting_tasks_count > 0
          AND ws.wait_time_ms > 0
    )
    SELECT TOP (@TopN)
        wait_type,
        waiting_tasks_count,
        wait_time_ms,
        wait_time_s,
        signal_wait_time_ms,
        signal_wait_time_s,
        resource_wait_time_s,
        pct,
        interpretation =
            CASE
                WHEN wait_type LIKE N'LCK[_]%' THEN N'Locking / blocking contention'
                WHEN wait_type LIKE N'PAGEIOLATCH[_]%' THEN N'Physical IO pressure or poor indexing'
                WHEN wait_type IN (N'CXPACKET', N'CXCONSUMER') THEN N'Parallelism-related wait'
                WHEN wait_type = N'SOS_SCHEDULER_YIELD' THEN N'CPU pressure or long CPU-bound queries'
                WHEN wait_type LIKE N'WRITELOG%' THEN N'Transaction log bottleneck'
                WHEN wait_type LIKE N'ASYNC_NETWORK_IO%' THEN N'Client/network consumption delay'
                WHEN wait_type LIKE N'TEMPDB%' THEN N'TempDB contention or heavy TempDB usage'
                WHEN wait_type LIKE N'PAGELATCH[_]%' THEN N'In-memory latch contention, often TempDB/allocation related'
                ELSE N'General investigation required'
            END,
        recommended_next_step =
            CASE
                WHEN wait_type LIKE N'LCK[_]%' THEN N'Check blocking sessions'
                WHEN wait_type LIKE N'PAGEIOLATCH[_]%' THEN N'Review indexes and storage throughput'
                WHEN wait_type IN (N'CXPACKET', N'CXCONSUMER') THEN N'Review parallelism settings and high CPU queries'
                WHEN wait_type = N'SOS_SCHEDULER_YIELD' THEN N'Run SQLToolbox.TopSlowQueries'
                WHEN wait_type LIKE N'WRITELOG%' THEN N'Check transaction log throughput, autogrowth, long transactions'
                ELSE N'Investigate related workload and waits'
            END
    FROM Waits
    ORDER BY wait_time_ms DESC;
END
GO

-----------------------------------------------------------
-- 3) Procedure: TopSlowQueries
-----------------------------------------------------------
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

-----------------------------------------------------------
-- 4) Procedure: BlockingMonitor
-----------------------------------------------------------
CREATE OR ALTER PROCEDURE SQLToolbox.BlockingMonitor
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        er.session_id,
        er.blocking_session_id,
        DB_NAME(er.database_id) AS database_name,
        er.status,
        er.command,
        er.wait_type,
        er.wait_time,
        er.wait_resource,
        er.cpu_time,
        er.total_elapsed_time,
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
    WHERE er.blocking_session_id <> 0
    ORDER BY er.wait_time DESC, er.total_elapsed_time DESC;
END
GO

-----------------------------------------------------------
-- 5) Procedure: DeadlockDetector (system_health)
-----------------------------------------------------------
CREATE OR ALTER PROCEDURE SQLToolbox.DeadlockDetector
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH Deadlocks AS
    (
        SELECT
            event_time = xed.event_data.value('(event/@timestamp)[1]', 'datetime2'),
            deadlock_graph = CAST(xed.event_data.query('(event/data/value/deadlock)[1]') AS nvarchar(max))
        FROM
        (
            SELECT CAST(st.target_data AS xml) AS target_data
            FROM sys.dm_xe_session_targets st
            INNER JOIN sys.dm_xe_sessions s
                ON s.address = st.event_session_address
            WHERE s.name = N'system_health'
              AND st.target_name = N'ring_buffer'
        ) AS src
        CROSS APPLY src.target_data.nodes('//RingBufferTarget/event[@name="xml_deadlock_report"]') AS xed(event_data)
    )
    SELECT TOP (20)
        event_time,
        deadlock_graph
    FROM Deadlocks
    ORDER BY event_time DESC;
END
GO

-----------------------------------------------------------
-- 6) Procedure: LivePerformanceAnalyzer (simple)
-----------------------------------------------------------
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

PRINT '===========================================================';
PRINT 'SQL TOOLBOX COMMUNITY INSTALL COMPLETE';
PRINT 'Installed procedures:';
PRINT '  - SQLToolbox.WaitStatsSummary';
PRINT '  - SQLToolbox.TopSlowQueries';
PRINT '  - SQLToolbox.BlockingMonitor';
PRINT '  - SQLToolbox.DeadlockDetector';
PRINT '  - SQLToolbox.LivePerformanceAnalyzer';
PRINT '';
PRINT 'Quick start:';
PRINT '  1) USE [SQLToolbox]';
PRINT '  2) EXEC SQLToolbox.WaitStatsSummary;';
PRINT '  3) EXEC SQLToolbox.TopSlowQueries @TopN = 10, @SortBy = ''CPU'';';
PRINT '  4) EXEC SQLToolbox.BlockingMonitor;';
PRINT '  5) EXEC SQLToolbox.LivePerformanceAnalyzer @TopN = 10;';
PRINT '';
PRINT 'For full workflow, HTML reporting, history, automation, and RunAll orchestration,';
PRINT 'see SQL Toolbox PRO.';
PRINT '===========================================================';
GO