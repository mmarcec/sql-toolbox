/*
===========================================================
Source file: WaitStatsSummary.sql
Included in: Community + PRO

Purpose:
  Show top meaningful SQL Server wait statistics with
  interpretation and recommended next step.
===========================================================
*/

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