/*
===========================================================
Source file: BlockingMonitor.sql
Included in: Community + PRO

Purpose:
  Show currently blocked requests with session, wait,
  and running statement details.
===========================================================
*/

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