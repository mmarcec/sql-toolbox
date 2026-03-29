/*
===========================================================
Source file: DeadlockDetector.sql
Included in: Community + PRO

Purpose:
  Read recent deadlock graphs from the system_health
  extended events session.
===========================================================
*/

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