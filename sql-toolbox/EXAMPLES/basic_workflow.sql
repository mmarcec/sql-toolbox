/*
===========================================================
SQL TOOLBOX COMMUNITY - BASIC WORKFLOW

Purpose:
  Example of how to use the toolkit in a real troubleshooting flow.

Recommended order:
  1) Start with waits
  2) Find expensive queries
  3) Check blocking
  4) Review current live activity
  5) Check recent deadlocks

Notes:
  - Community Edition is manual by design
  - SQL Toolbox PRO adds RunAll, HTML reporting,
    history, health score, and automation
===========================================================
*/

USE [SQLToolbox];
GO

PRINT '===========================================================';
PRINT 'SQL TOOLBOX COMMUNITY - BASIC WORKFLOW';
PRINT '===========================================================';
PRINT '';

-----------------------------------------------------------
-- 1) What is SQL Server waiting on?
-----------------------------------------------------------
PRINT '1) WAIT STATS SUMMARY - WHERE TO LOOK FIRST';
EXEC SQLToolbox.WaitStatsSummary @TopN = 10;
GO

-----------------------------------------------------------
-- 2) Which cached queries are most expensive?
-----------------------------------------------------------
PRINT '2) TOP SLOW QUERIES - WHAT IS USING CPU / DURATION';
EXEC SQLToolbox.TopSlowQueries
    @TopN = 10,
    @SortBy = N'CPU';
GO

-----------------------------------------------------------
-- 3) Is there active blocking right now?
-----------------------------------------------------------
PRINT '3) BLOCKING MONITOR - IS SOMEONE BLOCKING OTHERS';
EXEC SQLToolbox.BlockingMonitor;
GO

-----------------------------------------------------------
-- 4) What is happening right now?
-----------------------------------------------------------
PRINT '4) LIVE PERFORMANCE ANALYZER - WHAT IS RUNNING NOW';
EXEC SQLToolbox.LivePerformanceAnalyzer @TopN = 10;
GO

-----------------------------------------------------------
-- 5) Were there any recent deadlocks?
-----------------------------------------------------------
PRINT '5) DEADLOCK DETECTOR - CHECK RECENT DEADLOCKS';
EXEC SQLToolbox.DeadlockDetector;
GO

PRINT '';
PRINT '===========================================================';
PRINT 'WORKFLOW COMPLETE';
PRINT '===========================================================';
PRINT 'Tip: If you run these steps repeatedly, SQL Toolbox PRO';
PRINT 'adds one-command execution, HTML reports, history,';
PRINT 'health scoring, and automation.';
PRINT '===========================================================';
GO