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
PRINT '1) WAIT STATS SUMMARY';
EXEC SQLToolbox.WaitStatsSummary @TopN = 10;
GO

-----------------------------------------------------------
-- 2) Which cached queries are most expensive?
-----------------------------------------------------------
PRINT '2) TOP SLOW QUERIES';
EXEC SQLToolbox.TopSlowQueries
    @TopN = 10,
    @SortBy = N'CPU';
GO

-----------------------------------------------------------
-- 3) Is there active blocking right now?
-----------------------------------------------------------
PRINT '3) BLOCKING MONITOR';
EXEC SQLToolbox.BlockingMonitor;
GO

-----------------------------------------------------------
-- 4) What is happening right now?
-----------------------------------------------------------
PRINT '4) LIVE PERFORMANCE ANALYZER';
EXEC SQLToolbox.LivePerformanceAnalyzer @TopN = 10;
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