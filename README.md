![SQL Toolbox Banner](https://raw.githubusercontent.com/mmarcec/sql-toolbox/main/DOCS/sql-toolbox-banner.png)

# SQL Toolbox

A practical SQL Server performance diagnostic toolkit built from real production troubleshooting.

Designed for developers and DBAs who need fast answers when a SQL Server database becomes slow.

---

## What is SQL Toolbox?

SQL Toolbox is a collection of SQL Server diagnostic scripts that help identify performance problems quickly.

Typical issues it helps investigate:

- slow SQL Server performance
- blocking sessions
- missing indexes
- long running queries
- database health problems
- performance regressions

The goal is simple:

When a database is slow, developers should be able to find the root cause in minutes — not hours.

---

## Features

SQL Toolbox Community Edition includes:

- SQL Server performance diagnostic scripts
- server-wide database scan
- missing index impact analysis
- blocking session detection
- slow query analysis
- database health checks

The toolkit runs entirely in **T-SQL**.

No agents.  
No services.  
No external dependencies.

---

## Example Usage

Run the main diagnostic procedure:

```sql
EXEC SQLToolbox.RunAll;
```

This performs a structured diagnostic scan and highlights the most important performance issues.

---

## Typical Use Cases

SQL Toolbox helps when:

- a SQL Server database suddenly becomes slow
- blocking chains appear in production
- performance degrades after deployment
- you want to perform a quick SQL Server health check
- you need a fast diagnostic overview of a server

---

## Installation

Run the installation script included in the repository:

```
SQLToolbox_INSTALL.sql
```

After installation:

```sql
EXEC SQLToolbox.RunAll;
```

---

## SQL Server Performance Diagnostic Scripts

SQL Toolbox was created as a practical toolkit for SQL Server performance troubleshooting.

It can help identify:

- missing indexes
- blocking sessions
- slow queries
- database health issues
- potential performance bottlenecks

---

## Community vs PRO Version

The Community Edition provides core diagnostic scripts.

The **PRO version** adds:

- structured installer
- automation via SQL Agent jobs
- advanced index analysis
- HTML performance reports
- performance history tracking
- utility framework for maintenance tasks

---

## When to Use the PRO Version

The Community Edition is perfect for quick diagnostics.

You might want the PRO version if you need:

- automated performance scans
- structured installation
- HTML performance reports
- SQL Agent job automation
- extended diagnostic modules

The PRO version builds on the same core scripts but adds automation and production-ready workflows.

👉 PRO version available here:  
[GUMROAD LINK](https://mariovista01.gumroad.com/l/SQLToolBox_PRO)


---

## Roadmap

SQL Toolbox is actively developed.

Planned improvements include:

- smarter index advisor
- Query Store integration
- improved HTML reporting
- extended performance diagnostics

---

## Support the Project

If you find this project useful:

⭐ Star the repository  
🛠 Try the PRO version

---

## Philosophy

SQL Toolbox was built from real production troubleshooting experience.

The focus is simple:

Provide practical SQL Server diagnostic tools that help developers
understand performance problems quickly.

---

## Example HTML Report

![SQL Toolbox Report](DOCS/sqltoolbox-report.png)