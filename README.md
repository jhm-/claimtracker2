**Claimtracker2 v. 0.3.0 "Minimum Viable Product"**

Requirements: pandas, SQLAlchemy, pygsheets, configparser, mysql-replication, pyodbc, bmi-arcgis-restapi, flask_wtf,
              cron-converter

This software has the basic feature set, and a great deal of brittle code. A more exhaustive README will describe the
software in more detail, once it achieves better stability.

It is designed to be used in conjunction with QGIS processing algorithms (in progress).

Immediate goals: cleanup; better exception handling; Claimtable.update() method external use to application; unit tests

Subsequent: add the final functions to the top bar (import); increase portability (from MySQL and QGIS);
calendar view for dates in browser; can we get the iframe working or render as HTML directly?
