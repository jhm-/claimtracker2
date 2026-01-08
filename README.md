**Claimtracker2 v. 0.2.1 "Minimum Viable Product"**

Requirements: pandas, SQLAlchemy, pygsheets, configparser, pyodbc, bmi-arcgis-restapi, flask_wtf, cron-converter

This software has the basic feature set, and a great deal of brittle code. A more exhaustive README will describe the
software in mode detail, once it achives better stability.

It is designed to be used in conjunction with QGIS processing algorithms (in progress).

Immediate goals: cleanup; better exception handling; Claimtable.update() method external use to application; unit tests

Proximate goal: compaction is not working; validate cron dates

Subsequent: add the final functions to the top bar (update, import); increase portability (from MySQL and QGIS);
calendar view for dates in browser; can we get the iframe working?
(nb. if the iframe works, replace ClaimTable.sheet1.url with ClaimTable.address with the parameter 'rm=minimal' appended)
