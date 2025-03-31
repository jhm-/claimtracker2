-- <!TableName> and <!Suffix> are replaced at runtime
CREATE TABLE <!TableName><!Suffix> AS
WITH RECURSIVE TitleNumberSeries AS (
    SELECT 
        RegTitleNumber AS RegTitleFrom, 
        RegTitleNumber AS RegTitleTo, 
        CAST(SUBSTRING(RegTitleNumber, 3) AS UNSIGNED) AS NumericTitle,
        ROW_NUMBER() OVER (ORDER BY RegTitleNumber) AS rn,
        ParcelName,  -- Include ParcelName here
        Area_ha, Comments, Jurisdiction, NextDueDate, Owner, ProjectName, RegDate, UpdateDate
    FROM <!TableName>
    UNION ALL
    SELECT 
        tns.RegTitleFrom, 
        yt.RegTitleNumber, 
        CAST(SUBSTRING(yt.RegTitleNumber, 3) AS UNSIGNED),
        tns.rn,
        yt.ParcelName,  -- Include ParcelName in the recursive part
        tns.Area_ha, tns.Comments, tns.Jurisdiction, tns.NextDueDate, tns.Owner, tns.ProjectName, tns.RegDate, tns.UpdateDate
    FROM TitleNumberSeries tns
    INNER JOIN <!TableName> yt ON yt.RegTitleNumber = CONCAT(SUBSTRING(tns.RegTitleFrom, 1, 2), tns.NumericTitle + 1)
),
ParcelNames AS ( -- CTE to get the ParcelNames
    SELECT 
        RegTitleFrom,
        MAX(RegTitleTo) AS RegTitleTo,
        MIN(ParcelName) as ParcelNameFrom,
        MAX(ParcelName) as ParcelNameTo,
        CAST(SUBSTRING(MAX(RegTitleTo), 3) AS UNSIGNED) - CAST(SUBSTRING(RegTitleFrom, 3) AS UNSIGNED) AS TitleNumberDistance,
        ANY_VALUE(Area_ha) AS Area_ha,
        ANY_VALUE(Comments) AS Comments,
        ANY_VALUE(Jurisdiction) AS Jurisdiction,
        ANY_VALUE(NextDueDate) AS NextDueDate,
        ANY_VALUE(Owner) AS Owner,
        ANY_VALUE(ProjectName) AS ProjectName,
        ANY_VALUE(RegDate) AS RegDate,
        ANY_VALUE(UpdateDate) AS UpdateDate
    FROM TitleNumberSeries
    GROUP BY RegTitleFrom, rn
)
SELECT * FROM ParcelNames;
DELETE t1 FROM <!TableName><!Suffix> t1
INNER JOIN <!TableName><!Suffix> t2 
WHERE 
    t1.RegTitleFrom <> t2.RegTitleFrom 
    AND t1.RegTitleFrom BETWEEN t2.RegTitleFrom AND t2.RegTitleTo
    AND t1.TitleNumberDistance <= t2.TitleNumberDistance
