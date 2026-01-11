-- <!TableName> and <!Suffix> are replaced at runtime
CREATE TABLE <!TableName><!Suffix> AS
WITH PreparedData AS (
    SELECT *,
           CAST(SUBSTRING(RegTitleNumber, 3) AS UNSIGNED) AS TitleNum,
           ROW_NUMBER() OVER (
               PARTITION BY ProjectName, Owner, Jurisdiction 
               ORDER BY RegTitleNumber
           ) AS seq
    FROM <!TableName>
),
GroupedData AS (
    SELECT *, (TitleNum - seq) AS IslandID
    FROM PreparedData
)
SELECT 
    MIN(RegTitleNumber) AS RegTitleFrom,
    MAX(RegTitleNumber) AS RegTitleTo,
    MIN(ParcelName) AS ParcelNameFrom,
    MAX(ParcelName) AS ParcelNameTo,
    (MAX(TitleNum) - MIN(TitleNum)) AS TitleNumberDistance,
    ANY_VALUE(Area_ha) AS Area_ha,
    Jurisdiction,
    ANY_VALUE(Comments) AS Comments,
    ANY_VALUE(NextDueDate) AS NextDueDate,
    Owner,
    ProjectName,
    MIN(RegDate) AS RegDate,
    MAX(UpdateDate) AS UpdateDate
FROM GroupedData
GROUP BY ProjectName, Owner, Jurisdiction, IslandID
