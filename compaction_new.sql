-- <!TableName> and <!Suffix> are replaced at runtime

CREATE TABLE <!TableName><!Suffix> AS
WITH PreparedData AS (
    SELECT *,
        REGEXP_SUBSTR(RegTitleNumber, '^[A-Za-z]+') AS TitlePrefix,
        CAST(REGEXP_SUBSTR(RegTitleNumber, '[0-9]+$') AS UNSIGNED) AS TitleNum,
        SUBSTRING_INDEX(ParcelName, ' ', 1) AS ParcelPrefix,
        CAST(SUBSTRING_INDEX(ParcelName, ' ', -1) AS UNSIGNED) AS ParcelNum
    FROM <!TableName>
),
SequencedData AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ProjectName, NextDueDate, TitlePrefix
            ORDER BY TitleNum
        ) AS TitleSeq,
        ROW_NUMBER() OVER (
            PARTITION BY ProjectName, NextDueDate, ParcelPrefix
            ORDER BY ParcelNum
        ) AS ParcelSeq
    FROM PreparedData
),
GroupedData AS (
    SELECT *,
        (CAST(TitleNum AS SIGNED) - CAST(TitleSeq AS SIGNED)) AS TitleIslandID,
        (CAST(ParcelNum AS SIGNED) - CAST(ParcelSeq AS SIGNED)) AS ParcelIslandID
    FROM SequencedData
)
SELECT
    ProjectName,
    Jurisdiction,
    CONCAT(ParcelPrefix, ' ', MIN(ParcelNum)) AS ParcelNameFrom,
    NULLIF(CONCAT(ParcelPrefix, ' ', MAX(ParcelNum)), CONCAT(ParcelPrefix, ' ', MIN(ParcelNum))) AS ParcelNameTo,
    CONCAT(TitlePrefix, MIN(TitleNum)) AS RegTitleFrom,
    NULLIF(CONCAT(TitlePrefix, MAX(TitleNum)), CONCAT(TitlePrefix, MIN(TitleNum))) AS RegTitleTo,
    (MAX(TitleNum) - MIN(TitleNum)) AS TitleNumberDistance,
    ANY_VALUE(Owner) AS Owner,
    MIN(RegDate) AS RegDate,
    NextDueDate,
    MAX(UpdateDate) AS UpdateDate,
    ANY_VALUE(Comments) AS Comments
FROM GroupedData
GROUP BY ProjectName, Jurisdiction, NextDueDate, TitlePrefix, ParcelPrefix, TitleIslandID, ParcelIslandID
ORDER BY ProjectName, NextDueDate, TitlePrefix, MIN(TitleNum)
