SELECT DISTINCT
    m.name, m.ts, t.high AS mx
FROM (
    SELECT name, ts, high, ROW_NUMBER() OVER (PARTITION BY name ORDER BY high DESC) AS rn 
    FROM sta9760_project3_s3 
) t JOIN sta9760_project3_s3 m ON m.name = t.name AND m.ts = t.ts AND t.rn = 1
order by mx desc
;