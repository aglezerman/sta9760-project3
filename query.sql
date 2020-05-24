select distinct
       name,
       date(cast(ts as timestamp)) as "date",
       extract(hour from cast(ts as timestamp)) hour, 
       max(high) OVER (PARTITION BY name, extract(hour from cast(ts as timestamp)) ORDER BY high DESC) AS rn 
from sta9760_project3_s3 
order by name, hour asc