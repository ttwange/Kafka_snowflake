select 
    distinct Minutes1UTC as time_id,
    date_part('year', Minutes1UTC) as year,
    date_part('month', Minutes1UTC) as month,
    date_part('day', Minutes1UTC) as day,
    date_part('hour', Minutes1UTC) as hour,
from {{ ref('stg_energy') }};