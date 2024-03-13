select
    t.hour as hour,
    SUM(e.CO2Emission) as total_co2_emission
from  {{ ref('time_dimension') }} as t
join {{ ref('energy_fact') }} as e
on e.time_id=t.time_id
group by t.hour