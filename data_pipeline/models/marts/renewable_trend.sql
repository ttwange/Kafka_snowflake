select
    t.year,
    t.month,
    t.day,
    t.hour,
    f.SolarPower,
    f.OffshoreWindPower,
    f.OnshoreWindPower
from  {{ ref('time_dimension') }} as t
join {{ ref('energy_fact') }} as e
on e.time_id=t.time_id
