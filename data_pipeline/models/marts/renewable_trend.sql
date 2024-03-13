select
    t.year,
    t.month,
    t.day,
    t.hour,
    f.SolarPower,
    f.OffshoreWindPower,
    f.OnshoreWindPower
from {{ ref('dimensions.time_dimension') }} as t
join {{ ref('facts.energy_fact') }} as f on f.time_id = t.time_id
