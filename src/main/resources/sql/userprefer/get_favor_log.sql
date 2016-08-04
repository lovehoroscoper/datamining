select
  user_id,
  ENTITY as entity_id,
  time
from
  s_dg_user_base_log
where
  pt >= 'BIZDATESUBA'
  and pt <= 'BIZDATESUBB'
  and action_type = 'favor'