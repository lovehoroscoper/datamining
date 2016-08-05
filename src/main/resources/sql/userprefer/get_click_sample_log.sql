select
  user_id,
  ENTITY as entity_id,
  time
from
  s_dg_user_base_log
where
  pt = 'BIZDATE'
  and action_type = 'click'
  and platform_type = 'app'