select
  user_id,
  ENTITY as entity_id,
  time
from
  s_dg_user_base_log
where
  pt = 'BIZDATE'
  and action_type = 'order'
  and platform_type = 'app'