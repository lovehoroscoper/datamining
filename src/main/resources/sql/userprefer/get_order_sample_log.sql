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
  and user_id not in ('19800')
	and device_id not in ('000000000000000','0000000000000000','Unknown','00000000','0','00000000000000','111111111111111','mgj_2012')