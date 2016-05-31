SELECT
	device_id,
	user_id,
	entity_id AS item_id,
	exp_id AS app_id,
	time,
	pos,
	label
FROM
	s_algo_sample
WHERE
	pt = 'BIZDATE'
	and type = 'click'
	and platform = 'all'
	and user_id not in ('19800')
	and device_id not in ('000000000000000','0000000000000000','Unknown','00000000','0','00000000000000','111111111111111','mgj_2012')