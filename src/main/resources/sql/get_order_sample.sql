SELECT
	device_id,
	user_id,
	entity_id AS item_id,
	exp_id AS app_id,
	time,
	user_define1 AS gmv,
	"0" as pos,
	label
FROM
	s_algo_sample
WHERE
	pt = 'BIZDATE'
	and type = 'order'
	and platform = 'all'
	and device_id not in ('000000000000000','0000000000000000','Unknown','00000000','0','00000000000000','111111111111111','mgj_2012')