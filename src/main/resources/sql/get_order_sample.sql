SELECT
	device_id,
	user_id,
	entity_id AS item_id,
	exp_id AS app_id,
	time,
	user_define as gmv,
	pos,
	label
FROM
	s_algo_sample
WHERE
	pt = 'YESTERDAY'
	and type = 'order'
	and platform = 'all'