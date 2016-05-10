SELECT
	did AS device_id,
	decode(userid) AS user_id,
	decode(iid) AS entity_id,
	'' AS resource_id,
	get_app_id(acm) AS app_id,
	idx AS pos,
	tm AS expose_time,
	url AS refer,
	rid AS request_id
FROM
	acm_expose_v1
WHERE
	visit_date = 'BIZDATE'