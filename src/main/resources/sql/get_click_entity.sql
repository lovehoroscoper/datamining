SELECT
	did AS device_id_click,
	decode(userid) AS user_id_click,
	decode(iid) AS entity_id_click,
	get_app_id(acm) AS app_id_click,
	tm AS click_time,
	url AS url,
	get_request_id(acm) AS request_id_click
FROM
	acm_click_v1
WHERE
	visit_date = 'BIZDATE'