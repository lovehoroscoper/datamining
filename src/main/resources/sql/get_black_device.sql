SELECT 
  distinct id
 FROM 
  s_algo_user_black_set
 WHERE 
  pt >= 'BIZDATE_SUB7'
  AND type = 'device_id'