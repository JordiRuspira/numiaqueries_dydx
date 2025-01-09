-- CTE0 takes matches table, joins with mempool canceled orders by taker, market and taker order ID
-- We keep those canceled orders with timestamp earlier than match table timestamp (should have been canceled)
-- we start at dec 1
-- We keep those canceled orders with goodtilblock later than match block

WITH CTE0 AS 
(

SELECT
 distinct  
 TIMESTAMP_DIFF( 
    A.block_timestamp, 
    m.publish_time,
    SECOND
  ) as timediff, --Time difference in seconds
  JSON_EXTRACT_SCALAR(m.attributes, '$.timestamp') AS canceled_order_submitted_time, 
  m.publish_time AS canceled_order_numia_publish_time,
  CASE 
    WHEN m.publish_time > A.block_timestamp THEN 'Ok' 
    ELSE 'NOK' 
  END AS test,
  CAST(JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.client_id') AS INT64) AS order_id_canceled,
  JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.subaccount_id.owner') AS subaccount,
  JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.clob_pair_id') AS clob_pair_id, 

m.data, 
    CAST(m.data AS string) AS full_data,
    JSON_EXTRACT_SCALAR(m.attributes, '$.tx_hash') AS tx_hash,
    JSON_EXTRACT_SCALAR(m.attributes, '$.tx_msg_type') AS order_type,
    JSON_EXTRACT_SCALAR(m.attributes, '$.timestamp') AS order_submitted_time,
    m.publish_time AS numia_publish_time,
    CAST(JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.client_id') AS INT64) AS trade_id,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.subaccount_id.owner') AS subaccount,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.clob_pair_id') AS clob_pair_id, 
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.GoodTilOneof.good_til_block') AS good_til_block,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.GoodTilOneof.good_til_block_time') AS good_til_block_time,
  A.*
FROM 
  `numia-data.dydx_mainnet.dydx_match` A 
LEFT JOIN `numia-data.dydx_mainnet.dydx_mempool_transactions` M
ON A.TAKER_ORDER_ID = CAST(JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.client_id') AS INT64) 
AND A.perpetual_id = CAST(
  CASE 
    WHEN JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.clob_pair_id') IS NULL THEN '0' 
    ELSE JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.clob_pair_id') 
  END 
  AS INT64
)
AND A.taker = JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.subaccount_id.owner') 
WHERE 
  1 = 1
  AND JSON_EXTRACT_SCALAR(m.attributes, '$.tx_msg_type') = '/dydxprotocol.clob.MsgCancelOrder'
  AND A.block_height >= 31423923
  AND m.publish_time < A.block_timestamp 
  AND CAST(JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.GoodTilOneof.good_til_block') AS INT) >= A.block_height
  AND TIMESTAMP_DIFF( 
    A.block_timestamp, 
    m.publish_time,
    SECOND
  ) < 240 -- 4 minutes in seconds, arbitrary and for data query reduction purpose

)


SELECT 
CASE WHEN B.PROPOSER_ADDRESS = 'EC340C55235751EDF6F329C601FCF100560C1327' THEN 'Hashkey Cloud'
when B.PROPOSER_ADDRESS = '1BDBD17A3E0612F2FCAEB2A45B4C786D8BD271E6' THEN 'PRO Delegators'
when B.proposer_address = '4B0BD3AFBA9F58C2CF35F952F9B06C0DE741C56C' THEN 'OKX Earn'
else 'other' end as proposer,
b.block_height,
A.*
FROM CTE0 A 
LEFT JOIN `numia-data.dydx_mainnet.dydx_blocks` B 
ON A.BLOCK_HEIGHT = B.block_height
WHERE B.block_height > 31423923
AND B.PROPOSER_ADDRESS IN ('EC340C55235751EDF6F329C601FCF100560C1327', '1BDBD17A3E0612F2FCAEB2A45B4C786D8BD271E6', '4B0BD3AFBA9F58C2CF35F952F9B06C0DE741C56C')  
LIMIT 50
