-- Matched example ID 

SELECT * FROM 
  `numia-data.dydx_mainnet.dydx_match`  A
WHERE TAKER = 'dydx1z6eelhaspk2dn0rl50uvv5c8ace57rymn2hlaq' 
AND TAKER_ORDER_ID = 933993928


;

-- Placed order via mempool table, returns tx 2E9EA9DD9BA00E7D3835D40A370399D468029383DBBFF96719CDD34C66289EF9

SELECT 
m.data,
    m.attributes,
    CAST(m.data AS string) AS full_data,
    JSON_EXTRACT_SCALAR(m.attributes, '$.tx_hash') AS tx_hash,
    JSON_EXTRACT_SCALAR(m.attributes, '$.tx_msg_type') AS order_type,
    JSON_EXTRACT_SCALAR(m.attributes, '$.timestamp') AS order_submitted_time,
    m.publish_time AS numia_publish_time,
    CAST(JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.order_id.client_id') AS INT64) AS trade_id,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.order_id.subaccount_id.owner') AS subaccount,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.order_id.clob_pair_id') AS clob_pair_id,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.quantums') AS trade_volume,
    CASE
      WHEN JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.side') = '1' THEN 'buy'
    ELSE
    'sell'
  END
    AS side,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.subticks') AS subticks,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.GoodTilOneof.good_til_block') AS good_til_block,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.GoodTilOneof.good_til_block_time') AS good_til_block_time,
    case when b.taker is null then 'not matched' else 'matched' end as match
  FROM
    `numia-data.dydx_mainnet.dydx_mempool_transactions` m 
    left join   `numia-data.dydx_mainnet.dydx_match` b
    on CAST(JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.order_id.client_id') AS INT64) = b.taker_order_id

  WHERE
    1 = 1
    AND JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.order_id.subaccount_id.owner') = 'dydx1z6eelhaspk2dn0rl50uvv5c8ace57rymn2hlaq'
    AND JSON_EXTRACT_SCALAR(m.attributes, '$.tx_msg_type') = '/dydxprotocol.clob.MsgPlaceOrder'   
    AND JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.order_id.clob_pair_id') is null
    AND CAST(JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order.order_id.client_id') AS INT64) IN (933993928)
    --AND TIMESTAMP(m.publish_time) <= TIMESTAMP('2024-12-05 18:16:55')
  ORDER BY CAST(m.publish_time AS STRING) desc  

  LIMIT 10
 

 ;

-- Same order is not found in place_order table, neither by order ID nor by tx hash

SELECT * FROM `numia-data.dydx_mainnet.dydx_place_order` 
WHERE ORDER_ID = 933993928
