-- Example of a canceled order


SELECT 
m.data,
    m.attributes,
    CAST(m.data AS string) AS full_data,
    JSON_EXTRACT_SCALAR(m.attributes, '$.tx_hash') AS tx_hash,
    JSON_EXTRACT_SCALAR(m.attributes, '$.tx_msg_type') AS order_type,
    JSON_EXTRACT_SCALAR(m.attributes, '$.timestamp') AS order_submitted_time,
    m.publish_time AS numia_publish_time,
    CAST(JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.client_id') AS INT64) AS trade_id,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.subaccount_id.owner') AS subaccount,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.order_id.clob_pair_id') AS clob_pair_id, 
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.GoodTilOneof.good_til_block') AS good_til_block,
    JSON_EXTRACT_SCALAR(CAST(m.data AS string), '$.GoodTilOneof.good_til_block_time') AS good_til_block_time
  FROM
    `numia-data.dydx_mainnet.dydx_mempool_transactions` m 
  WHERE
    1 = 1
    AND JSON_EXTRACT_SCALAR(m.attributes, '$.tx_msg_type') = '/dydxprotocol.clob.MsgCancelOrder'   
  AND JSON_EXTRACT_SCALAR(m.attributes, '$.tx_hash')  = '76C54A0E1D7E2A8D735315BA165092C8E0C0303FCC1D5BD3478D136B9B8F7880'
  LIMIT 200
 

;

-- We cannot find it in the cancel order table, neither by order ID nor by tx hash

SELECT * FROM `numia-data.dydx_mainnet.dydx_cancel_order` 
WHERE ORDER_ID = 1724040439