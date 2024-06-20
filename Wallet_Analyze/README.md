# Wallet_Analyze

# 1. GET ALL SWAP TRANSACTIONS FROM THE GIVEN WALLET

사용자 지갑을 기준으로 get_asset_transfer를 호출
get_asset_transfer()
params:
api_key: Alchemy API key
wallet_address: 검색할 wallet
router_address: UNISWAP ROUTER, 1INCH ROUTER, SUSHISWAP ROUTER ...


wallet address의 block_in_200days만큼의 블록수만큼 fromAddress: wallet_address, toAddress: router_address (initialize params)

response는 해당 지갑의 모든 transactions를 포함
------------------------------------------------------------------------------------------------------------------------------------------------
# 2. Log Analytics Loggic
# max_concurrent_requests는 etherscan API의 초당 요청 수 제한(현재 5 call per sec)

corutines list에 각각의 item에 대한 Log를 분석하는 메소드 get_transaction()를 추가

get_transaction()
params:
api_key: Alchemy API key
escan_api_key: Etherscan API key
transaction_hash: 각 transaction의 hash (log 데이터 요청을 위해 필요)
wallet_address: given wallet address(topic 데이터 비교를 위해 필요)
timestamp: eth_price 요청을 위한 param

1) len(data) < 2 : 단순 토큰 전송 트랜잭션에 대한 filtering
2) 해당 transaction의 sell token 추출
3) 해당 transaction의 buy token 추출
4) 해당 transaction의 WETH data 계산
5) eth_price 지정
6) sell token과 buy token이 동시에 존재하는 경우는 두개의 dict를 trace_data에 추가(extend)
7) buy token만 존재하는 경우 buy토큰의 dict를 trace_data에 추가(append)
8) sell token만 존재하는 경우 sell토큰의 dict를 trace_data에 추가(append)
--------------------------------------------------------------------------------------------------------------------------------------------------
# 3. sqlite3 
# symbol을 기준으로 create table


------------------------------------------------------------------------------------------------------------------------------------------------

