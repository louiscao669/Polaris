To run the correctness test (first mint a jwt token), run: 
 
export BENCHMARK_JWT="$(curl -sS -X POST 'https://d2vrkasldxh3jt.cloudfront.net/auth/login' \
  -H 'Content-Type: application/json' \
  -d '{"username":"alex","password":"abc123456"}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')"
python backend/event_bus/tests/test_request.py

To run the read benchmark:
The first run can take ~1 min
python3 backend/event_bus/benchmarks/market_read_benchmark.py \
  --base-url "https://d2vrkasldxh3jt.cloudfront.net" \
  --requests 1000 \
  --cache-mode default \
  --concurrency-sweep 2,4,8,16,32,64,100 \
  --auto-pick-role-users \
  --org-id 3 \
  --engineer-role-id engineer \
  --marketing-role-id marketing \
  --engineer-sample-size 100 \
  --marketing-sample-size 100 \
  --role-pick-seed 42 \
  --engineer-market-start 1 \
  --engineer-market-count 15 \
  --marketing-market-start 16 \                                    
  --marketing-market-count 15 \
  --db-host "polaris-db.clmsauq4mqfc.us-east-2.rds.amazonaws.com" \
  --db-port 3306 \              
  --db-user "PolarisAdmin" \
  --db-password "Polarishorse" \
  --db-name "polarisDB" \
  --request-timeout 60 \        
  --retry-attempts 4 \                             
  --retry-backoff-seconds 0.25


To run a mini version of our write benchmark (full benchmark takes a while):
polaris % python3 backend/event_bus/benchmarks/market_bet_benchmark_v1.py \
  --base-url "https://d2vrkasldxh3jt.cloudfront.net" \
  --jwt "$BENCHMARK_JWT" \
  --user-ids "18,19,20" \
  --per-request-x-user-id \
  --market-id 1 \
  --token-id 1 \
  --requests 50 \
  --concurrency-sweep 2,4,8,16,32 \
  --qty 1 \
  --side 1 \
  --transaction-type BUY \
  --request-timeout 20 \
  --poll-timeout 60 \
  --poll-interval 1
