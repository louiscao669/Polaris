to run multi-server: 
open up docker and make sure it is running
docker compose up --scale api=3 -d (specify number of servers by changing api value)

check the containers: docker compose ps
look at logs in real time: docker compose logs -f api