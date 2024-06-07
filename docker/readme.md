## remove all docker
```shell
docker stop $(docker ps -aq)
docker rm $(docker ps -aq)

docker rmi $(docker images -q)

docker volume rm $(docker volume ls -q)

docker network rm $(docker network ls -q)

docker system prune -a --volumes -f
```


## prepare
```shell
chmod +x init_airflow_db.sh
chmod +x entrypoint.sh
```

## build docker 
```shell
docker-compose build --no-cache
docker-compose up -d
docker-compose ps
```

# init airflow db
```shell
docker-compose exec airflow airflow db init
```

## create airflow user
```shell
docker-compose exec airflow airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

## run init shell
```shell
docker exec -it  docker_airflow_1 sh
python /opt/airflow/tools/init_ak_dag_db.py
python /opt/airflow/dags/dg_ak/store_daily/s-zh-a/init_dg_ak_s_zh_a.py
```

## run airflow
```shell
docker-compose exec airflow airflow webserver
docker-compose exec airflow airflow scheduler
```

## Done
open browse and visit website 'http://localhost:8080'