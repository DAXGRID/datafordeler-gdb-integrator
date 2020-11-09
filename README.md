# datafordeler-gdb-integrator
This is a consumer application which reads from a Kafka topic and inserts the data into a relational database.

## Requirements
* Docker

## Configure environment variable
```
. ./development/docker-compose.yml
```
## Building
```
docker build -t datafordelerenconsumer
```

## Running
```
docker-compose up
```
