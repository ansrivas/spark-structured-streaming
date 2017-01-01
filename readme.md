#### This is an example of structured-streaming with latest Spark v2.1.0.
##### A spark job reads from Kafka topic, manipulates data as datasets/dataframes and writes to Cassandra.


#### Usage:

1. Inside `setup` directory, run `docker-compose up -d` to launch instances of `zookeeper`, `kafka` and `cassandra`

2. Wait for a few seconds and then run `docker ps` to make sure all the three services are running.

3. Then run `pip install -r requirements.txt`

4. `main.py` generates some random data and publishes it to a topic in kafka.

5. Run the spark-app using `sbt clean compile run` in a console. This app will listen on topic (check Main.scala) and writes it to Cassandra.

6. Again run `main.py` to write some test data on a kafka topic.

7. Finally check if the data has been published in cassandra.
  * Go to cqlsh `docker exec -it cas_01_test cqlsh localhost`
  * And then run `select * from my_keyspace.test_table  ;`

Credits:

* This repository has borrowed some snippets from [killrweather](https://github.com/killrweather/killrweather) app.
