# POC RabbitMq

This is one poc with frontend talking via `ws` to one services layer and then the services talking to the other layers via `amqp` using RabbitMq.

```

THE WHOLE THING LACK TESTING AND I NEED TO SPEND TIME MAKING EVERYTHING BULLET PROOF. BUT FOR THE POC´S SAKE IT IS OK. SO PLEASE BE KIND.

```

## Folder structure / services
- client - React client that will connect to one services agregator. The client will listen on port 3000;
- services_agregator - Service that will receive client connections and bridge to rabbit. This service 
    has to expose three diferent ports so that client connects to (4001,4002,4003). If you want fail-over, start the new service with 5001, 5002, 5003 since it is what client is expecting.
    The service will start publishing on a fanout(pub/sub) queue on a regular interval where agregators consume, so that all other agregators can ensure message delivery on fail-over scenario.
- server - The actual server instance that will receive events from rabbit;
- external_api - One api exposed with three POST routes on port 4000:
    - /external/voice
    - /external/chat
    - /external/tickets
- shared - Some shared classes between all the services. Should go as npm private modules.
- settings - Has connection settings(configs.json), broker configurations settings(brokerConfigs.json) and one script to create all the system queues (for testing only)

## TODO
- [ ] The agregator failover does not seem to be behaving like it should on stress situations.
- [ ] Guarantee the acknowledgement of all the messages in all the situations.
- [ ] Configure messages ttl´s.
- [x] Leaking socket connections.
- [ ] Spark if persistence and make services_agregator stateless to support beeing all agregators down and persist communications.
- [ ] Refactor Broker service to use promises

## Architecture

![alt text](RabbitMqPOC.png "Architecture")

Broker on heroku : 
- Consumers url - amqp://3GdtrY_s:78ov3IyCFAxyMNpysTBMt8AgR38PZnik@sad-silver-53.bigwig.lshift.net:10003/6TAe0JLKstBE
- Publisher url - amqp://3GdtrY_s:78ov3IyCFAxyMNpysTBMt8AgR38PZnik@sad-silver-53.bigwig.lshift.net:10002/6TAe0JLKstBE
- Web interface - https://bigwig.lshift.net/management/192272/

## Scenario setup - RabbitMq
### Install server and management plugin
- Install server - https://www.rabbitmq.com/download.html
- Install management plugin - https://www.rabbitmq.com/management.html
    - By default the management plugin comes with the user guest and password guest.

### Init slave
``` 
RABBITMQ_NODE_PORT=5674 RABBITMQ_NODENAME=slave@localhost RABBITMQ_SERVER_START_ARGS=" -rabbitmq_management listener [{port,15674}] -rabbitmq_mqtt tcp_listeners [1884]" rabbitmq-server
```

### Turn off some plugins (Just an example, turn off the uneded ones)
```
rabbitmq-plugins -n slave@localhost --offline rabbitmq_stomp
rabbitmq-plugins -n slave@localhost --offline rabbitmq_stomp
```

### Add the slave to the cluster
```
rabbitmqctl -n slave@localhost stop_app
rabbitmqctl -n slave@localhost join_cluster rabbit@localhost
rabbitmqctl -n slave@localhost start_app
```
### Set the queue mirroring policy (For two slaves here. Can be more)
```
rabbitmqctl set_policy ha-all "^" \ '{"ha-mode":"exactly","ha-params":2,"ha-sync-mode":"automatic"}'
```

### Troubleshooting
```
rabbitmqctl -n rabbit@localhost stop_app
rabbitmqctl -n slave@localhost stop_app

rabbitmqctl -n slave@localhost reset
rabbitmqctl -n rabbit@localhost reset

rabbitmqctl -n rabbit@localhost start_app
rabbitmqctl -n slave@localhost start_app
```
- Happened to me that mnesia database was corrupted because i lost battery on my laptop. In order to fix that i issued the command `rabbitmq -n {node} force_boot`. Can be usefull in many situations

## Scenario setup - Nodes
Please do not look at the code. Not my best face. Start two instance of each type to see the redundancy scenarios. You will need to install the dependencies on each service via `npm i`.

Each service is on the intuitive respective folder mentioned above.

Please be concise with the broker you are connecting to. Or localhosting it or not. We can´t have two services communicating with diferent brokers.

- Client.
    - `npm start`
- services_agregator - Start the two instances for master and slave. 6th arg is the host where it will run, 3th, 4th and 5th are the Voice, chat and tickets ports respectively. The localhost arg will connect to whatever you put on configs.json file localhost* keys. If you omit localhost, it will connect to one broker setted up on heroku for you.
    - `npm start 4001 4002 4003 localhost`
    - `npm start 5001 5002 5003 localhost`    
- server - he localhost arg will connect to whatever you put on configs.json file localhost* keys. If you omit localhost, it will connect to one broker setted up on heroku for you.
    - `npm start localhost`        
    - `npm start localhost`        
- external_api. The service will expose three POST routes
    - `npm start 4000 localhost`

