# POC RabbitMq

This is one poc with frontend talking via `ws` to one services layer and then the services talking to the other layers via `amqp` using RabbitMq.

```

THE WHOLE THING LACK TESTING AND I NEED TO SPEND TIME MAKING EVERYTHING BULLET PROOF. BUT FOR THE POC´S SAKE IT IS OK. SO PLEASE BE KIND AND SUBMIT ONE ISSUE.

```

## TODO
- [ ] Create configs to broker addresses, ports, etc
- [ ] Some messages stick with the queue because of not beeing ack´d. THis may be because of the agregator failover. The original socked, never acks the original message Only the slave connection tries to do it, but ona diferent tag. 
- [ ] The agregator failover does not seem to be behaving like it should on stress situations.
- [ ] Guarantee the acknowledgement of all the messages in all the situations.
- [ ] Configure messages ttl´s.

## Architecture

![alt text](RabbitMqPOC.png "Architecture")

Broker on heroku : 
- Consumers url - amqp://3GdtrY_s:78ov3IyCFAxyMNpysTBMt8AgR38PZnik@sad-silver-53.bigwig.lshift.net:10003/6TAe0JLKstBE
- Publisher url - amqp://3GdtrY_s:78ov3IyCFAxyMNpysTBMt8AgR38PZnik@sad-silver-53.bigwig.lshift.net:10002/6TAe0JLKstBE
- Web interface - https://bigwig.lshift.net/management/192272/

## Scenario setup - RabbitMq
Rabbitmq-server
https://www.vultr.com/docs/how-to-install-rabbitmq-on-centos-7

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
Please do not look at the code. Not my best face. Start two instance of each type to see the redundancy scenarios

Other thinf. Make 

- Client - The client will connect to service_agregator master node in voice, chat and tickets on 4001, 4002 and 4003 ports respectively. The slave is one 5001, 5002, 5003.
    - `yarn start`
- external_api
    - `npm start`
- server - You can ommit the localhost (third args parameter) and it will connect to heroku broker. 
    - `npm start localhost`        
    - `npm start localhost`        
- services_agregator - Start the two instances for master and slave.
    - `npm localhost 4001 4002 4003`
    - `npm localhost 5001 5002 5003`
