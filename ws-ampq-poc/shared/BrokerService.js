const amqp = require('amqplib/callback_api');
const EventEmitter = require('events');
const brokerConfigs = require('../settings/brokerConfigs.json');

module.exports = class BrokerService extends EventEmitter {
    constructor(host, slavesConsumer=[], slavesPublisher=[]) {
        super();
        console.log('[BROKER SERVICE] New connection to '+JSON.stringify(host));
        this.masterDiscoveryTimeout = 3000;
        this.CONN_REFUSED_ERR = 'ECONNREFUSED';
        this.CHANNEL_CLOSE_ERR = 'Channel closed'
        this.isBrokerConnected = false;
        this.discoverdMaster = false;    

        this.publishedSparkIds = [];

        this.brokerUriConsumer = `amqp://${host.rx}`;
        this.brokerUriProducer = `amqp://${host.tx}`;

        this.activeChannels = [];

        this.configs = brokerConfigs;

        this.slavesConsumer = slavesConsumer.map( slave => {
            return `amqp://${slave}`;
        });

        this.slavesPublisher = slavesPublisher.map( slave => {
            return `amqp://${slave}`;
        });      
        
        this.socketOptions = {
            heartbeat: this.configs.heartbeat,
        }
        
    }        

    /**
     * Start the master discovery process.
     * 
     * There is a timeout. Each tick, will try to connect to master or slave
     */
    startMasterDiscovery() {    
        this.discoverdMaster = false    
        console.log('[BROKER SERVICE] Master lookup');
        amqp.connect(this.brokerUriProducer, this.socketOptions, (err, conn) => {            
            if (err && err.code === 'ECONNREFUSED') {
                return setTimeout( () => {
                    this.startMasterDiscovery();   
                    conn.close();                 
                }, this.masterDiscoveryTimeout);                
            }                        
            this.onMasterDiscovery()            
        });
    }

    /**
     * Force logout remaining connections if active.
     * 
     * Just to prevent something to be connected even if it is not used.     
     */
    forceLogout(channels) {
        this.activeChannels = channels.filter( channel => {
            try{
                channel.close();                
            }catch(e) {
                if (e.message !== this.CHANNEL_CLOSE_ERR) {
                    console.log('[BROKER SERVICE] Exception forceLogout ',e);                
                }                
            }            
            return false;
        }); 
    }

    /**
     * Consume one queue
     */
    consume(queue, channel ,ack = false, exchange = null, prefetch = 0, slave = undefined, slaveIndes = 0) {        
        amqp.connect(slave ? slave : this.brokerUriConsumer, this.socketOptions,  (err, conn) => {
            if (err && err.code === this.CONN_REFUSED_ERR) {                               
                console.log(`[BROKER SERVICE] [ERROR] CONNECTING TO BROKER. Will retry with slave index ${slaveIndes} URL:${this.slavesConsumer[slaveIndes]}`);
                const newSlaveIndex = slaveIndes + 1;
                if (slaveIndes === 0) {
                    this.startMasterDiscovery();
                }
                if (newSlaveIndex > this.slavesConsumer.length) {
                    console.log(`[BROKER SERVICE] [ERROR] COULD NOT RECONNECT TO SLAVES`); 
                    return;                     
                }
                return this.consume(queue,channel,ack,exchange,prefetch,this.slavesConsumer[slaveIndes], newSlaveIndex);
            }
            
            this.isBrokerConnected = true;
            conn.on('close',this.onConnectionClose.bind(this));

            conn.createChannel((err, ch) => {   
                this.activeChannels.push(ch);                                             
                exchange !== null && ch.assertExchange(exchange.name, exchange.type, {
                    durable: exchange.durable,
                    autoDelete: exchange.autoDelete,                                   
                });
                prefetch > 0 && ch.prefetch(prefetch);
                ch.assertQueue(queue.name, {
                    exclusive: queue.exclusive,                    
                    durable: queue.durable,
                    autoDelete: queue.autoDelete,
                }, (err, q) => {
                    console.log('[BROKER SERVICE] Waiting for logs.',q);
                    exchange !== null && ch.bindQueue(q.queue, exchange.name, queue.key);
                    ch.consume(q.queue, (msg) => {  
                        console.log(`[BROKER SERVICE] New data on ${queue.key}`)                                         
                        this.emit('received_queue_data', queue, msg, channel, () => {
                            ack && ch.ack(msg);
                            !ack && conn.close();
                        } );
                    }, {noAck: !ack});
                });
            });
        });
    }

    /** 
     * Publish on one exchange
    */
    publish(exchange, key, msg, slave=undefined, slaveIndes=0) {
        amqp.connect(slave ? slave : this.brokerUriProducer,  this.socketOptions, (err, conn) => {
            if (err && err.code === this.CONN_REFUSED_ERR) {                               
                console.log(`[ERROR] CONNECTING TO BROKER. Will retry with slave index ${slaveIndes} URL:${this.slavesPublisher[slaveIndes]}`);
                const newSlaveIndex = slaveIndes + 1;
                if (slaveIndes === 0) {
                    this.startMasterDiscovery();
                }
                if (newSlaveIndex > this.slavesPublisher.length) {
                    console.log(`[BROKER SERVICE] [ERROR] COULD NOT RECONNECT TO SLAVES`); 
                    return;                     
                }
                return this.publish(exchange, key,msg, this.slavesPublisher[slaveIndes], newSlaveIndex);
            }                           
            if(!conn) {
                return this.publish(exchange, key,msg, this.slavesPublisher[slaveIndes], 0);
            }         
            this.isBrokerConnected = true;
            conn.on('close',this.onConnectionClose.bind(this));

            conn.createConfirmChannel( (err, ch) => {                                
                ch.assertExchange(exchange.name, exchange.type, {
                    durable: exchange.durable,                    
                    autoDelete: exchange.autoDelete,                    
                });
                
                const newMsg = typeof msg.payload !== 'string' ? JSON.stringify(msg.payload) : msg.payload;
                ch.publish(exchange.name, key, new Buffer.from(newMsg), {
                    contentType: 'application/json',
                    persistent: msg.persistent
                }, () => { conn.close() });
                
                console.log(`[BROKER SERVICE] New publish ${key}`);
            });        
        });
    }

    /**
     * Utility method to consume several queues from several types.
     * 
     * It even sees if, depending on the queue we are trying to consume, it should publish enything.
     * It is used for the spark on each channel.
     */
    consumeChannels(channels, queueType, spark=undefined, userId=undefined, socketChannel=undefined, shoudAckMsgs = true) {
        channels.forEach( channel => {
            queueType.forEach( type => {
                const queue = {
                    name : this.configs[channel][type].queue.name,
                    durable : this.configs[channel][type].queue.durable,
                    autoDelete : this.configs[channel][type].queue.autoDelete,
                    exclusive : this.configs[channel][type].queue.exclusive,
                    key : this.configs[channel][type].queue.bindingKey,
                }
                const exchange = {
                    name : this.configs[channel][type].exchange.name,
                    type : this.configs[channel][type].exchange.type,
                }
                this.consume(queue, this.configs[channel].channel, true, exchange);
            })        
        })  
        this.publishSparkIfNeeded(channels, userId, spark, socketChannel)      
    }

    /**
     * Utility method to publish on a give exchange. 
     * 
     * Its just used internally. Not really needed, it is here dont know why. TODO: remove it
     */
    publishInExchange(channel, queueType, msg, persistMsg = true) {
        const exchange = {
            name : this.configs[channel][queueType].exchange.name,
            type : this.configs[channel][queueType].exchange.type,
            durable: this.configs[channel][queueType].exchange.durable,
            autoDelete: this.configs[channel][queueType].exchange.autoDelete,
        }
        const routingKey = this.configs[channel][queueType].queue.bindingKey;      
        this.publish(exchange, routingKey, {persistent : persistMsg, payload : JSON.stringify(msg)});
    }

    /**
     * Method to publish on sparks fanout new mappings if needed.
     */
    publishSparkIfNeeded(channels, userId=undefined, spark=undefined, socketChannel=undefined) {                    
        if (channels[0] !== 'SPARKS' && spark) {                              
            setInterval( () => {
                if (this.publishedSparkIds.indexOf(spark.id) !== -1 ){
                    return;
                }
                this.publishedSparkIds.push(spark.id);                
                const msg = {userId: userId, currSocket : spark.id, socketChannel : socketChannel}
                this.publishInExchange('SPARKS', ['pub_sub'], msg, false)
                console.log('[BROKER SERVICE] PUBLISHED SPARK IDS ', JSON.stringify(this.publishedSparkIds))
            },2000);        
        }
    }

    /**
     * Handler to fire closed_connection on connection close
     */
    onConnectionClose() {    
        console.log('[BROKER SERVICE] ON CONNECTION CLOSE')                          
        this.isBrokerConnected && this.emit('closed_connection');
        this.isBrokerConnected = false;
    }

    /**
     * Handler to fire dicovered_master when master was discovered
     */
    onMasterDiscovery() {           
        if(this.discoverdMaster) {
            return;
        }       

        this.forceLogout(this.activeChannels); 
        this.emit('dicovered_master');               
        this.discoverdMaster = true;
    }
}
