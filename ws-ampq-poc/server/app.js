const configs = require('../settings/configs.json');
const QueueModel = require('../shared/QueueModel');
const BrokerService = require('../shared/BrokerService');
const QueueDataManager = require('../shared/QueueDataManager');
const ExchangeModel = require('../shared/ExchangeModel');
const MessageModel = require('../shared/MessageModel');

const environment = process.argv[2];
const brokerMasterConn = {
    rx : environment === 'localhost' ? configs.broker.localhost_master.rx : configs.broker.heroku.rx,
    tx : environment === 'localhost' ? configs.broker.localhost_master.tx : configs.broker.heroku.tx,
}

/**
 * Some initial instances like the queue data manager to handle when we receive data from 
 * one queue and the broker service to handle the consumption and data publish,
 * as well as the connections and queue callbacks.
 */
const brokerService = new BrokerService(brokerMasterConn, [configs.broker.localhost_master.rx], [configs.broker.localhost_master.tx]);
const queueDataManager = new QueueDataManager(brokerService, QueueDataManager.SERVICE_LAYERS.server);

/** 
 *  Init the consumption of VOICE, CHAT AND TICKETS queues
 */
brokerService.consumeChannels(['VOICE','CHAT','TICKETS'],['external','in']);

/**
 * Act when the broker service receive data on the queue.
 * For the POC sake, we only forward received queue data to the out exchange
 */
brokerService.on('received_queue_data', (queue, data, channel, finish) => {     
    const exchange = new ExchangeModel(channel, 'out').parsed;   
    queueDataManager.onQueueDataReceivedPipeTo(channel, exchange, 'out', data, finish);
})

/**
 * If there is one closed connection from the broker, we will keep retying to consume
 * queues. The service will loop through the connections we gave him on the constructor
 */
brokerService.on('closed_connection', () => {        
    brokerService.consumeChannels(['VOICE','CHAT','TICKETS'],['external','in']);
})

/**
 * On discovered master event from broker service, we need to consume the channels again.
 * 
 * This will create one new connection to the master
 */
brokerService.on('dicovered_master', () => {       
    brokerService.consumeChannels(['VOICE','CHAT','TICKETS'],['external','in']);
    brokerService.discoverdMaster = false;
})