const express = require('express')
const amqp = require('amqplib/callback_api');

const ExchangeModel = require('../shared/ExchangeModel');
const MessageModel = require('../shared/MessageModel');
const BrokerService = require('../shared/BrokerService');
const SocketService = require('./SocketService');
const configs = require('../settings/configs.json');

const app = express();

const environment = process.argv[5];
const brokerMasterConn = {
    rx : environment === 'localhost' ? configs.broker.localhost_master.rx : configs.broker.heroku.rx,
    tx : environment === 'localhost' ? configs.broker.localhost_master.tx : configs.broker.heroku.tx,
}
const brokerService = new BrokerService(brokerMasterConn, [configs.broker.localhost_master.rx], [configs.broker.localhost_master.tx]);
 
const VOICE_PORT = process.argv[2];
const CHAT_PORT = process.argv[3];
const TICKETS_PORT = process.argv[4];

const VOICE_CHANNEL = 'VOICE';
const CHAT_CHANNEL = 'CHAT';
const TICKETS_CHANNEL = 'TICKETS'
const SPARKS_CHANNEL = 'SPARKS';

/**
 * Start voice web socket service
 */
const serverVoice = require('http').createServer(app)
serverVoice.service = VOICE_CHANNEL;
const voiceSocket = new SocketService(serverVoice, VOICE_PORT)
.on('consume_queue' , (spark, prevSockId, socketChannel) => brokerService.consumeChannels([VOICE_CHANNEL],['out'], spark, prevSockId, socketChannel) )
.on('publish_queue', (channel, data) => onPublishQueue(channel,data,['in'] ));

/**
 * Start chat web socket service
 */
const serverChat = require('http').createServer(app)
serverChat.service = CHAT_CHANNEL;
const chatSocket = new SocketService(serverChat, CHAT_PORT)
.on('consume_queue' , (spark, prevSockId, socketChannel) => brokerService.consumeChannels(['CHAT'],['out'], spark, prevSockId, socketChannel) )
.on('publish_queue', (channel, data) => onPublishQueue(channel,data,['in'] ));

/**
 * Start tickets web socket service
 */
const serverTickets = require('http').createServer(app)
serverTickets.service = TICKETS_CHANNEL;
const ticketsSocket = new SocketService(serverTickets, TICKETS_PORT)
.on('consume_queue' , (spark, prevSockId, socketChannel) => brokerService.consumeChannels(['TICKETS'],['out'], spark, prevSockId, socketChannel) )
.on('publish_queue', (channel, data) => onPublishQueue(channel,data,['in']) );

const queueChannelMapper = {
    VOICE : { socket : voiceSocket },
    CHAT : { socket : chatSocket },
    TICKETS : { socket : ticketsSocket },       
}

/**
 * Handler to publish on queue after receiving web socket message,
 */
function onPublishQueue(channel, data, queueType, persistMsg = true) {   
    const exchange = new ExchangeModel(channel, queueType).parsed;
    const message = new MessageModel(data, persistMsg).parsed;        
    brokerService.publish(exchange, exchange.key, message);
}

brokerService.consumeChannels([SPARKS_CHANNEL],['pub_sub'], false);

brokerService.on('received_queue_data', (queue, data, channel, reply) => {     
    const parsedData = JSON.parse(data.content.toString()); 
    if (channel === 'SPARKS') {
        reply();
        const mappings = {userId: parsedData.userId, currSocket: parsedData.currSocket};
        return queueChannelMapper[parsedData.socketChannel].socket.sparkMappings = mappings;                    
    }  
    return queueChannelMapper[channel].socket.onSendRequest(parsedData, reply);
})