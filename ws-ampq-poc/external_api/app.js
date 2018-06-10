const express = require('express');
const bodyParser = require('body-parser');
const configs = require('../settings/configs.json');
const BrokerService = require('../shared/BrokerService');

const QueueModel = require('../shared/QueueModel');
const ExchangeModel = require('../shared/ExchangeModel');
const MessageModel = require('../shared/MessageModel');

const environment = process.argv[2];
const brokerMasterConn = {
    rx : environment === 'localhost' ? configs.broker.localhost_master.rx : configs.broker.heroku.rx,
    tx : environment === 'localhost' ? configs.broker.localhost_master.tx : configs.broker.heroku.tx,
}
const brokerService = new BrokerService(brokerMasterConn, [configs.broker.localhost_master.rx], [configs.broker.localhost_master.tx]);

const app = express(); 
const router = express.Router();

function publish(data, channel) {    
    const exchange = new ExchangeModel(channel,'external').parsed;    
    const message = new MessageModel(data).parsed;    
    brokerService.publish(exchange, exchange.key, message);    
}

router.post('/voice', (req, res) => {    
    publish(req.body, 'VOICE')
    res.send('DONE');        
});

router.post('/chat', (req, res) => {    
    publish(req.body, 'CHAT')
    res.send('DONE');        
});

router.post('/tickets', (req, res) => {    
    publish(req.body, 'TICKETS')
    res.send('DONE');        
});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use('/external', router);

app.listen(4000, () => {
    console.log('[API] Started on port 4000')
})