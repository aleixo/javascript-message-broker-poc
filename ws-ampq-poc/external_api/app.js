const express = require('express');
const bodyParser = require('body-parser');
const configs = require('../settings/configs.json');
const BrokerService = require('../shared/BrokerService');

const ExchangeModel = require('../shared/ExchangeModel');
const MessageModel = require('../shared/MessageModel');

const environment = process.argv[3];
const brokerMasterConn = {
    rx : environment === 'localhost' ? configs.broker.localhost_master.rx : configs.broker.heroku.rx,
    tx : environment === 'localhost' ? configs.broker.localhost_master.tx : configs.broker.heroku.tx,
}
const brokerService = new BrokerService(brokerMasterConn, [configs.broker.localhost_master.rx], [configs.broker.localhost_master.tx]);

const app = express(); 
const router = express.Router();

/**
 * Publish on the external queue. Used when receiveing api data.
 */
function publish(data, channel) {    
    const exchange = new ExchangeModel(channel,'external').parsed;    
    const message = new MessageModel(data).parsed;    
    brokerService.publish(exchange, exchange.key, message);    
}

/**
 * Route to post voice events
 */
router.post('/voice', (req, res) => {    
    publish(req.body, 'VOICE')
    res.send('DONE');        
});

/**
 * Route to post chat events
 */
router.post('/chat', (req, res) => {    
    publish(req.body, 'CHAT')
    res.send('DONE');        
});

/**
 * Route to post tickets events
 */
router.post('/tickets', (req, res) => {    
    publish(req.body, 'TICKETS')
    res.send('DONE');        
});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use('/external', router);

app.listen(process.argv[2] || 4000, () => {
    console.log(`[API] Started on port ${process.argv[2] || 4000}`)
})