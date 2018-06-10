const brokerConfigs = require('../settings/brokerConfigs.json');

module.exports = class ExchangeModel {
    constructor(channel, queueType) {
        this.parsed = {
            name : brokerConfigs[channel][queueType].exchange.name,
            type : brokerConfigs[channel][queueType].exchange.type,
            durable: brokerConfigs[channel][queueType].exchange.durable,
            autoDelete: brokerConfigs[channel][queueType].exchange.autoDelete,
            key : brokerConfigs[channel][queueType].queue.bindingKey,
        }
        Object.freeze(this);
    }
}