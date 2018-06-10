const brokerConfigs = require('../settings/brokerConfigs.json');

module.exports = class QueueModel {
    constructor(chanel, queueType) {
        this.parsed = {
            name : brokerConfigs[channel][queueType].queue.name,
            durable : brokerConfigs[channel][queueType].queue.durable,
            autoDelete : brokerConfigs[channel][queueType].queue.autoDelete,
            exclusive : brokerConfigs[channel][queueType].queue.exclusive,
            key : brokerConfigs[channel][queueType].queue.bindingKey,
        }        
    }
}