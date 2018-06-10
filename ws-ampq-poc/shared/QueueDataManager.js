module.exports = class QueueDataManager {

    /**
     * Available service layers
     */
    static get SERVICE_LAYERS() {
        return {
            server : 'server',
            agregator : 'agregator',
            api : 'api'
        };
    }

    constructor(brokerService, layer) {
        this.brokerService = brokerService;
        this.layer = layer;
    }

    /**
     * Used to publish on some situation.
     * 
     * TODO: Check if there is still usage for this. I don´t think so. Now, it is
     * only beeing used by the server. Either it is used on all of the other sides, or remove it.:
     */
    onQueueDataReceivedPipeTo(channel, exchange, queueType, data, finish) {

        if (this.layer === QueueDataManager.SERVICE_LAYERS.agregator) {
            this.onServerData(channel, exchange, queueType, data, finish);
        }

        let jsonedData = JSON.parse(data.content)
        if (typeof jsonedData === 'string') {
            jsonedData = JSON.parse(jsonedData);
        }
        
        setTimeout( () => {            
            finish();
            
            if (jsonedData.noReply) {
                console.log('[SERVER] Will break the event since client sent noReply');
                return;
            }                
            this.brokerService.publish(exchange, exchange.key, {persistent : true, payload : data.content.toString()});                
        }, this.timeoutMilisecounds(jsonedData.work_time))    
    }  
    
    /**
     * Calculate the milisecounds based on the workTime flag and the layer of the instance     
     */
    timeoutMilisecounds(workTime) {        
        if (this.layer !== QueueDataManager.SERVICE_LAYERS.server) {
            return 0;
        }        
        return workTime * 1000;
    }   
}