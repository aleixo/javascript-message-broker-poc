const Primus = require('primus')
const EventEmitter = require('events');
const PrimusResponder = require('primus-responder');

module.exports = class SocketService extends EventEmitter {
    constructor(server, targetPort) {
        super();
        this.server = server;        
            
        this.ACTIVE_SPARKS_COLLECTION = 'prev_sparks_per_user';
        
        this.SPARKS = {};
            
        console.log('[SOCKET SERVICE] Will start ws connection')
        const options = { transformer: "engine.io" };
        const primus = new Primus(this.server, { options });        
        primus.plugin('responder', PrimusResponder);  
        primus.on("connection", (spark) => {    
            spark.on("request from server", (data) => this.onData(data, spark) )
            spark.on("request", (data) => this.onData(data, spark) )
            spark.on("data", (data) => this.onData(data, spark) )
            spark.on("end", (data) => delete this.SPARKS[spark.id])
        });
        server.listen(targetPort, console.log(`[SOCKET SERVICE] Listeneing in ${targetPort} for ${this.server.service}`));         
        //primus.save(__dirname +'/primus.js');

    }

    /**
     * Update the sparks object with the previous agregator spark id mapped to the user.
     * 
     * Used later to map sparks with ther used.
     */
    set sparkMappings(mappings) {       
        if (typeof this.SPARKS[mappings.currSocket] !== 'object') {
            this.SPARKS[mappings.currSocket] = mappings.userId;                 
        }        
    }    

    /**
     * Handler to emit events when we get data on the socket
     * 
     * Emits consume_queue when user sends the subscribe request or publish_queue on the other
     * cases
     */
    onData(data, spark) {          
        if (data.subscribe)  {        
            spark.userId = data.prevSockId;
            this.SPARKS[spark.id] = spark; 
            this.checkForExistingUserMappings(spark);                 
            console.log(`[SOCKET SERVICE] Will emit consume_queue event for id ${spark.id} and userId ${data.prevSockId}`);  
            return this.emit('consume_queue', spark, data.prevSockId, this.server.service);                     
        }  
                 
        data.sparkId = spark.id;
        data.unicast = true;
        console.log('[SOCKET SERVICE] Will emit publish_queue event');
        return this.emit('publish_queue', this.server.service, data, spark);
    }    

    /**
     * Used to update the current sparks object to reflect all the sparks of a given user.
     * 
     * It´s used because of the fail-over. It lacks to remove the unused sparks.
     */
    checkForExistingUserMappings(nextSpark) {       
        Object.keys(this.SPARKS).forEach( spark => {     
            if (nextSpark.userId === this.SPARKS[spark]) {                
                this.SPARKS[spark] = nextSpark;
            }
        })                
    }

    /**
     * Send via WS one message to a given user
     */
    sendTo(sparkId, msg, finishTransmission) {          
        if (!Object.keys(this.SPARKS).includes(sparkId) || typeof this.SPARKS[sparkId] !== 'object') {
            console.log('[SOCKET SERVICE] No user logged to receive the message')              
            return;
        }        
        console.log('[SOCKET SERVICE] Will send unicast message', this.SPARKS)
        this.SPARKS[sparkId].writeAndWait(msg, (response) => {
            console.log('[SOCKET SERVICE] Message sent')            
        });        
        finishTransmission();
    }
    
    /**
     * Send via WS one message to a given group of used identified by
     * one array of spark id´s
     */
    sendMulticast(sparkIdsArray, msg, finishTransmission) {
        console.log('[SOCKET SERVICE] Will send multicast message')
        sparkIdsArray.forEach( sparkId => {
            this.sendTo(sparkId, msg, finishTransmission);
        })
    }

    /**
     * Send via WS one message to all the active sparks
     */
    sendBroadcast(msg, finishTransmission) {      
        console.log('[SOCKET SERVICE] Will send broadcast message')  
        Object.keys(this.SPARKS).forEach( spark => {
            this.sendTo(spark.id, msg, finishTransmission);
        });             
    }    

    /**
     * Called with data that has to be according one protocol to send
     * ws data to client
     */
    onSendRequest(data, reply) {
        console.log('DATA to send', data)
        if (data.unicast) {        
            this.sendTo(data.sparkId, data.message, reply);
        }  
    
        if (data.multicast) {
            this.sendMulticast(data.sparkIds, data.message, reply);
        }
        
        if (data.broadcast) {
            this.sendBroadcast(data.message, reply);
        }       
    }
}
