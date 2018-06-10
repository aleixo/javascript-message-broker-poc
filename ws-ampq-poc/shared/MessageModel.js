module.exports = class MessageModel {
    constructor(data, persistent=true) {
        this.parsed = {
            payload : {
                unicast : data.unicast,
                multicast : data.multicast,
                broadcast : data.broadcast,
                sparkId : data.sparkId || '',
                sparkIds : data.sparkIds || [],
                message: data.message,
                work_time : data.work_time || 0,
            },
            persistent: persistent 
        }
    }
}