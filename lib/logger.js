const winston = require('winston');

let logger = new (winston.Logger)({
    level: 'debug',
    transports: [
        new (winston.transports.Console)({
            timestamp: function() { return Date.now(); },
            formatter: function(options) {
                return options.timestamp() + ' ' + options.level.toUpperCase() +' '+ (options.message ? options.message : '') +
                (options.meta && Object.keys(options.meta).length ? '\n\t'+ JSON.stringify(options.meta) : '' );
            }
        })/*,
        new (winston.transports.File)({
            timestamp: function() { return Date.now(); },
            formatter: function(options) {
                return options.timestamp() + ' ' + options.level.toUpperCase() +' '+ (options.message ? options.message : '') +
                (options.meta && Object.keys(options.meta).length ? '\n\t'+ JSON.stringify(options.meta) : '' );
            },
            filename: '/tmp/databus-server.log'
        })*/
    ]
});

module.exports = logger;