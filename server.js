/* jshint esversion:6,node:true */

"use strict";
const http = require("http");
const express = require('express');
const app = express();
const compression = require('compression');
const mysql = require('mysql');
const elasticsearch = require('elasticsearch');
const recommendation = require('./lib/recommendation.js');
const logger = require('./lib/logger.js');

const configs = require('./configs.js');
const cur_env = process.env.ENV || "local";
const config  = configs[cur_env];

const PORT    = parseInt(config.EXPRESS_PORT) || 8080;
const HOST    = config.EXPRESS_HOST || '0.0.0.0';
const MAX_AGE = parseInt(config.HEADERS_MAX_AGE) || 18000; // 4 hours
const TIMEOUT = parseInt(config.REQ_TIMEOUT) || 3000;

const pool    = mysql.createPool(config.MYSQL);
const es_client = new elasticsearch.Client({
    host: `${config.ES_HOST}:${config.ES_PORT}`
});




app.use(compression());
app.use((req, res, next) => {
    res.setHeader('Content-Type', 'application/json');
    next();
});

let type_config = [
    {
        path: '/vendor',
        events: ['leads', 'view'],
        table: 'pio_event_1',
        index: 'vendor',
        default_bias: {
          "item_bias": 2,
          "status_filter": -1,
          "category_bias": 150,
          "city_bias": 10,
          "budget_bias": 10,
          "project_bias": 1000
        },
        bias:{

        },
        fields: [
            {
                configBiasName: "status_filter",
                fieldName: "status"
            },
            {
                configBiasName: "category_bias",
                fieldName: "category_slug"
            },
            {
                configBiasName: "city_bias",
                fieldName: "city_slug"
            },
            {
                configBiasName: "budget_bias",
                fieldName: "budget"
            },
            {
                configBiasName: "project_bias",
                fieldName: "projects"
            }
        ]
    }
];
let bias_config = {
    vendor: {}
};

let default_limit = 10;
let max_limit = 100;
let default_page = 1;

let init = function(){

};

for(let i in type_config){
    app.get(type_config[i].path , (req, res, next) => {
        let query = req.query;
        let limit = default_limit;
        let page = default_page;
        /* use page & limit , so convert it to from and size */
        if('limit' in query){
            try{ limit = parseInt(query.limit); } catch(e) {}
            if(limit<=0) limit = default_limit;
        }
        if('page' in query){
            try{ page = parseInt(query.page); } catch(e) {}
            if(page<=0) page = default_page;
        }
        query.from = (page-1) * limit;
        query.size = limit;
        if(limit>max_limit){
            res.status(500).send({ error: `Max limit : ${max_limit}` });
        }
        else{
            let recommendation_engine = new recommendation(pool, es_client, query, type_config[i]);
            recommendation_engine.predict((err, result) => {
                if(err) {
                    logger.error(err.toString());
                    res.setHeader('Cache-Control', 'private');
                    res.status(500).send({ error: err.toString() });
                }
                else {
                    res.setHeader('Cache-Control',     `max-age=${MAX_AGE}`);
                    res.status(200).send(result);
                }
                recommendation_engine = null;
            });
        }
    });
}

// app.get('/vendor', (req, res) => {
//     let query = req.query;
//     let queryEventNames = ['leads', 'view'];
//     let vendor_recommendation = new recommendation(pool, 'pio_event_1', 'vendor', query, queryEventNames);
//     vendor_recommendation.predict((err, result) => {
//         //console.log(err, result);
//         if(err) {
//             logger.error(err.toString());
//             res.setHeader('Cache-Control', 'private');
//             res.status(500).send({ error: err.toString() });
//         }
//         else {
//             //let vendor_id = "non_exists";
//             //try { vendor_id = data.reviews[0].vendorId; } catch(e) { /**/ }

//             res.setHeader('Cache-Control',     `max-age=${MAX_AGE}`);
//             //res.setHeader('Surrogate-Control', `max-age=${MAX_AGE}`);
//             //res.setHeader('Surrogate-Key',     `vendors_v1 vendors_v1_vendors vendors_v1_vendor_${vendor_id}`);
//             res.status(200).send(result);
//         }
//     });    
// });

app.all("*", (req, res) => {
    res.setHeader('Cache-Control', 'private');
    res.status(404).send({ error: "Path is not exists... Like your girlfriend." });
});

app.listen(PORT, HOST);
logger.info(`Server Running on http://${HOST}:${PORT} @${cur_env}`);
logger.info(`Config : ${JSON.stringify(config)}`);

process.on("uncaughtException", (err) => {
    logger.error(err.stack);
});