const async = require('async');
const logger = require('../lib/logger.js');

const configs = require('../configs.js');
const cur_env = process.env.ENV || "local";
const config  = configs[cur_env];

let RecsModels = {
    All : "all",
    CF  : "collabFiltering",
    BF  : "backfill"
};

let RankingType = {
    Popular : "popular",
    Trending : "trending",
    Hot : "hot",
    UserDefined : "userDefined",
    Random : "random"
}

let RankingFieldName = {
    UserRank : "userRank",
    UniqueRank : "uniqueRank",
    PopRank : "popRank",
    TrendRank : "trendRank",
    HotRank : "hotRank",
    UnknownRank : "unknownRank"
}

let PopModel = {
    nameByType : function(backfillType){
        let result = "";
        switch(backfillType){
            case RankingType.Popular :
                result = RankingFieldName.PopRank;
                break;
            case RankingType.Trending :
                result = RankingFieldName.TrendRank;
                break;
            case RankingType.Hot :
                result = RankingFieldName.HotRank;
                break;
            case RankingType.UserDefined :
                result = RankingFieldName.UserRank;
                break;
            case RankingType.Random :
                result = RankingFieldName.UniqueRank;
                break;
            default:
                result = RankingFieldName.UnknownRank;
        }
        return result;
    }
}

// this will build the tuned bias fields
let buildTunedBiasFields = function(itemData, type_config){
    let tunedBiasFields = [], itemBias = 1.0, userBias = 1.0;
    let bias_keys = Object.keys(type_config.bias);
    let bias_data = type_config.bias;
    if(!bias_keys.length){
        bias_keys = Object.keys(type_config.default_bias);
        bias_data = type_config.default_bias;
    }
    let fields = type_config.fields;

    if('item_bias' in bias_data){
        itemBias = bias_data.item_bias;
    }
    if('user_bias' in bias_data){
        userBias = bias_data.user_bias;
    }

    for(let i in fields){
        let field = fields[i];
        if(bias_keys.indexOf(field.configBiasName)>=0 && field.fieldName in itemData){
            tunedBiasFields.push({
                name: field.fieldName,
                values: itemData[field.fieldName],
                bias: bias_data[field.configBiasName]
            });
        }
    }
    
    let data = {
        fieldBias: tunedBiasFields,
        itemBias: itemBias,
        userBias: userBias
    };
    logger.debug('Fields:', JSON.stringify(data, null, 4));
    return data;
}

class BoostableCorrelators {
    constructor(actionName, itemIDs, boost){
        console.log(actionName, itemIDs, boost);
        const self = this;
        self.actionName = actionName || null,
        self.itemIDs = itemIDs || [],
        self.boost = boost || 0
    }

    toFilterCorrelators(){
        const self = this;
        return {
            actionName : self.actionName,
            itemIDs    : self.itemIDs
        }
    }
}

class RankingParams {
    constructor(name, type, eventNames, offsetDate, endDate, duration){
        const self = this;
        self.name = name || null,
        self.type = type || null,
        self.eventNames = eventNames || null,
        self.offsetDate = offsetDate || null,
        self.endDate = endDate || null,
        self.duration = duration || null
    }

    toString(){
        const self = this;
        return JSON.stringify({
            name: self.name,
            type: self.type,
            eventNames: self.eventNames,
            offsetDate: self.offsetDate,
            endDate: self.endDate,
            duration: self.duration
        });
    }
}


class Recommendation {
    constructor(pool, es_client, query, type_config){
        const self = this;
        const tableName = type_config.table;
        const indexName = type_config.index;
        const eventNames = type_config.events;

        self.pool = pool;
        self.esClient = es_client;
        self.eventNames = eventNames;
        self.blacklistEvents = [];
        self.indexName = indexName;
        self.typeName  = 'items';
        self.tableName = tableName;
        self.type_config = type_config;

        self.maxItemsPerUser = 2000;
        self.maxQueryEvents = 100;
        self.userBias = 1.0;
        self.itemBias = 1.0;
        self.fields = [];
        self.recsModel = RecsModels.All;
        self.BackfillFieldName = RankingFieldName.PopRank;
        self.BackfillType = RankingType.Popular;
        self.BackfillDuration = "3650 days"
        self.withRanks = true;

        self.itemData = {}; // this required for tuned bias parameter

        self.rankings = [];
        self.defaultRankings = [new RankingParams(
            self.BackfillFieldName,
            self.BackfillType,
            self.eventNames[0],
            null,
            null,
            self.BackfillDuration
        )]
        self.rankingsParams = JSON.parse(JSON.stringify(self.defaultRankings));
        //if(self.rankings.length) self.rankingsParams = self.rankings || self.defaultRankings;


        self.rankingFieldNames = self.rankingsParams.map((rankingParams) => {
            let rankingType = rankingParams.type;
            let rankingFieldName = rankingParams.name || PopModel.nameByType(rankingType);
            return rankingFieldName;
        });

        self.query = {
            user: ('user' in query)? query.user : null,
            item: ('item' in query)? query.item : null,
            userBias: ('userBias' in query)? query.userBias : self.userBias,
            itemBias: ('itemBias' in query)? query.itemBias : self.itemBias,
            fields: ('fields' in query)? query.fields : [],
            from: ('from' in query)? query.from : 0,
            size: ('size' in query)? query.size : 10,
            blacklistItems: ('blacklistItems' in query)? query.blacklistItems : []
        };
        logger.debug(`req query: ${JSON.stringify(self.query, null, 4)}`);

        self.similarItems = [];
        self.boostable = [];
        self.events = [];
    }

    predict(callback){
        const self = this;
        self.buildQuery((err, query) => {
            if(err) callback(err.toString(), null);
            else{
                logger.debug(`es query: ${JSON.stringify(query.body)}`);
                self.esClient.search(query, (err_es, result) => {
                    if(err_es) callback(err_es.toString(), null);
                    else{
                        
                        let source = result.hits.hits.map((el) => {
                            let result = {
                                id: el._source.id,
                                score: el._score,
                                data: JSON.parse(el._source.info)
                            }
                            if(self.withRanks){
                                let ranks = self.rankingsParams.map((backfillParams) => {
                                    let backfillType = backfillParams.type;
                                    let backfillFieldName = backfillParams.name || PopModel.nameByType(backfillType);
                                    if(backfillFieldName in el._source) return el._source[backfillFieldName];
                                }).filter((_) => { return _ !== undefined});
                                result.ranks = ranks;
                            }
                            return result;
                        });
                        
                        //let source = result.hits.hits;
                        callback(null, source);
                    }
                });
            }
        });
    }

    buildQuery(callback){
        const self = this;
        logger.debug('buildQuery working');

        let size         = self.query.size,
            from         = self.query.from,
            should       = [],
            must         = {},
            mustNot      = {},
            sort         = {};

        async.series([
            function(cb){
                self.getBiasedSimilarItems((err, results) => {
                    if(err) cb(err, null);
                    else{
                        self.similarItems = results;
                        cb();
                    }
                });
            },  
            function(cb){
                self.getBiasedRecentUserActions((err, results) => {
                    if(err) cb(err);
                    else{
                        self.boostable = results.boostable;
                        self.events = results.events;
                        cb();
                    }
                });
            },
            function(cb){
                // populate tuned bias fields;
                let tunedBiasFields = buildTunedBiasFields(self.itemData, self.type_config);
                if('fieldBias' in tunedBiasFields) self.fields = tunedBiasFields.fieldBias;
                if('itemBias' in tunedBiasFields) self.itemBias = tunedBiasFields.itemBias;
                if('userBias' in tunedBiasFields) self.userBias = tunedBiasFields.userBias;

                should = self.buildQueryShould();
                must = self.buildQueryMust();
                mustNot = self.buildQueryMustNot();
                sort = self.buildQuerySort()
                cb();
            }
        ], (err, results) => {
            let json = {
                index : self.indexName,
                type  : self.typeName,
                size  : self.query.size,
                from  : self.query.from,
                body  : {
                    sort  : sort,
                    query : {
                        bool : {
                            should : should,
                            must : must,
                            must_not : mustNot,
                            minimum_should_match : 1
                        }
                    }
                }
            }
            callback(err, json);
        })
    }

    buildQueryShould(){
        const self = this;
        logger.debug('buildQueryShould working');

        let recentUserHistory = (self.userBias >= 0.0)? self.boostable.slice(0, self.maxQueryEvents - 1): [];
        let boostedMetadata = self.getBoostedMetadata();
        
        let allBoostedCorrelators = [].concat(recentUserHistory, self.similarItems, boostedMetadata).filter((_) => {
            return _.actionName !== null;
        });

        let shouldFields = allBoostedCorrelators.map((_) => {
            return {
                terms : {
                    [_.actionName] : _.itemIDs,
                    boost          : _.boost
                }
            }
        });

        let shouldScore = {
            constant_score : {
                filter : {
                    match_all : {}
                },
                boost: 0
            }
        }
        return [].concat(shouldFields, shouldScore);
    }

    buildQueryMust(){
        const self = this;
        logger.debug('buildQueryMust working');

        let recentUserHistoryFilter = [(new BoostableCorrelators).toFilterCorrelators()];
        if(self.query.userBias < 0.0){
            recentUserHistoryFilter = self.boostable.map((_) => {
                return _.toFilterCorrelators();
            });
        }

        let similarItemsFilter = [(new BoostableCorrelators).toFilterCorrelators()];
        if(self.query.itemBias < 0.0){
            similarItemsFilter = self.similarItems.map((_) => {
                return _.toFilterCorrelators();
            });
        }

        let filteringMetadata = self.getFilteringMetadata();
        //let filteringDateRange = self.getFilteringDateRange();

        let allFilteringCorrelators = [].concat(recentUserHistoryFilter, similarItemsFilter, filteringMetadata).filter((_) => {
            return _.actionName !== null;
        });

        let mustFields = allFilteringCorrelators.map((_) => {
            return {
                terms : {
                    [_.actionName] : _.itemIDs,
                    boost          : 0
                }
            }
        });

        //return [].concat(mustFields, filteringDateRange);
        return [].concat(mustFields);
    }

    buildQueryMustNot(){
        const self = this;
        logger.debug('buildQueryMustNot working');

        let mustNotItems = {
            ids : {
                values : self.getExcludedItems(),
                boost : 0
            }
        };

        let exclusionFields = self.query.fields.filter((_) => { return _.bias == 0; } );
        let exclusionProperties = exclusionFields.map((field) => {
            if('name' in field && 'values' in field && 'bias' in field){
                if(field_added.indexOf(field.name)<0){
                    field_added.push(field);
                    return {
                        terms : {
                            [field.name]: field.values,
                            boost : 0
                        }
                    }
                }
            }
        }).filter((_) => { return _ !== undefined});

        return exclusionProperties.concat(mustNotItems);
    }

    buildQuerySort(){
        const self = this;
        logger.debug('buildQuerySort working');

        if(self.recsModel == RecsModels.All || self.recsModel == RecsModels.BF) {
            let sortByScore = [
                {
                    _score : {
                        order : "desc"
                    }
                }
            ];
            let sortByRanks = self.rankingFieldNames.map((fieldName) => {
                return {
                    [fieldName] : {
                        unmapped_type : "double",
                        order : "desc"
                    }
                }
            });            
            return [].concat(sortByScore, sortByRanks);
        }
        else return {};
    }


    getExcludedItems(){
        const self = this;
        logger.debug('getExcludedItems working');
        let blacklistedItems = self.events.filter((event) => {
            if(self.blacklistEvents.length){
                return self.blacklistEvents.indexOf(event.event) >= 0;
            }
            else return false;
        })
        .map((event) => { return event.targetEntityId; })
        .concat(self.query.blacklistItems)
        .filter((v, i, a) => a.indexOf(v) === i);

        return blacklistedItems;
    }

    getBiasedSimilarItems(callback){
        const self = this;
        logger.debug('getBiasedSimilarItems working');

        let similarItems = [];

        if(self.query.item && self.query.item != -1){
            self.esClient.get({
                index: self.indexName,
                type: self.typeName,
                id: self.query.item
            }, (err, results) => {
                if(err) callback(err, null);
                else{
                    self.itemData = results._source;
                    let itemEventBias = self.query.itemBias;
                    let itemEventsBoost = (itemEventBias>0 && itemEventBias!=1)? itemEventBias: null;
                    let items = [];
                    for(let i in self.eventNames){
                        let event = self.eventNames[i];
                        if(event in results._source && results._source[event]!==null){
                            items = results._source[event];
                        }
                        let rItems = (items.length <= self.maxQueryEvents)? items : items.slice(0, self.maxQueryEvents - 1);
                        similarItems.push(new BoostableCorrelators(event, rItems, itemEventsBoost));
                    }
                    callback(null, similarItems);
                }
            })
        }
        else{
            callback(null, [new BoostableCorrelators()]);
        }
    }

    getBiasedRecentUserActionsQuery(){
        const self = this;
        logger.debug('getBiasedRecentUserActionsQuery working');

        let result        = false,
            entityId      = self.query.user,
            query         = [],
            queryParams   = [],
            limit         = self.maxItemsPerUser,
            queryPattern  = `(select p.* from ${self.tableName} p ` +
                            "where p.entityType='user' " +
                            "  and p.entityId=? " +
                            "  and p.event=? " +
                            "  and p.eventTime = ( " +
                            `      select max(q.eventTime) from ${self.tableName} q ` +
                            "      where p.entityType = q.entityType " +
                            "        and p.entityId = q.entityId " +
                            "  ) " +
                            "limit ?) ";

        
        
        for(let i in self.eventNames){
            let event = self.eventNames[i];
            query.push(queryPattern);
            queryParams.push(entityId, event, limit);
        }

        if(query.length && queryParams.length){
            result = {
                query       : query.join(" union all "),
                queryParams : queryParams
            }
        }
        return result;
    }

    getBiasedRecentUserActions(callback){
        const self = this;
        logger.debug('getBiasedRecentUserActions working');

        let actions = {},
            rActions = [],
            recentEvents = [],
            userEventBias = (self.query.userBias)? self.query.userBias : self.userBias,
            userEventsBoost = (userEventBias>0 && userEventBias!=1)? userEventBias : null;

        self.pool.getConnection((err_pool, conn) => {
            if(err_pool) callback(err_pool, null);
            else{
                let query = self.getBiasedRecentUserActionsQuery();
                logger.debug('getBiasedRecentUserActions query:\n' + JSON.stringify(query, null, 4));
                let q = conn.query(query.query, query.queryParams);
                
                q.on('result', (row) => {
                    recentEvents.push(JSON.parse(JSON.stringify(row)));
                    for(let i in self.eventNames) {
                        let event = self.eventNames[i];
                        if(!(event in actions)) actions[event] = [];
                        if(row.event = event &&
                            actions[event].length < self.maxItemsPerUser &&
                            actions[event].indexOf(row.targetEntityId)<0
                        ) {
                            actions[event].push(row.targetEntityId);
                        }
                    }
                });
                q.on('error', (err_stream) => { logger.error(err_stream); });
                q.on('end', () => { 
                    conn.release();
                    for(let i in self.eventNames) {
                        let event = self.eventNames[i];
                        rActions.push(new BoostableCorrelators(event, actions[event], userEventsBoost));
                    }
                    let result = {
                        boostable : rActions,
                        events    : recentEvents
                    };
                    callback(null, result);
                });
            }
        });
    }

    getBoostedMetadata(){
        const self = this;
        logger.debug('getBoostedMetadata working');

        let boostedMetadata = [new BoostableCorrelators];

        let paramsBoostedFields = self.fields.filter((_) => { return _.bias < 0.0; });
        let queryBoostedFields = self.query.fields.filter((_) => { return _.bias < 0.0; });

        let combined = [].concat(queryBoostedFields, paramsBoostedFields);
        let field_added = [];
        if(combined.length){
            boostedMetadata = combined.map((field) => {
                if('name' in field && 'values' in field && 'bias' in field){
                    if(field_added.indexOf(field.name)<0){
                        field_added.push(field);
                        return new BoostableCorrelators(field.name, field.values, field.bias);
                    }
                }
            }).filter((_) => { return _ !== undefined});
        }
        return boostedMetadata;
    }

    getFilteringMetadata(){
        const self = this;
        logger.debug('getFilteringMetadata working');

        let filteringMetadata = [(new BoostableCorrelators).toFilterCorrelators()];
        
        let paramsFilterFields = self.fields.filter((_) => { return _.bias > 0.0; });
        let queryFilterFields = self.query.fields.filter((_) => { return _.bias > 0.0; });
        
        let combined = [].concat(queryFilterFields, paramsFilterFields);
        let field_added = [];

        if(combined.length){
            filteringMetadata = combined.map((field) => {
                if('name' in field && 'values' in field && 'bias' in field){
                    if(field_added.indexOf(field.name)<0){
                        field_added.push(field);
                        return (new BoostableCorrelators(field.name, field.values, field.bias)).toFilterCorrelators();
                    }
                }
            }).filter((_) => { return _ !== undefined});
        }
        return filteringMetadata;
    }

    getFilteringDateRange(){
        const self = this;
        logger.debug('getFilteringDateRange working');
    }
}


module.exports = Recommendation;