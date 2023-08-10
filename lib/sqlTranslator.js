"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SqlTranslator = void 0;
var tslib_1 = require("tslib");
var assert_1 = tslib_1.__importDefault(require("assert"));
var lodash_1 = require("lodash");
var types_1 = require("oak-domain/lib/types");
var relation_1 = require("oak-domain/lib/store/relation");
var filter_1 = require("oak-domain/lib/store/filter");
;
;
var SqlTranslator = /** @class */ (function () {
    function SqlTranslator(schema) {
        this.schema = this.makeFullSchema(schema);
    }
    SqlTranslator.prototype.makeFullSchema = function (schema2) {
        var schema = (0, lodash_1.cloneDeep)(schema2);
        for (var entity in schema) {
            var _a = schema[entity], attributes = _a.attributes, indexes = _a.indexes;
            // 增加默认的属性
            (0, lodash_1.assign)(attributes, {
                id: {
                    type: 'char',
                    params: {
                        length: 36,
                    },
                },
                $$seq$$: {
                    type: 'sequence',
                    sequenceStart: 10000,
                },
                $$createAt$$: {
                    type: 'datetime',
                    notNull: true,
                },
                $$updateAt$$: {
                    type: 'datetime',
                    notNull: true,
                },
                $$deleteAt$$: {
                    type: 'datetime',
                },
                $$triggerData$$: {
                    type: 'object',
                },
                $$triggerTimestamp$$: {
                    type: 'datetime',
                },
            });
            // 增加默认的索引
            var intrinsticIndexes = [
                {
                    name: "".concat(entity, "_create_at_auto_create"),
                    attributes: [{
                            name: '$$createAt$$',
                        }, {
                            name: '$$deleteAt$$',
                        }]
                }, {
                    name: "".concat(entity, "_update_at_auto_create"),
                    attributes: [{
                            name: '$$updateAt$$',
                        }, {
                            name: '$$deleteAt$$',
                        }],
                }, {
                    name: "".concat(entity, "_trigger_ts_auto_create"),
                    attributes: [{
                            name: '$$triggerTimestamp$$',
                        }, {
                            name: '$$deleteAt$$',
                        }],
                }
            ];
            var _loop_1 = function (attr) {
                if (attributes[attr].type === 'ref') {
                    if (!(indexes === null || indexes === void 0 ? void 0 : indexes.find(function (ele) { return ele.attributes[0].name === attr; }))) {
                        intrinsticIndexes.push({
                            name: "".concat(entity, "_fk_").concat(attr, "_auto_create"),
                            attributes: [{
                                    name: attr,
                                }, {
                                    name: '$$deleteAt$$',
                                }]
                        });
                    }
                }
                if (attr === 'entity' && attributes[attr].type === 'varchar') {
                    var entityIdDef = attributes.entityId;
                    if ((entityIdDef === null || entityIdDef === void 0 ? void 0 : entityIdDef.type) === 'varchar') {
                        if (!(indexes === null || indexes === void 0 ? void 0 : indexes.find(function (ele) { var _a; return ele.attributes[0].name === 'entity' && ((_a = ele.attributes[1]) === null || _a === void 0 ? void 0 : _a.name) === 'entityId'; }))) {
                            intrinsticIndexes.push({
                                name: "".concat(entity, "_fk_entity_entityId_auto_create"),
                                attributes: [{
                                        name: 'entity',
                                    }, {
                                        name: 'entityId',
                                    }, {
                                        name: '$$deleteAt$$',
                                    }]
                            });
                        }
                    }
                }
                if (attr.endsWith('State') && attributes[attr].type === 'varchar') {
                    if (!(indexes === null || indexes === void 0 ? void 0 : indexes.find(function (ele) { return ele.attributes[0].name === attr; }))) {
                        intrinsticIndexes.push({
                            name: "".concat(entity, "_").concat(attr, "_auto_create"),
                            attributes: [{
                                    name: attr,
                                }, {
                                    name: '$$deleteAt$$',
                                }]
                        });
                    }
                }
                if (attr === 'expired' && attributes[attr].type === 'boolean') {
                    var expiresAtDef = attributes.expiresAt;
                    if ((expiresAtDef === null || expiresAtDef === void 0 ? void 0 : expiresAtDef.type) === 'datetime') {
                        if (!(indexes === null || indexes === void 0 ? void 0 : indexes.find(function (ele) { var _a; return ele.attributes[0].name === 'expired' && ((_a = ele.attributes[1]) === null || _a === void 0 ? void 0 : _a.name) === 'expiresAt'; }))) {
                            intrinsticIndexes.push({
                                name: "".concat(entity, "_expires_expiredAt_auto_create"),
                                attributes: [{
                                        name: 'expired',
                                    }, {
                                        name: 'expiresAt',
                                    }, {
                                        name: '$$deleteAt$$',
                                    }]
                            });
                        }
                    }
                }
            };
            // 增加外键等相关属性上的索引
            for (var attr in attributes) {
                _loop_1(attr);
            }
            if (indexes) {
                indexes.push.apply(indexes, intrinsticIndexes);
            }
            else {
                (0, lodash_1.assign)(schema[entity], {
                    indexes: intrinsticIndexes,
                });
            }
        }
        return schema;
    };
    SqlTranslator.prototype.getStorageName = function (entity) {
        var storageName = this.schema[entity].storageName;
        return (storageName || entity);
    };
    SqlTranslator.prototype.translateInsert = function (entity, data) {
        var _this = this;
        var schema = this.schema;
        var _a = schema[entity], attributes = _a.attributes, _b = _a.storageName, storageName = _b === void 0 ? entity : _b;
        var sql = "insert into `".concat(storageName, "`(");
        /**
         * 这里的attrs要用所有行的union集合
         */
        var dataFull = data.reduce(function (prev, cur) { return Object.assign({}, cur, prev); }, {});
        var attrs = Object.keys(dataFull).filter(function (ele) { return attributes.hasOwnProperty(ele); });
        attrs.forEach(function (attr, idx) {
            sql += " `".concat(attr, "`");
            if (idx < attrs.length - 1) {
                sql += ',';
            }
        });
        sql += ') values ';
        data.forEach(function (d, dataIndex) {
            sql += '(';
            attrs.forEach(function (attr, attrIdx) {
                var attrDef = attributes[attr];
                var dataType = attrDef.type;
                var value = _this.translateAttrValue(dataType, d[attr]);
                sql += value;
                if (attrIdx < attrs.length - 1) {
                    sql += ',';
                }
            });
            if (dataIndex < data.length - 1) {
                sql += '),';
            }
            else {
                sql += ')';
            }
        });
        return sql;
    };
    /**
     * analyze the join relations in projection/query/sort
     * 所有的层次关系都当成left join处理，如果有内表为空的情况，请手动处理
     * {
     *      b: {
     *          name: {
     *              $exists: false,
     *          }
*           }
     * }
     * 这样的query会把内表为空的行也返回
     * @param param0
     */
    SqlTranslator.prototype.analyzeJoin = function (entity, _a, initialNumber) {
        var _this = this;
        var projection = _a.projection, filter = _a.filter, sorter = _a.sorter, aggregation = _a.aggregation;
        var schema = this.schema;
        var number = initialNumber || 1;
        var projectionRefAlias = {};
        var filterRefAlias = {};
        var alias = "".concat(entity, "_").concat(number++);
        var from = " `".concat(this.getStorageName(entity), "` `").concat(alias, "` ");
        var aliasDict = {
            './': alias,
        };
        var analyzeFilterNode = function (_a) {
            var _b;
            var node = _a.node, path = _a.path, entityName = _a.entityName, alias = _a.alias;
            Object.keys(node).forEach(function (op) {
                var _a, _b;
                if (['$and', '$or'].includes(op)) {
                    node[op].forEach(function (subNode) { return analyzeFilterNode({
                        node: subNode,
                        path: path,
                        entityName: entityName,
                        alias: alias,
                    }); });
                }
                else if (['$not'].includes(op)) {
                    analyzeFilterNode({
                        node: node[op],
                        path: path,
                        entityName: entityName,
                        alias: alias,
                    });
                }
                else if (['$text'].includes(op)) {
                }
                else {
                    var rel = (0, relation_1.judgeRelation)(_this.schema, entityName, op);
                    if (typeof rel === 'string') {
                        var alias2 = void 0;
                        var pathAttr = "".concat(path).concat(op, "/");
                        if (!aliasDict.hasOwnProperty(pathAttr)) {
                            alias2 = "".concat(rel, "_").concat(number++);
                            (0, lodash_1.assign)(aliasDict, (_a = {},
                                _a[pathAttr] = alias2,
                                _a));
                            from += " left join `".concat(_this.getStorageName(rel), "` `").concat(alias2, "` on `").concat(alias, "`.`").concat(op, "Id` = `").concat(alias2, "`.`id`");
                        }
                        else {
                            alias2 = aliasDict[pathAttr];
                        }
                        analyzeFilterNode({
                            node: node[op],
                            path: pathAttr,
                            entityName: rel,
                            alias: alias2,
                        });
                    }
                    else if (rel === 2) {
                        var alias2 = void 0;
                        var pathAttr = "".concat(path).concat(op, "/");
                        if (!aliasDict.hasOwnProperty(pathAttr)) {
                            alias2 = "".concat(op, "_").concat(number++);
                            (0, lodash_1.assign)(aliasDict, (_b = {},
                                _b[pathAttr] = alias2,
                                _b));
                            from += " left join `".concat(_this.getStorageName(op), "` `").concat(alias2, "` on `").concat(alias, "`.`entityId` = `").concat(alias2, "`.`id` and `").concat(alias, "`.`entity` = '").concat(op, "'");
                        }
                        else {
                            alias2 = aliasDict[pathAttr];
                        }
                        analyzeFilterNode({
                            node: node[op],
                            path: pathAttr,
                            entityName: op,
                            alias: alias2,
                        });
                    }
                    else {
                        // 不支持一对多
                        // assert(rel === 0 || rel === 1);
                    }
                }
            });
            if (node['#id']) {
                (0, assert_1.default)(!filterRefAlias[node['#id']]);
                (0, lodash_1.assign)(filterRefAlias, (_b = {},
                    _b[node['#id']] = [alias, entityName],
                    _b));
            }
        };
        if (filter) {
            analyzeFilterNode({
                node: filter,
                path: './',
                entityName: entity,
                alias: alias,
            });
        }
        var analyzeSortNode = function (_a) {
            var _b, _c;
            var node = _a.node, path = _a.path, entityName = _a.entityName, alias = _a.alias;
            var attr = (0, lodash_1.keys)(node)[0];
            var rel = (0, relation_1.judgeRelation)(_this.schema, entityName, attr);
            if (typeof rel === 'string') {
                var pathAttr = "".concat(path).concat(attr, "/");
                var alias2 = void 0;
                if (!aliasDict.hasOwnProperty(pathAttr)) {
                    alias2 = "".concat(rel, "_").concat(number++);
                    (0, lodash_1.assign)(aliasDict, (_b = {},
                        _b[pathAttr] = alias2,
                        _b));
                    from += " left join `".concat(_this.getStorageName(rel), "` `").concat(alias2, "` on `").concat(alias, "`.`").concat(attr, "Id` = `").concat(alias2, "`.`id`");
                }
                else {
                    alias2 = aliasDict[pathAttr];
                }
                analyzeSortNode({
                    node: node[attr],
                    path: pathAttr,
                    entityName: rel,
                    alias: alias2,
                });
            }
            else if (rel === 2) {
                var pathAttr = "".concat(path).concat(attr, "/");
                var alias2 = void 0;
                if (!aliasDict.hasOwnProperty(pathAttr)) {
                    alias2 = "".concat(attr, "_").concat(number++);
                    (0, lodash_1.assign)(aliasDict, (_c = {},
                        _c[pathAttr] = alias2,
                        _c));
                    from += " left join `".concat(_this.getStorageName(attr), "` `").concat(alias2, "` on `").concat(alias, "`.`entityId` = `").concat(alias2, "`.`id` and `").concat(alias, "`.`entity` = '").concat(attr, "'");
                }
                else {
                    alias2 = aliasDict[pathAttr];
                }
                analyzeSortNode({
                    node: node[attr],
                    path: pathAttr,
                    entityName: attr,
                    alias: alias2,
                });
            }
            else {
                (0, assert_1.default)(rel === 0 || rel === 1);
            }
        };
        if (sorter) {
            sorter.forEach(function (sortNode) {
                analyzeSortNode({
                    node: sortNode.$attr,
                    path: './',
                    entityName: entity,
                    alias: alias,
                });
            });
        }
        var analyzeProjectionNode = function (_a) {
            var _b;
            var node = _a.node, path = _a.path, entityName = _a.entityName, alias = _a.alias;
            var attributes = schema[entityName].attributes;
            Object.keys(node).forEach(function (attr) {
                var _a, _b;
                var rel = (0, relation_1.judgeRelation)(_this.schema, entityName, attr);
                if (typeof rel === 'string') {
                    var pathAttr = "".concat(path).concat(attr, "/");
                    var alias2 = void 0;
                    if (!aliasDict.hasOwnProperty(pathAttr)) {
                        alias2 = "".concat(rel, "_").concat(number++);
                        (0, lodash_1.assign)(aliasDict, (_a = {},
                            _a[pathAttr] = alias2,
                            _a));
                        from += " left join `".concat(_this.getStorageName(rel), "` `").concat(alias2, "` on `").concat(alias, "`.`").concat(attr, "Id` = `").concat(alias2, "`.`id`");
                    }
                    else {
                        alias2 = aliasDict[pathAttr];
                    }
                    analyzeProjectionNode({
                        node: node[attr],
                        path: pathAttr,
                        entityName: rel,
                        alias: alias2,
                    });
                }
                else if (rel === 2) {
                    var pathAttr = "".concat(path).concat(attr, "/");
                    var alias2 = void 0;
                    if (!aliasDict.hasOwnProperty(pathAttr)) {
                        alias2 = "".concat(attr, "_").concat(number++);
                        (0, lodash_1.assign)(aliasDict, (_b = {},
                            _b[pathAttr] = alias2,
                            _b));
                        from += " left join `".concat(_this.getStorageName(attr), "` `").concat(alias2, "` on `").concat(alias, "`.`entityId` = `").concat(alias2, "`.`id` and `").concat(alias, "`.`entity` = '").concat(attr, "'");
                    }
                    else {
                        alias2 = aliasDict[pathAttr];
                    }
                    analyzeProjectionNode({
                        node: node[attr],
                        path: pathAttr,
                        entityName: attr,
                        alias: alias2,
                    });
                }
            });
            if (node['#id']) {
                (0, assert_1.default)(!projectionRefAlias[node['#id']], "projection\u4E0A\u6709\u91CD\u590D\u7684#id\u5B9A\u4E49\u300C".concat(node['#id'], "\u300D"));
                (0, lodash_1.assign)(projectionRefAlias, (_b = {},
                    _b[node['#id']] = [alias, entityName],
                    _b));
            }
        };
        if (projection) {
            analyzeProjectionNode({ node: projection, path: './', entityName: entity, alias: alias });
        }
        else if (aggregation) {
            for (var k in aggregation) {
                analyzeProjectionNode({
                    node: aggregation[k],
                    path: './',
                    entityName: entity,
                    alias: alias,
                });
            }
        }
        return {
            aliasDict: aliasDict,
            from: from,
            projectionRefAlias: projectionRefAlias,
            filterRefAlias: filterRefAlias,
            currentNumber: number,
        };
    };
    SqlTranslator.prototype.translateComparison = function (attr, value, type) {
        var SQL_OP = {
            $gt: '>',
            $lt: '<',
            $gte: '>=',
            $lte: '<=',
            $eq: '=',
            $ne: '<>',
        };
        if (Object.keys(SQL_OP).includes(attr)) {
            if (type) {
                return " ".concat(SQL_OP[attr], " ").concat(this.translateAttrValue(type, value));
            }
            else {
                return " ".concat(SQL_OP[attr], " ").concat(value);
            }
        }
        switch (attr) {
            case '$startsWith': {
                return " like '".concat(value, "%'");
            }
            case '$endsWith': {
                return " like '%".concat(value, "'");
            }
            case '$includes': {
                return " like '%".concat(value, "%'");
            }
            default: {
                throw new Error("unrecoganized comparison operator ".concat(attr));
            }
        }
    };
    SqlTranslator.prototype.translateEvaluation = function (attr, value, entity, alias, type, initialNumber, refAlias) {
        switch (attr) {
            case '$in':
            case '$nin': {
                var IN_OP = {
                    $in: 'in',
                    $nin: 'not in',
                };
                if (value instanceof Array) {
                    return {
                        stmt: this.translatePredicate(attr, value, type),
                        currentNumber: initialNumber,
                    };
                }
                else {
                    (0, assert_1.default)(false, '子查询已经改写为一对多的形式');
                    // sub query
                    /* const { stmt: subQueryStmt, currentNumber } = this.translateSelectInner(value.entity, value, initialNumber, refAlias, undefined);
                    return {
                        stmt: ` ${IN_OP[attr]}(${subQueryStmt})`,
                        currentNumber,
                    }; */
                }
            }
            default: {
                throw new Error("".concat(attr, " is not evaluation predicate"));
            }
        }
    };
    SqlTranslator.prototype.translatePredicate = function (predicate, value, type) {
        if (['$gt', '$gte', '$lt', '$lte', '$eq', '$ne', '$startsWith', '$endsWith', '$includes'].includes(predicate)) {
            return this.translateComparison(predicate, value, type);
        }
        else if (['$in', '$nin'].includes(predicate)) {
            (0, assert_1.default)(value instanceof Array);
            var IN_OP = {
                $in: 'in',
                $nin: 'not in',
            };
            var values = value.map(function (v) {
                if (type && ['varchar', 'char', 'text', 'nvarchar', 'ref', 'enum'].includes(type) || typeof v === 'string') {
                    return "'".concat(v, "'");
                }
                else {
                    return "".concat(v);
                }
            });
            if (values.length > 0) {
                return " ".concat(IN_OP[predicate], "(").concat(values.join(','), ")");
            }
            if (predicate === '$in') {
                return ' in (null)';
            }
            return ' is not null';
        }
        else if (predicate === '$between') {
            var values = value.map(function (v) {
                if (type && ['varchar', 'char', 'text', 'nvarchar', 'ref', 'enum'].includes(type) || typeof v === 'string') {
                    return "'".concat(v, "'");
                }
                else {
                    return "".concat(v);
                }
            });
            // between是所有数据库都支持的语法吗？
            return " between ".concat(values[0], " and ").concat(values[1]);
        }
        else {
            (0, assert_1.default)(predicate === '$exists');
            if (value) {
                return ' is not null';
            }
            return ' is null';
        }
    };
    SqlTranslator.prototype.translateFilter = function (entity, filter, aliasDict, filterRefAlias, initialNumber, option) {
        var _this = this;
        var schema = this.schema;
        var currentNumber = initialNumber;
        var translateInner = function (entity2, path, filter2, type) {
            var alias = aliasDict[path];
            var attributes = schema[entity2].attributes;
            var whereText = type ? '' : _this.getDefaultSelectFilter(alias, option);
            if (filter2) {
                var attrs = Object.keys(filter2).filter(function (ele) { return !ele.startsWith('#'); });
                attrs.forEach(function (attr) {
                    var _a;
                    if (whereText) {
                        whereText += ' and ';
                    }
                    if (['$and', '$or', '$xor', '$not'].includes(attr)) {
                        whereText += '(';
                        switch (attr) {
                            case '$and':
                            case '$or':
                            case '$xor': {
                                var logicQueries_1 = filter2[attr];
                                logicQueries_1.forEach(function (logicQuery, index) {
                                    var sql = translateInner(entity2, path, logicQuery, 'ref'); // 只要传个值就行了，应该无所谓
                                    if (sql) {
                                        whereText += " (".concat(sql, ")");
                                        if (index < logicQueries_1.length - 1) {
                                            whereText += " ".concat(attr.slice(1));
                                        }
                                    }
                                });
                                break;
                            }
                            default: {
                                (0, assert_1.default)(attr === '$not');
                                var logicQuery = filter2[attr];
                                var sql = translateInner(entity2, path, logicQuery, 'ref'); // 只要传个值就行了，应该无所谓
                                if (sql) {
                                    whereText += " not (".concat(sql, ")");
                                    break;
                                }
                            }
                        }
                        whereText += ')';
                    }
                    else if (attr === '$text') {
                        whereText += "(".concat(_this.translateFullTextSearch(filter2[attr], entity2, alias), ")");
                    }
                    else if (attr.toLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                        // expression
                        whereText += " (".concat(_this.translateExpression(entity2, alias, filter2[attr], filterRefAlias), ")");
                    }
                    else {
                        var rel = (0, relation_1.judgeRelation)(_this.schema, entity2, attr);
                        if (rel === 2) {
                            whereText += " (".concat(translateInner(attr, "".concat(path).concat(attr, "/"), filter2[attr]), ")");
                        }
                        else if (typeof rel === 'string') {
                            whereText += " (".concat(translateInner(rel, "".concat(path).concat(attr, "/"), filter2[attr]), ")");
                        }
                        else if (rel instanceof Array) {
                            var subEntity = rel[0], foreignKey = rel[1];
                            var predicate = (filter2[attr]['#sqp'] || 'in');
                            /**
                             *
                            * in代表外键连接后至少有一行数据
                            * not in代表外键连接后一行也不能有
                            * all代表反外键连接条件的一行也不能有（符合的是否至少要有一行？直觉上没这个限制）
                            * not all 代表反外键连接条件的至少有一行
                            *
                            * 目前将这种子查询翻译成了exists查询，当外表很大而子查询结果集很小时可能有性能问题，取决于MySQL执行器的能力
                            * by Xc 20230726
                             */
                            var refAlia = Object.keys(filterRefAlias).find(function (ele) { return filterRefAlias[ele][0] === alias; });
                            var refAlia2 = refAlia || alias; // alias一定是唯一的，可以用来作为node id
                            if (!refAlia) {
                                (0, assert_1.default)(!filterRefAlias[refAlia2]);
                                Object.assign(filterRefAlias, (_a = {},
                                    _a[refAlia2] = [alias, entity2],
                                    _a));
                            }
                            var fk = foreignKey || 'entityId';
                            var joinFilter = ['not in', 'in'].includes(predicate) ? {
                                $expr12: {
                                    $eq: [
                                        {
                                            '#attr': fk,
                                        },
                                        {
                                            '#refId': refAlia2,
                                            '#refAttr': 'id',
                                        }
                                    ],
                                }
                            } : {
                                $expr12: {
                                    $ne: [
                                        {
                                            '#attr': fk,
                                        },
                                        {
                                            '#refId': refAlia2,
                                            '#refAttr': 'id',
                                        }
                                    ],
                                }
                            };
                            if (!foreignKey) {
                                Object.assign(joinFilter, {
                                    entity: entity2,
                                });
                            }
                            var _b = _this.translateSelectInner(subEntity, {
                                data: {
                                    id: 1,
                                },
                                filter: (0, filter_1.combineFilters)(subEntity, _this.schema, [joinFilter, filter2[attr]]),
                                indexFrom: 0,
                                count: 1,
                            }, currentNumber, filterRefAlias, option), stmt = _b.stmt, ct2 = _b.currentNumber;
                            currentNumber = ct2;
                            var PREDICATE_DICT = {
                                'in': 'exists',
                                'not in': 'not exists',
                                'all': 'not exists',
                                'not all': 'exists',
                            };
                            whereText += " ".concat(PREDICATE_DICT[predicate], " (").concat(stmt, ")");
                        }
                        else {
                            (0, assert_1.default)(attributes.hasOwnProperty(attr), "\u975E\u6CD5\u7684\u5C5E\u6027".concat(attr));
                            var type2 = attributes[attr].type;
                            // assert (type2 !== 'ref');
                            if (typeof filter2[attr] === 'object') {
                                if (['object', 'array'].includes(type2)) {
                                    // 对object数据的深层次查询，这里调用数据库所支持的属性对象级查询，如mysql中的json查询
                                    whereText += "(".concat(_this.translateObjectPredicate(filter2[attr], alias, attr), ")");
                                }
                                else {
                                    (0, assert_1.default)(Object.keys(filter2[attr]).length === 1);
                                    var predicate = Object.keys(filter2[attr])[0];
                                    (0, assert_1.default)(predicate.startsWith('$'));
                                    // 对属性上的谓词处理
                                    whereText += " (`".concat(alias, "`.`").concat(attr, "` ").concat(_this.translatePredicate(predicate, filter2[attr][predicate], type2), ")");
                                }
                            }
                            else {
                                whereText += " (`".concat(alias, "`.`").concat(attr, "` = ").concat(_this.translateAttrValue(type2, filter2[attr]), ")");
                            }
                        }
                    }
                });
            }
            if (!whereText) {
                whereText = 'true'; // 如果为空就赋一个永真条件，以便处理and
            }
            return whereText;
        };
        var where = translateInner(entity, './', filter);
        return {
            stmt: where,
            currentNumber: currentNumber,
        };
    };
    SqlTranslator.prototype.translateSorter = function (entity, sorter, aliasDict) {
        var _this = this;
        var translateInner = function (entity2, sortAttr, path) {
            (0, assert_1.default)(Object.keys(sortAttr).length === 1);
            var attr = Object.keys(sortAttr)[0];
            var alias = aliasDict[path];
            if (attr.toLocaleLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                return _this.translateExpression(entity2, alias, sortAttr[attr], {});
            }
            else if (sortAttr[attr] === 1) {
                return "`".concat(alias, "`.`").concat(attr, "`");
            }
            else {
                var rel = (0, relation_1.judgeRelation)(_this.schema, entity2, attr);
                if (typeof rel === 'string') {
                    return translateInner(rel, sortAttr[attr], "".concat(path).concat(attr, "/"));
                }
                else {
                    (0, assert_1.default)(rel === 2);
                    return translateInner(attr, sortAttr[attr], "".concat(path).concat(attr, "/"));
                }
            }
        };
        var sortText = '';
        sorter.forEach(function (sortNode, index) {
            var $attr = sortNode.$attr, $direction = sortNode.$direction;
            sortText += translateInner(entity, $attr, './');
            if ($direction) {
                sortText += " ".concat($direction);
            }
            if (index < sorter.length - 1) {
                sortText += ',';
            }
        });
        return sortText;
    };
    SqlTranslator.prototype.translateProjection = function (entity, projection, aliasDict, projectionRefAlias, commonPrefix, disableAs) {
        var _this = this;
        var schema = this.schema;
        var as = '';
        var translateInner = function (entity2, projection2, path) {
            var alias = aliasDict[path];
            var attributes = schema[entity2].attributes;
            var projText = '';
            var prefix = path.slice(2).replace(/\//g, '.');
            var attrs = Object.keys(projection2).filter(function (attr) {
                if (attr.toLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                    return true;
                }
                var rel = (0, relation_1.judgeRelation)(_this.schema, entity2, attr);
                return [1, 2].includes(rel) || typeof rel === 'string';
            });
            attrs.forEach(function (attr, idx) {
                var prefix2 = commonPrefix ? "".concat(commonPrefix, ".").concat(prefix) : prefix;
                if (attr.toLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                    var exprText = _this.translateExpression(entity2, alias, projection2[attr], projectionRefAlias);
                    if (disableAs) {
                        projText += " ".concat(exprText);
                    }
                    else {
                        projText += " ".concat(exprText, " as `").concat(prefix2).concat(attr, "`");
                        if (!as) {
                            as = "`".concat(prefix2).concat(attr, "`");
                        }
                        else {
                            as += ", `".concat(prefix2).concat(attr, "`");
                        }
                    }
                }
                else {
                    var rel = (0, relation_1.judgeRelation)(_this.schema, entity2, attr);
                    if (typeof rel === 'string') {
                        projText += translateInner(rel, projection2[attr], "".concat(path).concat(attr, "/"));
                    }
                    else if (rel === 2) {
                        projText += translateInner(attr, projection2[attr], "".concat(path).concat(attr, "/"));
                    }
                    else if (rel === 1) {
                        var type = attributes[attr].type;
                        if (projection2[attr] === 1) {
                            if (disableAs) {
                                projText += " ".concat(_this.translateAttrProjection(type, alias, attr));
                            }
                            else {
                                projText += " ".concat(_this.translateAttrProjection(type, alias, attr), " as `").concat(prefix2).concat(attr, "`");
                                if (!as) {
                                    as = "`".concat(prefix2).concat(attr, "`");
                                }
                                else {
                                    as += ", `".concat(prefix2).concat(attr, "`");
                                }
                            }
                        }
                        else if (typeof projection2[attr] === 'object') {
                            // 对JSON对象的取值
                            (0, assert_1.default)(!disableAs);
                            (0, assert_1.default)(['object', 'array'].includes(type));
                            projText += " ".concat(_this.translateObjectProjection(projection2[attr], alias, attr, prefix2));
                        }
                        else {
                            (0, assert_1.default)(typeof projection2 === 'string');
                            if (disableAs) {
                                projText += " ".concat(_this.translateAttrProjection(type, alias, attr));
                            }
                            else {
                                projText += " ".concat(_this.translateAttrProjection(type, alias, attr), " as `").concat(prefix2).concat(projection2[attr], "`");
                                if (!as) {
                                    as = "`".concat(prefix2).concat(projection2[attr], "`");
                                }
                                else {
                                    as += "`".concat(prefix2).concat(projection2[attr], "`");
                                }
                            }
                        }
                    }
                }
                if (idx < attrs.length - 1) {
                    projText += ',';
                }
            });
            return projText;
        };
        return {
            projText: translateInner(entity, projection, './'),
            as: as,
        };
    };
    SqlTranslator.prototype.translateSelectInner = function (entity, selection, initialNumber, refAlias, option) {
        var data = selection.data, filter = selection.filter, sorter = selection.sorter, indexFrom = selection.indexFrom, count = selection.count;
        var _a = this.analyzeJoin(entity, {
            projection: data,
            filter: filter,
            sorter: sorter,
        }, initialNumber), fromText = _a.from, aliasDict = _a.aliasDict, projectionRefAlias = _a.projectionRefAlias, filterRefAlias = _a.filterRefAlias, currentNumber = _a.currentNumber;
        (0, assert_1.default)((0, lodash_1.intersection)((0, lodash_1.keys)(refAlias), (0, lodash_1.keys)(filterRefAlias)).length === 0, 'filter中的#node结点定义有重复');
        (0, lodash_1.assign)(refAlias, filterRefAlias);
        var projText = this.translateProjection(entity, data, aliasDict, projectionRefAlias).projText;
        var _b = this.translateFilter(entity, filter, aliasDict, refAlias, currentNumber, option), filterText = _b.stmt, currentNumber2 = _b.currentNumber;
        var sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);
        return {
            stmt: this.populateSelectStmt(projText, fromText, aliasDict, filterText, sorterText, undefined, indexFrom, count, option, selection),
            currentNumber: currentNumber2,
            filterStmt: filterText,
        };
    };
    SqlTranslator.prototype.translateSelect = function (entity, selection, option) {
        var stmt = this.translateSelectInner(entity, selection, 1, {}, option).stmt;
        return stmt;
    };
    SqlTranslator.prototype.translateWhere = function (entity, selection, option) {
        var filterStmt = this.translateSelectInner(entity, selection, 1, {}, option).filterStmt;
        return filterStmt;
    };
    SqlTranslator.prototype.translateAggregate = function (entity, aggregation, option) {
        var data = aggregation.data, filter = aggregation.filter, sorter = aggregation.sorter, indexFrom = aggregation.indexFrom, count = aggregation.count;
        var _a = this.analyzeJoin(entity, {
            aggregation: data,
            filter: filter,
            sorter: sorter,
        }, 1), fromText = _a.from, aliasDict = _a.aliasDict, projectionRefAlias = _a.projectionRefAlias, filterRefAlias = _a.filterRefAlias, currentNumber = _a.currentNumber;
        var projText = '';
        var groupByText = '';
        for (var k in data) {
            if (k === '#aggr') {
                var _b = this.translateProjection(entity, data[k], aliasDict, projectionRefAlias, '#data'), projSubText = _b.projText, as = _b.as;
                if (!projText) {
                    projText = projSubText;
                }
                else {
                    projText += ", ".concat(projSubText);
                }
                groupByText = as;
            }
            else {
                var projSubText = this.translateProjection(entity, data[k], aliasDict, projectionRefAlias, undefined, true).projText;
                var projSubText2 = '';
                if (k.startsWith('#max')) {
                    projSubText2 = "max(".concat(projSubText, ") as `").concat(k, "`");
                }
                else if (k.startsWith('#min')) {
                    projSubText2 = "min(".concat(projSubText, ") as `").concat(k, "`");
                }
                else if (k.startsWith('#count')) {
                    projSubText2 = "count(".concat(projSubText, ") as `").concat(k, "`");
                }
                else if (k.startsWith('#sum')) {
                    projSubText2 = "sum(".concat(projSubText, ") as `").concat(k, "`");
                }
                else {
                    (0, assert_1.default)(k.startsWith('#avg'));
                    projSubText2 = "avg(".concat(projSubText, ") as `").concat(k, "`");
                }
                if (!projText) {
                    projText = projSubText2;
                }
                else {
                    projText += ", ".concat(projSubText2);
                }
            }
        }
        var filterText = this.translateFilter(entity, filter, aliasDict, {}, currentNumber, option).stmt;
        var sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);
        return this.populateSelectStmt(projText, fromText, aliasDict, filterText, sorterText, groupByText, indexFrom, count, option, undefined, aggregation);
    };
    SqlTranslator.prototype.translateCount = function (entity, selection, option) {
        var filter = selection.filter, count = selection.count;
        var _a = this.analyzeJoin(entity, {
            filter: filter,
        }), fromText = _a.from, aliasDict = _a.aliasDict, filterRefAlias = _a.filterRefAlias, currentNumber = _a.currentNumber;
        var projText = 'count(1) cnt';
        var filterText = this.translateFilter(entity, filter, aliasDict, filterRefAlias, currentNumber, option).stmt;
        if (count) {
            var subQuerySql = this.populateSelectStmt('1', fromText, aliasDict, filterText, undefined, undefined, undefined, undefined, option, Object.assign({}, selection, { indexFrom: 0, count: count }));
            return "select count(1) cnt from (".concat(subQuerySql, ") __tmp");
        }
        return this.populateSelectStmt(projText, fromText, aliasDict, filterText, undefined, undefined, undefined, undefined, option, selection);
    };
    SqlTranslator.prototype.translateRemove = function (entity, operation, option) {
        var filter = operation.filter, sorter = operation.sorter, indexFrom = operation.indexFrom, count = operation.count;
        (0, assert_1.default)(!sorter, '当前remove不支持sorter行为');
        var _a = this.analyzeJoin(entity, { filter: filter, sorter: sorter }), aliasDict = _a.aliasDict, filterRefAlias = _a.filterRefAlias, fromText = _a.from, currentNumber = _a.currentNumber;
        var alias = aliasDict['./'];
        var filterText = this.translateFilter(entity, filter, aliasDict, filterRefAlias, currentNumber, { includedDeleted: true }).stmt;
        // const sorterText = sorter && sorter.length > 0 ? this.translateSorter(entity, sorter, aliasDict) : undefined;
        return this.populateRemoveStmt(alias, fromText, aliasDict, filterText, /* sorterText */ undefined, indexFrom, count, option);
    };
    SqlTranslator.prototype.translateUpdate = function (entity, operation, option) {
        var attributes = this.schema[entity].attributes;
        var filter = operation.filter, sorter = operation.sorter, indexFrom = operation.indexFrom, count = operation.count, data = operation.data;
        (0, assert_1.default)(!sorter, '当前update不支持sorter行为');
        var _a = this.analyzeJoin(entity, { filter: filter, sorter: sorter }), aliasDict = _a.aliasDict, filterRefAlias = _a.filterRefAlias, fromText = _a.from, currentNumber = _a.currentNumber;
        var alias = aliasDict['./'];
        var updateText = '';
        for (var attr in data) {
            if (updateText) {
                updateText += ',';
            }
            (0, assert_1.default)(attributes.hasOwnProperty(attr));
            var value = this.translateAttrValue(attributes[attr].type, data[attr]);
            updateText += "`".concat(alias, "`.`").concat(attr, "` = ").concat(value);
        }
        var filterText = this.translateFilter(entity, filter, aliasDict, filterRefAlias, currentNumber).stmt;
        // const sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);
        return this.populateUpdateStmt(updateText, fromText, aliasDict, filterText, /* sorterText */ undefined, indexFrom, count, option);
    };
    SqlTranslator.prototype.translateDestroyEntity = function (entity, truncate) {
        var schema = this.schema;
        var _a = schema[entity], _b = _a.storageName, storageName = _b === void 0 ? entity : _b, view = _a.view;
        var sql;
        if (view) {
            sql = "drop view if exists `".concat(storageName, "`");
        }
        else {
            sql = truncate ? "truncate table `".concat(storageName, "`") : "drop table if exists `".concat(storageName, "`");
        }
        return sql;
    };
    SqlTranslator.prototype.escapeStringValue = function (value) {
        var result = "'".concat(value.replace(/'/g, '\\\'').replace(/"/g, '\\\"'), "'");
        return result;
    };
    return SqlTranslator;
}());
exports.SqlTranslator = SqlTranslator;
