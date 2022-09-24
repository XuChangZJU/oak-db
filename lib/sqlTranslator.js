"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SqlTranslator = void 0;
var tslib_1 = require("tslib");
var assert_1 = tslib_1.__importDefault(require("assert"));
var lodash_1 = require("lodash");
var types_1 = require("oak-domain/lib/types");
var relation_1 = require("oak-domain/lib/store/relation");
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
                    name: "".concat(entity, "_create_at"),
                    attributes: [{
                            name: '$$createAt$$',
                        }]
                }, {
                    name: "".concat(entity, "_update_at"),
                    attributes: [{
                            name: '$$updateAt$$',
                        }],
                }, {
                    name: "".concat(entity, "_trigger_ts"),
                    attributes: [{
                            name: '$$triggerTimestamp$$',
                        }],
                }
            ];
            var _loop_1 = function (attr) {
                if (attributes[attr].type === 'ref') {
                    if (!(indexes === null || indexes === void 0 ? void 0 : indexes.find(function (ele) { return ele.attributes[0].name === attr; }))) {
                        intrinsticIndexes.push({
                            name: "".concat(entity, "_fk_").concat(attr),
                            attributes: [{
                                    name: attr,
                                }]
                        });
                    }
                }
                if (attr === 'entity' && attributes[attr].type === 'varchar') {
                    var entityIdDef = attributes.entityId;
                    if ((entityIdDef === null || entityIdDef === void 0 ? void 0 : entityIdDef.type) === 'varchar') {
                        if (!(indexes === null || indexes === void 0 ? void 0 : indexes.find(function (ele) { var _a; return ele.attributes[0].name === 'entity' && ((_a = ele.attributes[1]) === null || _a === void 0 ? void 0 : _a.name) === 'entityId'; }))) {
                            intrinsticIndexes.push({
                                name: "".concat(entity, "_fk_entity_entityId"),
                                attributes: [{
                                        name: 'entity',
                                    }, {
                                        name: 'entityId',
                                    }]
                            });
                        }
                    }
                }
            };
            // 增加外键上的索引
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
        var attrs = Object.keys(data[0]).filter(function (ele) { return attributes.hasOwnProperty(ele); });
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
        var projection = _a.projection, filter = _a.filter, sorter = _a.sorter, isStat = _a.isStat;
        var schema = this.schema;
        var number = initialNumber || 1;
        var projectionRefAlias = {};
        var filterRefAlias = {};
        var extraWhere = '';
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
                            from += " left join `".concat(_this.getStorageName(op), "` `").concat(alias2, "` on `").concat(alias, "`.`entityId` = `").concat(alias2, "`.`id`");
                            extraWhere += "`".concat(alias, "`.`entity` = '").concat(op, "'");
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
                    else {
                        // 不支持一对多
                        (0, assert_1.default)(rel === 0 || rel === 1);
                    }
                }
            });
            if (node['#id']) {
                (0, assert_1.default)(!filterRefAlias[node['#id']]);
                (0, lodash_1.assign)(filterRefAlias, (_b = {},
                    _b[node['#id']] = alias,
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
                    from += " left join `".concat(_this.getStorageName(attr), "` `").concat(alias2, "` on `").concat(alias, "`.`entityId` = `").concat(alias2, "`.`id`");
                    extraWhere += "`".concat(alias, "`.`entity` = '").concat(attr, "'");
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
                        from += " left join `".concat(_this.getStorageName(attr), "` `").concat(alias2, "` on `").concat(alias, "`.`entityId` = `").concat(alias2, "`.`id`");
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
                    extraWhere += "`".concat(alias, "`.`entity` = '").concat(attr, "'");
                }
            });
            if (node['#id']) {
                (0, assert_1.default)(!projectionRefAlias[node['#id']]);
                (0, lodash_1.assign)(projectionRefAlias, (_b = {},
                    _b[node['#id']] = alias,
                    _b));
            }
        };
        if (projection) {
            analyzeProjectionNode({ node: projection, path: './', entityName: entity, alias: alias });
        }
        return {
            aliasDict: aliasDict,
            from: from,
            projectionRefAlias: projectionRefAlias,
            filterRefAlias: filterRefAlias,
            extraWhere: extraWhere,
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
            return " ".concat(SQL_OP[attr], " ").concat(this.translateAttrValue(type, value));
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
    SqlTranslator.prototype.translateElement = function (attr, value) {
        (0, assert_1.default)(attr === '$exists'); // only support one operator now
        if (value) {
            return ' is not null';
        }
        return ' is null';
    };
    SqlTranslator.prototype.translateEvaluation = function (attr, value, entity, alias, type, initialNumber, refAlias) {
        switch (attr) {
            case '$text': {
                // fulltext search
                return {
                    stmt: this.translateFullTextSearch(value, entity, alias),
                    currentNumber: initialNumber,
                };
            }
            case '$in':
            case '$nin': {
                var IN_OP = {
                    $in: 'in',
                    $nin: 'not in',
                };
                if (value instanceof Array) {
                    var values = value.map(function (v) {
                        if (['varchar', 'char', 'text', 'nvarchar', 'ref'].includes(type)) {
                            return "'".concat(v, "'");
                        }
                        else {
                            return "".concat(v);
                        }
                    });
                    if (values.length > 0) {
                        return {
                            stmt: " ".concat(IN_OP[attr], "(").concat(values.join(','), ")"),
                            currentNumber: initialNumber,
                        };
                    }
                    else {
                        if (attr === '$in') {
                            return {
                                stmt: ' in (null)',
                                currentNumber: initialNumber,
                            };
                        }
                        else {
                            return {
                                stmt: ' is not null',
                                currentNumber: initialNumber,
                            };
                        }
                    }
                }
                else {
                    // sub query
                    var _a = this.translateSelectInner(value.entity, value, initialNumber, refAlias, undefined), subQueryStmt = _a.stmt, currentNumber = _a.currentNumber;
                    return {
                        stmt: " ".concat(IN_OP[attr], "(").concat(subQueryStmt, ")"),
                        currentNumber: currentNumber,
                    };
                }
            }
            default: {
                (0, assert_1.default)('$between' === attr);
                var values = value.map(function (v) {
                    if (['varchar', 'char', 'text', 'nvarchar', 'ref'].includes(type)) {
                        return "'".concat(v, "'");
                    }
                    else {
                        return "".concat(v);
                    }
                });
                return {
                    stmt: " between ".concat(values[0], " and ").concat(values[1]),
                    currentNumber: initialNumber,
                };
            }
        }
    };
    SqlTranslator.prototype.translateFilter = function (entity, selection, aliasDict, filterRefAlias, initialNumber, extraWhere, option) {
        var _this = this;
        var schema = this.schema;
        var filter = selection.filter;
        var currentNumber = initialNumber;
        var translateInner = function (entity2, path, filter2, type) {
            var alias = aliasDict[path];
            var attributes = schema[entity2].attributes;
            var whereText = type ? '' : _this.getDefaultSelectFilter(alias, option);
            if (filter2) {
                var attrs = Object.keys(filter2).filter(function (ele) { return !ele.startsWith('#'); });
                attrs.forEach(function (attr) {
                    if (whereText) {
                        whereText += ' and ';
                    }
                    whereText + '(';
                    if (['$and', '$or', '$xor', '$not'].includes(attr)) {
                        var result = '';
                        switch (attr) {
                            case '$and':
                            case '$or':
                            case '$xor': {
                                var logicQueries_1 = filter2[attr];
                                logicQueries_1.forEach(function (logicQuery, index) {
                                    var sql = translateInner(entity2, path, logicQuery);
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
                                var sql = translateInner(entity2, path, logicQuery);
                                if (sql) {
                                    whereText += " not (".concat(translateInner(entity2, path, logicQuery), ")");
                                    break;
                                }
                            }
                        }
                    }
                    else if (attr.toLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                        // expression
                        whereText += " (".concat(_this.translateExpression(alias, filter2[attr], filterRefAlias), ")");
                    }
                    else if (['$gt', '$gte', '$lt', '$lte', '$eq', '$ne', '$startsWith', '$endsWith', '$includes'].includes(attr)) {
                        whereText += _this.translateComparison(attr, filter2[attr], type);
                    }
                    else if (['$exists'].includes(attr)) {
                        whereText += _this.translateElement(attr, filter2[attr]);
                    }
                    else if (['$text', '$in', '$nin', '$between'].includes(attr)) {
                        var _a = _this.translateEvaluation(attr, filter2[attr], entity2, alias, type, initialNumber, filterRefAlias), stmt = _a.stmt, cn2 = _a.currentNumber;
                        whereText += stmt;
                        currentNumber = cn2;
                    }
                    else {
                        var rel = (0, relation_1.judgeRelation)(_this.schema, entity2, attr);
                        if (rel === 2) {
                            whereText += " ".concat(translateInner(attr, "".concat(path).concat(attr, "/"), filter2[attr]));
                        }
                        else if (typeof rel === 'string') {
                            whereText += " ".concat(translateInner(rel, "".concat(path).concat(attr, "/"), filter2[attr]));
                        }
                        else {
                            (0, assert_1.default)(attributes.hasOwnProperty(attr), "\u975E\u6CD5\u7684\u5C5E\u6027".concat(attr));
                            var type2 = attributes[attr].type;
                            //                                 assert (type2 !== 'ref');
                            if (typeof filter2[attr] === 'object' && Object.keys(filter2[attr])[0] && Object.keys(filter2[attr])[0].startsWith('$')) {
                                whereText += " `".concat(alias, "`.`").concat(attr, "` ").concat(translateInner(entity2, path, filter2[attr], type2));
                            }
                            else {
                                whereText += " `".concat(alias, "`.`").concat(attr, "` = ").concat(_this.translateAttrValue(type2, filter2[attr]));
                            }
                        }
                    }
                    whereText + ')';
                });
            }
            if (!whereText) {
                whereText = 'true'; // 如果为空就赋一个永真条件，以便处理and
            }
            return whereText;
        };
        var where = translateInner(entity, './', filter);
        if (extraWhere && where) {
            return {
                stmt: "".concat(extraWhere, " and ").concat(where),
                currentNumber: currentNumber,
            };
        }
        return {
            stmt: extraWhere || where,
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
                return _this.translateExpression(alias, sortAttr[attr], {});
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
    SqlTranslator.prototype.translateProjection = function (entity, projection, aliasDict, projectionRefAlias) {
        var _this = this;
        var schema = this.schema;
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
                if (attr.toLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                    var exprText = _this.translateExpression(alias, projection2[attr], projectionRefAlias);
                    projText += " ".concat(exprText, " as ").concat(prefix).concat(attr);
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
                            projText += " ".concat(_this.translateAttrProjection(type, alias, attr), " as `").concat(prefix).concat(attr, "`");
                        }
                        else {
                            (0, assert_1.default)(typeof projection2 === 'string');
                            projText += " ".concat(_this.translateAttrProjection(type, alias, attr), " as `").concat(prefix).concat(projection2[attr], "`");
                        }
                    }
                }
                if (idx < attrs.length - 1) {
                    projText += ',';
                }
            });
            return projText;
        };
        return translateInner(entity, projection, './');
    };
    SqlTranslator.prototype.translateSelectInner = function (entity, selection, initialNumber, refAlias, option) {
        var data = selection.data, filter = selection.filter, sorter = selection.sorter, indexFrom = selection.indexFrom, count = selection.count;
        var _a = this.analyzeJoin(entity, {
            projection: data,
            filter: filter,
            sorter: sorter,
        }, initialNumber), fromText = _a.from, aliasDict = _a.aliasDict, projectionRefAlias = _a.projectionRefAlias, extraWhere = _a.extraWhere, filterRefAlias = _a.filterRefAlias, currentNumber = _a.currentNumber;
        (0, assert_1.default)((0, lodash_1.intersection)((0, lodash_1.keys)(refAlias), (0, lodash_1.keys)(filterRefAlias)).length === 0, 'filter中的#node结点定义有重复');
        (0, lodash_1.assign)(refAlias, filterRefAlias);
        var projText = this.translateProjection(entity, data, aliasDict, projectionRefAlias);
        var _b = this.translateFilter(entity, selection, aliasDict, refAlias, currentNumber, extraWhere, option), filterText = _b.stmt, currentNumber2 = _b.currentNumber;
        var sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);
        return {
            stmt: this.populateSelectStmt(projText, fromText, selection, aliasDict, filterText, sorterText, indexFrom, count, option),
            currentNumber: currentNumber2,
        };
    };
    SqlTranslator.prototype.translateSelect = function (entity, selection, option) {
        var stmt = this.translateSelectInner(entity, selection, 1, {}, option).stmt;
        return stmt;
    };
    SqlTranslator.prototype.translateCount = function (entity, selection, option) {
        var filter = selection.filter, count = selection.count;
        var _a = this.analyzeJoin(entity, {
            filter: filter,
        }), fromText = _a.from, aliasDict = _a.aliasDict, extraWhere = _a.extraWhere, filterRefAlias = _a.filterRefAlias, currentNumber = _a.currentNumber;
        var projText = 'count(1)';
        var filterText = this.translateFilter(entity, selection, aliasDict, filterRefAlias, currentNumber, extraWhere, option).stmt;
        if (count) {
            var subQuerySql = this.populateSelectStmt('1', fromText, Object.assign({}, selection, { indexFrom: 0 }), aliasDict, filterText, undefined, undefined, undefined, option);
            return "select count(1) from (".concat(subQuerySql, ")");
        }
        return this.populateSelectStmt(projText, fromText, selection, aliasDict, filterText, undefined, undefined, undefined, option);
    };
    SqlTranslator.prototype.translateRemove = function (entity, operation, option) {
        var filter = operation.filter, sorter = operation.sorter, indexFrom = operation.indexFrom, count = operation.count;
        var _a = this.analyzeJoin(entity, { filter: filter, sorter: sorter }), aliasDict = _a.aliasDict, filterRefAlias = _a.filterRefAlias, extraWhere = _a.extraWhere, fromText = _a.from, currentNumber = _a.currentNumber;
        var alias = aliasDict['./'];
        var filterText = this.translateFilter(entity, operation, aliasDict, filterRefAlias, currentNumber, extraWhere).stmt;
        var sorterText = sorter && sorter.length > 0 ? this.translateSorter(entity, sorter, aliasDict) : undefined;
        return this.populateRemoveStmt(alias, fromText, aliasDict, filterText, sorterText, indexFrom, count, option);
    };
    SqlTranslator.prototype.translateUpdate = function (entity, operation, option) {
        var attributes = this.schema[entity].attributes;
        var filter = operation.filter, sorter = operation.sorter, indexFrom = operation.indexFrom, count = operation.count, data = operation.data;
        var _a = this.analyzeJoin(entity, { filter: filter, sorter: sorter }), aliasDict = _a.aliasDict, filterRefAlias = _a.filterRefAlias, extraWhere = _a.extraWhere, fromText = _a.from, currentNumber = _a.currentNumber;
        var alias = aliasDict['./'];
        var updateText = '';
        for (var attr in data) {
            if (updateText) {
                updateText += ',';
            }
            (0, assert_1.default)(attributes.hasOwnProperty(attr) && attributes[attr].type !== 'ref');
            var value = this.translateAttrValue(attributes[attr].type, data[attr]);
            updateText += "`".concat(alias, "`.`").concat(attr, "` = ").concat(value);
        }
        var filterText = this.translateFilter(entity, operation, aliasDict, filterRefAlias, currentNumber, extraWhere).stmt;
        var sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);
        return this.populateUpdateStmt(updateText, fromText, aliasDict, filterText, sorterText, indexFrom, count, option);
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
        var result = "'".concat(value.replace(/'/g, '\\\''), "'");
        return result;
    };
    return SqlTranslator;
}());
exports.SqlTranslator = SqlTranslator;
