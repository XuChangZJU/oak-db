"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MySqlTranslator = void 0;
var tslib_1 = require("tslib");
var assert_1 = tslib_1.__importDefault(require("assert"));
var util_1 = require("util");
var lodash_1 = require("lodash");
var sqlTranslator_1 = require("../sqlTranslator");
var GeoTypes = [
    {
        type: 'point',
        name: "Point"
    },
    {
        type: 'path',
        name: "LineString",
        element: 'point',
    },
    {
        name: "MultiLineString",
        element: "path",
        multiple: true,
    },
    {
        type: 'polygon',
        name: "Polygon",
        element: "path"
    },
    {
        name: "MultiPoint",
        element: "point",
        multiple: true,
    },
    {
        name: "MultiPolygon",
        element: "polygon",
        multiple: true,
    }
];
function transformGeoData(data) {
    if (data instanceof Array) {
        var element_1 = data[0];
        if (element_1 instanceof Array) {
            return " GeometryCollection(".concat(data.map(function (ele) { return transformGeoData(ele); }).join(','), ")");
        }
        else {
            var geoType_1 = GeoTypes.find(function (ele) { return ele.type === element_1.type; });
            if (!geoType_1) {
                throw new Error("".concat(element_1.type, " is not supported in MySQL"));
            }
            var multiGeoType = GeoTypes.find(function (ele) { return ele.element === geoType_1.type && ele.multiple; });
            return " ".concat(multiGeoType.name, "(").concat(data.map(function (ele) { return transformGeoData(ele); }).join(','), ")");
        }
    }
    else {
        var type_1 = data.type, coordinate = data.coordinate;
        var geoType = GeoTypes.find(function (ele) { return ele.type === type_1; });
        if (!geoType) {
            throw new Error("".concat(data.type, " is not supported in MySQL"));
        }
        var element_2 = geoType.element, name_1 = geoType.name;
        if (!element_2) {
            // Point
            return " ".concat(name_1, "(").concat(coordinate.join(','), ")");
        }
        // Polygon or Linestring
        return " ".concat(name_1, "(").concat(coordinate.map(function (ele) { return transformGeoData({
            type: element_2,
            coordinate: ele,
        }); }), ")");
    }
}
var MySqlTranslator = /** @class */ (function (_super) {
    tslib_1.__extends(MySqlTranslator, _super);
    function MySqlTranslator(schema) {
        var _this = _super.call(this, schema) || this;
        _this.maxAliasLength = 63;
        // MySQL为geometry属性默认创建索引
        _this.makeUpSchema();
        return _this;
    }
    MySqlTranslator.prototype.getDefaultSelectFilter = function (alias, option) {
        if (option === null || option === void 0 ? void 0 : option.includedDeleted) {
            return '';
        }
        return " `".concat(alias, "`.`$$deleteAt$$` is null");
    };
    MySqlTranslator.prototype.makeUpSchema = function () {
        for (var entity in this.schema) {
            var _a = this.schema[entity], attributes = _a.attributes, indexes = _a.indexes;
            var geoIndexes = [];
            var _loop_1 = function (attr) {
                if (attributes[attr].type === 'geometry') {
                    var geoIndex = indexes === null || indexes === void 0 ? void 0 : indexes.find(function (idx) {
                        var _a;
                        return ((_a = idx.config) === null || _a === void 0 ? void 0 : _a.type) === 'spatial' && idx.attributes.find(function (attrDef) { return attrDef.name === attr; });
                    });
                    if (!geoIndex) {
                        geoIndexes.push({
                            name: "".concat(entity, "_geo_").concat(attr),
                            attributes: [{
                                    name: attr,
                                }],
                            config: {
                                type: 'spatial',
                            }
                        });
                    }
                }
            };
            for (var attr in attributes) {
                _loop_1(attr);
            }
            if (geoIndexes.length > 0) {
                if (indexes) {
                    indexes.push.apply(indexes, geoIndexes);
                }
                else {
                    (0, lodash_1.assign)(this.schema[entity], {
                        indexes: geoIndexes,
                    });
                }
            }
        }
    };
    MySqlTranslator.prototype.populateDataTypeDef = function (type, params) {
        if (['date', 'datetime', 'time', 'sequence'].includes(type)) {
            return 'bigint ';
        }
        if (['object', 'array'].includes(type)) {
            return 'text ';
        }
        if (['image', 'function'].includes(type)) {
            return 'text ';
        }
        if (type === 'ref') {
            return 'char(36)';
        }
        if (MySqlTranslator.withLengthDataTypes.includes(type)) {
            if (params) {
                var length_1 = params.length;
                return "".concat(type, "(").concat(length_1, ") ");
            }
            else {
                var length_2 = MySqlTranslator.dataTypeDefaults[type].length;
                return "".concat(type, "(").concat(length_2, ") ");
            }
        }
        if (MySqlTranslator.withPrecisionDataTypes.includes(type)) {
            if (params) {
                var precision = params.precision, scale = params.scale;
                if (typeof scale === 'number') {
                    return "".concat(type, "(").concat(precision, ", ").concat(scale, ") ");
                }
                return "".concat(type, "(").concat(precision, ")");
            }
            else {
                var _a = MySqlTranslator.dataTypeDefaults[type], precision = _a.precision, scale = _a.scale;
                if (typeof scale === 'number') {
                    return "".concat(type, "(").concat(precision, ", ").concat(scale, ") ");
                }
                return "".concat(type, "(").concat(precision, ")");
            }
        }
        if (MySqlTranslator.withWidthDataTypes.includes(type)) {
            (0, assert_1.default)(type === 'int');
            var width = params.width;
            switch (width) {
                case 1: {
                    return 'tinyint';
                }
                case 2: {
                    return 'smallint';
                }
                case 3: {
                    return 'mediumint';
                }
                case 4: {
                    return 'int';
                }
                default: {
                    return 'bigint';
                }
            }
        }
        return "".concat(type, " ");
    };
    MySqlTranslator.prototype.translateAttrProjection = function (dataType, alias, attr) {
        switch (dataType) {
            case 'geometry': {
                return " st_astext(`".concat(alias, "`.`").concat(attr, "`)");
            }
            default: {
                return " `".concat(alias, "`.`").concat(attr, "`");
            }
        }
    };
    MySqlTranslator.prototype.translateAttrValue = function (dataType, value) {
        if (value === null || value === undefined) {
            return 'null';
        }
        switch (dataType) {
            case 'geometry': {
                return transformGeoData(value);
            }
            case 'datetime':
            case 'time':
            case 'date': {
                if (value instanceof Date) {
                    return "".concat(value.valueOf());
                }
                else if (typeof value === 'number') {
                    return "".concat(value);
                }
                return "'".concat((new Date(value)).valueOf(), "'");
            }
            case 'object':
            case 'array': {
                return this.escapeStringValue(JSON.stringify(value));
            }
            /* case 'function': {
                return `'${Buffer.from(value.toString()).toString('base64')}'`;
            } */
            default: {
                if (typeof value === 'string') {
                    return this.escapeStringValue(value);
                }
                return value;
            }
        }
    };
    MySqlTranslator.prototype.translateFullTextSearch = function (value, entity, alias) {
        var $search = value.$search;
        var indexes = this.schema[entity].indexes;
        var ftIndex = indexes && indexes.find(function (ele) {
            var config = ele.config;
            return config && config.type === 'fulltext';
        });
        (0, assert_1.default)(ftIndex);
        var attributes = ftIndex.attributes;
        var columns2 = attributes.map(function (_a) {
            var name = _a.name;
            return "".concat(alias, ".").concat(name);
        });
        return " match(".concat(columns2.join(','), ") against ('").concat($search, "' in natural language mode)");
    };
    MySqlTranslator.prototype.translateCreateEntity = function (entity, options) {
        var _this = this;
        var replace = options === null || options === void 0 ? void 0 : options.replace;
        var schema = this.schema;
        var entityDef = schema[entity];
        var storageName = entityDef.storageName, attributes = entityDef.attributes, indexes = entityDef.indexes, view = entityDef.view;
        var hasSequence = false;
        // todo view暂还不支持
        var entityType = view ? 'view' : 'table';
        var sql = "create ".concat(entityType, " ");
        if (storageName) {
            sql += "`".concat(storageName, "` ");
        }
        else {
            sql += "`".concat(entity, "` ");
        }
        if (view) {
            throw new Error(' view unsupported yet');
        }
        else {
            sql += '(';
            // 翻译所有的属性
            Object.keys(attributes).forEach(function (attr, idx) {
                var attrDef = attributes[attr];
                var type = attrDef.type, params = attrDef.params, defaultValue = attrDef.default, unique = attrDef.unique, notNull = attrDef.notNull, sequenceStart = attrDef.sequenceStart;
                sql += "`".concat(attr, "` ");
                sql += _this.populateDataTypeDef(type, params);
                if (notNull || type === 'geometry') {
                    sql += ' not null ';
                }
                if (unique) {
                    sql += ' unique ';
                }
                if (sequenceStart) {
                    if (hasSequence) {
                        throw new Error("\u300C".concat(entity, "\u300D\u53EA\u80FD\u6709\u4E00\u4E2Asequence\u5217"));
                    }
                    hasSequence = sequenceStart;
                    sql += ' auto_increment unique ';
                }
                if (defaultValue !== undefined) {
                    (0, assert_1.default)(type !== 'ref');
                    sql += " default ".concat(_this.translateAttrValue(type, defaultValue));
                }
                if (attr === 'id') {
                    sql += ' primary key';
                }
                if (idx < Object.keys(attributes).length - 1) {
                    sql += ',\n';
                }
            });
            // 翻译索引信息
            if (indexes) {
                sql += ',\n';
                indexes.forEach(function (_a, idx) {
                    var name = _a.name, attributes = _a.attributes, config = _a.config;
                    var _b = config || {}, unique = _b.unique, type = _b.type, parser = _b.parser;
                    if (unique) {
                        sql += ' unique ';
                    }
                    else if (type === 'fulltext') {
                        sql += ' fulltext ';
                    }
                    else if (type === 'spatial') {
                        sql += ' spatial ';
                    }
                    sql += "index ".concat(name, " ");
                    if (type === 'hash') {
                        sql += " using hash ";
                    }
                    sql += '(';
                    var includeDeleteAt = false;
                    attributes.forEach(function (_a, idx2) {
                        var name = _a.name, size = _a.size, direction = _a.direction;
                        sql += "`".concat(name, "`");
                        if (size) {
                            sql += " (".concat(size, ")");
                        }
                        if (direction) {
                            sql += " ".concat(direction);
                        }
                        if (idx2 < attributes.length - 1) {
                            sql += ',';
                        }
                        if (name === '$$deleteAt$$') {
                            includeDeleteAt = true;
                        }
                    });
                    if (!includeDeleteAt && !type) {
                        sql += ', $$deleteAt$$';
                    }
                    sql += ')';
                    if (parser) {
                        sql += " with parser ".concat(parser);
                    }
                    if (idx < indexes.length - 1) {
                        sql += ',\n';
                    }
                });
            }
        }
        sql += ')';
        if (typeof hasSequence === 'number') {
            sql += "auto_increment = ".concat(hasSequence);
        }
        if (!replace) {
            return [sql];
        }
        return ["drop ".concat(entityType, "  if exists `").concat(storageName || entity, "`;"), sql];
    };
    MySqlTranslator.prototype.translateFnName = function (fnName, argumentNumber) {
        switch (fnName) {
            case '$add': {
                var result = '%s';
                while (--argumentNumber > 0) {
                    result += ' + %s';
                }
                return result;
            }
            case '$subtract': {
                (0, assert_1.default)(argumentNumber === 2);
                return '%s - %s';
            }
            case '$multiply': {
                var result = '%s';
                while (--argumentNumber > 0) {
                    result += ' * %s';
                }
                return result;
            }
            case '$divide': {
                (0, assert_1.default)(argumentNumber === 2);
                return '%s / %s';
            }
            case '$abs': {
                return 'ABS(%s)';
            }
            case '$round': {
                (0, assert_1.default)(argumentNumber === 2);
                return 'ROUND(%s, %s)';
            }
            case '$ceil': {
                return 'CEIL(%s)';
            }
            case '$floor': {
                return 'FLOOR(%s)';
            }
            case '$pow': {
                (0, assert_1.default)(argumentNumber === 2);
                return 'POW(%s, %s)';
            }
            case '$gt': {
                (0, assert_1.default)(argumentNumber === 2);
                return '%s > %s';
            }
            case '$gte': {
                (0, assert_1.default)(argumentNumber === 2);
                return '%s >= %s';
            }
            case '$lt': {
                (0, assert_1.default)(argumentNumber === 2);
                return '%s < %s';
            }
            case '$lte': {
                return '%s <= %s';
            }
            case '$eq': {
                (0, assert_1.default)(argumentNumber === 2);
                return '%s = %s';
            }
            case '$ne': {
                (0, assert_1.default)(argumentNumber === 2);
                return '%s <> %s';
            }
            case '$startsWith': {
                (0, assert_1.default)(argumentNumber === 2);
                return '%s like CONCAT(%s, \'%\')';
            }
            case '$endsWith': {
                (0, assert_1.default)(argumentNumber === 2);
                return '%s like CONCAT(\'%\', %s)';
            }
            case '$includes': {
                (0, assert_1.default)(argumentNumber === 2);
                return '%s like CONCAT(\'%\', %s, \'%\')';
            }
            case '$true': {
                return '%s = true';
            }
            case '$false': {
                return '%s = false';
            }
            case '$and': {
                var result = '';
                for (var iter = 0; iter < argumentNumber; iter++) {
                    result += '%s';
                    if (iter < argumentNumber - 1) {
                        result += ' and ';
                    }
                }
                return result;
            }
            case '$or': {
                var result = '';
                for (var iter = 0; iter < argumentNumber; iter++) {
                    result += '%s';
                    if (iter < argumentNumber - 1) {
                        result += ' or ';
                    }
                }
                return result;
            }
            case '$not': {
                return 'not %s';
            }
            case '$year': {
                return 'YEAR(%s)';
            }
            case '$month': {
                return 'MONTH(%s)';
            }
            case '$weekday': {
                return 'WEEKDAY(%s)';
            }
            case '$weekOfYear': {
                return 'WEEKOFYEAR(%s)';
            }
            case '$day': {
                return 'DAY(%s)';
            }
            case '$dayOfMonth': {
                return 'DAYOFMONTH(%s)';
            }
            case '$dayOfWeek': {
                return 'DAYOFWEEK(%s)';
            }
            case '$dayOfYear': {
                return 'DAYOFYEAR(%s)';
            }
            case '$dateDiff': {
                (0, assert_1.default)(argumentNumber === 3);
                return 'DATEDIFF(%s, %s, %s)';
            }
            case '$contains': {
                (0, assert_1.default)(argumentNumber === 2);
                return 'ST_CONTAINS(%s, %s)';
            }
            case '$distance': {
                (0, assert_1.default)(argumentNumber === 2);
                return 'ST_DISTANCE(%s, %s)';
            }
            default: {
                throw new Error("unrecoganized function ".concat(fnName));
            }
        }
    };
    MySqlTranslator.prototype.translateAttrInExpression = function (entity, attr, exprText) {
        var attributes = this.schema[entity].attributes;
        var type = attributes[attr].type;
        if (['date', 'time', 'datetime'].includes(type)) {
            // 从unix时间戵转成date类型参加expr的运算
            return "from_unixtime(".concat(exprText, " / 1000)");
        }
        return exprText;
    };
    MySqlTranslator.prototype.translateExpression = function (entity, alias, expression, refDict) {
        var _this = this;
        var translateConstant = function (constant) {
            if (constant instanceof Date) {
                return " ".concat(constant.valueOf());
            }
            else if (typeof constant === 'string') {
                return " '".concat(constant, "'");
            }
            else {
                (0, assert_1.default)(typeof constant === 'number');
                return " ".concat(constant);
            }
        };
        var translateInner = function (expr) {
            var k = Object.keys(expr);
            var result;
            if (k.includes('#attr')) {
                var attrText = "`".concat(alias, "`.`").concat((expr)['#attr'], "`");
                result = _this.translateAttrInExpression(entity, (expr)['#attr'], attrText);
            }
            else if (k.includes('#refId')) {
                var refId = (expr)['#refId'];
                var refAttr = (expr)['#refAttr'];
                (0, assert_1.default)(refDict[refId]);
                var attrText = "`".concat(refDict[refId][0], "`.`").concat(refAttr, "`");
                result = _this.translateAttrInExpression(entity, (expr)['#attr'], attrText);
            }
            else {
                (0, assert_1.default)(k.length === 1);
                if ((expr)[k[0]] instanceof Array) {
                    var fnName = _this.translateFnName(k[0], (expr)[k[0]].length);
                    var args = [fnName];
                    args.push.apply(args, (expr)[k[0]].map(function (ele) {
                        if (['string', 'number'].includes(typeof ele) || ele instanceof Date) {
                            return translateConstant(ele);
                        }
                        else {
                            return translateInner(ele);
                        }
                    }));
                    result = util_1.format.apply(null, args);
                }
                else {
                    var fnName = _this.translateFnName(k[0], 1);
                    var args = [fnName];
                    var arg = (expr)[k[0]];
                    if (['string', 'number'].includes(typeof arg) || arg instanceof Date) {
                        args.push(translateConstant(arg));
                    }
                    else {
                        args.push(translateInner(arg));
                    }
                    result = util_1.format.apply(null, args);
                }
            }
            return result;
        };
        return translateInner(expression);
    };
    MySqlTranslator.prototype.populateSelectStmt = function (projectionText, fromText, aliasDict, filterText, sorterText, groupByText, indexFrom, count, option) {
        // todo hint of use index
        var sql = "select ".concat(projectionText, " from ").concat(fromText);
        if (filterText) {
            sql += " where ".concat(filterText);
        }
        if (sorterText) {
            sql += " order by ".concat(sorterText);
        }
        if (groupByText) {
            sql += " group by ".concat(groupByText);
        }
        if (typeof indexFrom === 'number') {
            (0, assert_1.default)(typeof count === 'number');
            sql += " limit ".concat(indexFrom, ", ").concat(count);
        }
        if (option === null || option === void 0 ? void 0 : option.forUpdate) {
            sql += ' for update';
        }
        return sql;
    };
    MySqlTranslator.prototype.populateUpdateStmt = function (updateText, fromText, aliasDict, filterText, sorterText, indexFrom, count, option) {
        // todo using index
        (0, assert_1.default)(updateText);
        var sql = "update ".concat(fromText, " set ").concat(updateText);
        if (filterText) {
            sql += " where ".concat(filterText);
        }
        if (sorterText) {
            sql += " order by ".concat(sorterText);
        }
        if (typeof indexFrom === 'number') {
            (0, assert_1.default)(typeof count === 'number');
            sql += " limit ".concat(indexFrom, ", ").concat(count);
        }
        return sql;
    };
    MySqlTranslator.prototype.populateRemoveStmt = function (removeText, fromText, aliasDict, filterText, sorterText, indexFrom, count, option) {
        // todo using index
        var alias = aliasDict['./'];
        var now = Date.now();
        var sql = "update ".concat(fromText, " set `").concat(alias, "`.`$$deleteAt$$` = '").concat(now, "'");
        if (filterText) {
            sql += " where ".concat(filterText);
        }
        if (sorterText) {
            sql += " order by ".concat(sorterText);
        }
        if (typeof indexFrom === 'number') {
            (0, assert_1.default)(typeof count === 'number');
            sql += " limit ".concat(indexFrom, ", ").concat(count);
        }
        return sql;
    };
    MySqlTranslator.supportedDataTypes = [
        // numeric types
        "bit",
        "int",
        "integer",
        "tinyint",
        "smallint",
        "mediumint",
        "bigint",
        "float",
        "double",
        "double precision",
        "real",
        "decimal",
        "dec",
        "numeric",
        "fixed",
        "bool",
        "boolean",
        // date and time types
        "date",
        "datetime",
        "timestamp",
        "time",
        "year",
        // string types
        "char",
        "nchar",
        "national char",
        "varchar",
        "nvarchar",
        "national varchar",
        "blob",
        "text",
        "tinyblob",
        "tinytext",
        "mediumblob",
        "mediumtext",
        "longblob",
        "longtext",
        "enum",
        "set",
        "binary",
        "varbinary",
        // json data type
        "json",
        // spatial data types
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection"
    ];
    MySqlTranslator.spatialTypes = [
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection"
    ];
    MySqlTranslator.withLengthDataTypes = [
        "char",
        "varchar",
        "nvarchar",
        "binary",
        "varbinary"
    ];
    MySqlTranslator.withPrecisionDataTypes = [
        "decimal",
        "dec",
        "numeric",
        "fixed",
        "float",
        "double",
        "double precision",
        "real",
        "time",
        "datetime",
        "timestamp"
    ];
    MySqlTranslator.withScaleDataTypes = [
        "decimal",
        "dec",
        "numeric",
        "fixed",
        "float",
        "double",
        "double precision",
        "real"
    ];
    MySqlTranslator.unsignedAndZerofillTypes = [
        "int",
        "integer",
        "smallint",
        "tinyint",
        "mediumint",
        "bigint",
        "decimal",
        "dec",
        "numeric",
        "fixed",
        "float",
        "double",
        "double precision",
        "real"
    ];
    MySqlTranslator.withWidthDataTypes = [
        'int',
    ];
    MySqlTranslator.dataTypeDefaults = {
        "varchar": { length: 255 },
        "nvarchar": { length: 255 },
        "national varchar": { length: 255 },
        "char": { length: 1 },
        "binary": { length: 1 },
        "varbinary": { length: 255 },
        "decimal": { precision: 10, scale: 0 },
        "dec": { precision: 10, scale: 0 },
        "numeric": { precision: 10, scale: 0 },
        "fixed": { precision: 10, scale: 0 },
        "float": { precision: 12 },
        "double": { precision: 22 },
        "time": { precision: 0 },
        "datetime": { precision: 0 },
        "timestamp": { precision: 0 },
        "bit": { width: 1 },
        "int": { width: 11 },
        "integer": { width: 11 },
        "tinyint": { width: 4 },
        "smallint": { width: 6 },
        "mediumint": { width: 9 },
        "bigint": { width: 20 }
    };
    return MySqlTranslator;
}(sqlTranslator_1.SqlTranslator));
exports.MySqlTranslator = MySqlTranslator;
