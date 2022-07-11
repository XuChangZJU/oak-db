"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MySqlTranslator = void 0;
const assert_1 = __importDefault(require("assert"));
const util_1 = require("util");
const lodash_1 = require("lodash");
const luxon_1 = require("luxon");
const sqlTranslator_1 = require("../sqlTranslator");
const GeoTypes = [
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
        const element = data[0];
        if (element instanceof Array) {
            return ` GeometryCollection(${data.map(ele => transformGeoData(ele)).join(',')})`;
        }
        else {
            const geoType = GeoTypes.find(ele => ele.type === element.type);
            if (!geoType) {
                throw new Error(`${element.type} is not supported in MySQL`);
            }
            const multiGeoType = GeoTypes.find(ele => ele.element === geoType.type && ele.multiple);
            return ` ${multiGeoType.name}(${data.map(ele => transformGeoData(ele)).join(',')})`;
        }
    }
    else {
        const { type, coordinate } = data;
        const geoType = GeoTypes.find(ele => ele.type === type);
        if (!geoType) {
            throw new Error(`${data.type} is not supported in MySQL`);
        }
        const { element, name } = geoType;
        if (!element) {
            // Point
            return ` ${name}(${coordinate.join(',')})`;
        }
        // Polygon or Linestring
        return ` ${name}(${coordinate.map((ele) => transformGeoData({
            type: element,
            coordinate: ele,
        }))})`;
    }
}
class MySqlTranslator extends sqlTranslator_1.SqlTranslator {
    getDefaultSelectFilter(alias, hint) {
        if (hint?.includeDeleted) {
            return '';
        }
        return ` \`${alias}\`.\`$$deleteAt$$\` is null`;
    }
    modifySchema() {
        for (const entity in this.schema) {
            const { attributes, indexes } = this.schema[entity];
            const geoIndexes = [];
            for (const attr in attributes) {
                if (attributes[attr].type === 'geometry') {
                    const geoIndex = indexes?.find((idx) => idx.config?.type === 'spatial' && idx.attributes.find((attrDef) => attrDef.name === attr));
                    if (!geoIndex) {
                        geoIndexes.push({
                            name: `${entity}_geo_${attr}`,
                            attributes: [{
                                    name: attr,
                                }],
                            config: {
                                type: 'spatial',
                            }
                        });
                    }
                }
            }
            if (geoIndexes.length > 0) {
                if (indexes) {
                    indexes.push(...geoIndexes);
                }
                else {
                    (0, lodash_1.assign)(this.schema[entity], {
                        indexes: geoIndexes,
                    });
                }
            }
        }
    }
    constructor(schema) {
        super(schema);
        // MySQL为geometry属性默认创建索引
        this.modifySchema();
    }
    static supportedDataTypes = [
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
    static spatialTypes = [
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection"
    ];
    static withLengthDataTypes = [
        "char",
        "varchar",
        "nvarchar",
        "binary",
        "varbinary"
    ];
    static withPrecisionDataTypes = [
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
    static withScaleDataTypes = [
        "decimal",
        "dec",
        "numeric",
        "fixed",
        "float",
        "double",
        "double precision",
        "real"
    ];
    static unsignedAndZerofillTypes = [
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
    static withWidthDataTypes = [
        'int',
    ];
    static dataTypeDefaults = {
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
    maxAliasLength = 63;
    populateDataTypeDef(type, params) {
        if (MySqlTranslator.withLengthDataTypes.includes(type)) {
            if (params) {
                const { length } = params;
                return `${type}(${length}) `;
            }
            else {
                const { length } = MySqlTranslator.dataTypeDefaults[type];
                return `${type}(${length}) `;
            }
        }
        if (MySqlTranslator.withPrecisionDataTypes.includes(type)) {
            if (params) {
                const { precision, scale } = params;
                if (typeof scale === 'number') {
                    return `${type}(${precision}, ${scale}) `;
                }
                return `${type}(${precision})`;
            }
            else {
                const { precision, scale } = MySqlTranslator.dataTypeDefaults[type];
                if (typeof scale === 'number') {
                    return `${type}(${precision}, ${scale}) `;
                }
                return `${type}(${precision})`;
            }
        }
        if (MySqlTranslator.withWidthDataTypes.includes(type)) {
            (0, assert_1.default)(type === 'int');
            const { width } = params;
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
        if (['date'].includes(type)) {
            return 'datetime';
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
        return `${type} `;
    }
    translateAttrProjection(dataType, alias, attr) {
        switch (dataType) {
            case 'geometry': {
                return ` st_astext(\`${alias}\`.\`${attr}\`)`;
            }
            default: {
                return ` \`${alias}\`.\`${attr}\``;
            }
        }
    }
    translateAttrValue(dataType, value) {
        if (value === null) {
            return 'null';
        }
        switch (dataType) {
            case 'geometry': {
                return transformGeoData(value);
            }
            case 'date': {
                if (value instanceof Date) {
                    return luxon_1.DateTime.fromJSDate(value).toFormat('yyyy-LL-dd HH:mm:ss');
                }
                else if (typeof value === 'number') {
                    return luxon_1.DateTime.fromMillis(value).toFormat('yyyy-LL-dd HH:mm:ss');
                }
                return value;
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
    }
    translateFullTextSearch(value, entity, alias) {
        const { $search } = value;
        const { indexes } = this.schema[entity];
        const ftIndex = indexes && indexes.find((ele) => {
            const { config } = ele;
            return config && config.type === 'fulltext';
        });
        (0, assert_1.default)(ftIndex);
        const { attributes } = ftIndex;
        const columns2 = attributes.map(({ name }) => `${alias}.${name}`);
        return ` match(${columns2.join(',')}) against ('${$search}' in natural language mode)`;
    }
    translateCreateEntity(entity, options) {
        const replace = options?.replace;
        const { schema } = this;
        const entityDef = schema[entity];
        const { storageName, attributes, indexes, view } = entityDef;
        // todo view暂还不支持
        const entityType = view ? 'view' : 'table';
        let sql = `create ${entityType} `;
        if (storageName) {
            sql += `\`${storageName}\` `;
        }
        else {
            sql += `\`${entity}\` `;
        }
        if (view) {
            throw new Error(' view unsupported yet');
        }
        else {
            sql += '(';
            // 翻译所有的属性
            Object.keys(attributes).forEach((attr, idx) => {
                const attrDef = attributes[attr];
                const { type, params, default: defaultValue, unique, notNull, } = attrDef;
                sql += `\`${attr}\` `;
                sql += this.populateDataTypeDef(type, params);
                if (notNull || type === 'geometry') {
                    sql += ' not null ';
                }
                if (unique) {
                    sql += ' unique ';
                }
                if (defaultValue !== undefined) {
                    (0, assert_1.default)(type !== 'ref');
                    sql += ` default ${this.translateAttrValue(type, defaultValue)}`;
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
                indexes.forEach(({ name, attributes, config }, idx) => {
                    const { unique, type, parser } = config || {};
                    if (unique) {
                        sql += ' unique ';
                    }
                    else if (type === 'fulltext') {
                        sql += ' fulltext ';
                    }
                    else if (type === 'spatial') {
                        sql += ' spatial ';
                    }
                    sql += `index ${name} `;
                    if (type === 'hash') {
                        sql += ` using hash `;
                    }
                    sql += '(';
                    let includeDeleteAt = false;
                    attributes.forEach(({ name, size, direction }, idx2) => {
                        sql += `\`${name}\``;
                        if (size) {
                            sql += ` (${size})`;
                        }
                        if (direction) {
                            sql += ` ${direction}`;
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
                        sql += ` with parser ${parser}`;
                    }
                    if (idx < indexes.length - 1) {
                        sql += ',\n';
                    }
                });
            }
        }
        sql += ')';
        if (!replace) {
            return [sql];
        }
        return [`drop ${entityType} \`${storageName || entity}\`;`, sql];
    }
    translateFnName(fnName, argumentNumber) {
        switch (fnName) {
            case '$add': {
                return '%s + %s';
            }
            case '$subtract': {
                return '%s - %s';
            }
            case '$multiply': {
                return '%s * %s';
            }
            case '$divide': {
                return '%s / %s';
            }
            case '$abs': {
                return 'ABS(%s)';
            }
            case '$round': {
                return 'ROUND(%s, %s)';
            }
            case '$ceil': {
                return 'CEIL(%s)';
            }
            case '$floor': {
                return 'FLOOR(%s)';
            }
            case '$pow': {
                return 'POW(%s, %s)';
            }
            case '$gt': {
                return '%s > %s';
            }
            case '$gte': {
                return '%s >= %s';
            }
            case '$lt': {
                return '%s < %s';
            }
            case '$lte': {
                return '%s <= %s';
            }
            case '$eq': {
                return '%s = %s';
            }
            case '$ne': {
                return '%s <> %s';
            }
            case '$startsWith': {
                return '%s like CONCAT(%s, \'%\')';
            }
            case '$endsWith': {
                return '%s like CONCAT(\'%\', %s)';
            }
            case '$includes': {
                return '%s like CONCAT(\'%\', %s, \'%\')';
            }
            case '$true': {
                return '%s = true';
            }
            case '$false': {
                return '%s = false';
            }
            case '$and': {
                let result = '';
                for (let iter = 0; iter < argumentNumber; iter++) {
                    result += '%s';
                    if (iter < argumentNumber - 1) {
                        result += ' and ';
                    }
                }
                return result;
            }
            case '$or': {
                let result = '';
                for (let iter = 0; iter < argumentNumber; iter++) {
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
                return 'DATEDIFF(%s, %s, %s)';
            }
            case '$contains': {
                return 'ST_CONTAINS(%s, %s)';
            }
            case '$distance': {
                return 'ST_DISTANCE(%s, %s)';
            }
            default: {
                throw new Error(`unrecoganized function ${fnName}`);
            }
        }
    }
    translateExpression(alias, expression, refDict) {
        const translateConstant = (constant) => {
            if (typeof constant === 'string') {
                return `'${constant}'`;
            }
            else if (constant instanceof Date) {
                return `'${luxon_1.DateTime.fromJSDate(constant).toFormat('yyyy-LL-dd HH:mm:ss')}'`;
            }
            else {
                (0, assert_1.default)(typeof constant === 'number');
                return `${constant}`;
            }
        };
        const translateInner = (expr) => {
            const k = Object.keys(expr);
            let result;
            if (k.includes('#attr')) {
                const attrText = `\`${alias}\`.\`${(expr)['#attr']}\``;
                result = attrText;
            }
            else if (k.includes('#refId')) {
                const refId = (expr)['#refId'];
                const refAttr = (expr)['#refAttr'];
                (0, assert_1.default)(refDict[refId]);
                const attrText = `\`${refDict[refId]}\`.\`${refAttr}\``;
                result = attrText;
            }
            else {
                (0, assert_1.default)(k.length === 1);
                if ((expr)[k[0]] instanceof Array) {
                    const fnName = this.translateFnName(k[0], (expr)[k[0]].length);
                    const args = [fnName];
                    args.push(...(expr)[k[0]].map((ele) => {
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
                    const fnName = this.translateFnName(k[0], 1);
                    const args = [fnName];
                    const arg = (expr)[k[0]];
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
    }
    populateSelectStmt(projectionText, fromText, selection, aliasDict, filterText, sorterText, indexFrom, count) {
        const { hint } = selection;
        // todo hint of use index
        let sql = `select ${projectionText} from ${fromText}`;
        if (filterText) {
            sql += ` where ${filterText}`;
        }
        if (sorterText) {
            sql += ` order by ${sorterText}`;
        }
        if (typeof indexFrom === 'number') {
            (0, assert_1.default)(typeof count === 'number');
            sql += ` limit ${indexFrom}, ${count}`;
        }
        if (hint?.mysql?.forUpdate) {
            sql += ' for update';
        }
        return sql;
    }
    populateUpdateStmt(updateText, fromText, aliasDict, filterText, sorterText, indexFrom, count, params) {
        // todo using index
        const alias = aliasDict['./'];
        const now = luxon_1.DateTime.now().toFormat('yyyy-LL-dd HH:mm:ss');
        let sql = `update ${fromText} set ${updateText ? `${updateText},` : ''} \`${alias}\`.\`$$updateAt$$\` = '${now}'`;
        if (filterText) {
            sql += ` where ${filterText}`;
        }
        if (sorterText) {
            sql += ` order by ${sorterText}`;
        }
        if (typeof indexFrom === 'number') {
            (0, assert_1.default)(typeof count === 'number');
            sql += ` limit ${indexFrom}, ${count}`;
        }
        return sql;
    }
    populateRemoveStmt(removeText, fromText, aliasDict, filterText, sorterText, indexFrom, count, params) {
        // todo using index
        const alias = aliasDict['./'];
        const now = luxon_1.DateTime.now().toFormat('yyyy-LL-dd HH:mm:ss');
        let sql = `update ${fromText} set \`${alias}\`.\`$$deleteAt$$\` = '${now}'`;
        if (filterText) {
            sql += ` where ${filterText}`;
        }
        if (sorterText) {
            sql += ` order by ${sorterText}`;
        }
        if (typeof indexFrom === 'number') {
            (0, assert_1.default)(typeof count === 'number');
            sql += ` limit ${indexFrom}, ${count}`;
        }
        return sql;
    }
}
exports.MySqlTranslator = MySqlTranslator;
