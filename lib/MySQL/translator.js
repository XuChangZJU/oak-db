"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MySqlTranslator = void 0;
const tslib_1 = require("tslib");
const assert_1 = tslib_1.__importDefault(require("assert"));
const util_1 = require("util");
const lodash_1 = require("lodash");
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
    getDefaultSelectFilter(alias, option) {
        if (option?.includedDeleted) {
            return '';
        }
        return ` (\`${alias}\`.\`$$deleteAt$$\` is null)`;
    }
    makeUpSchema() {
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
        this.makeUpSchema();
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
    populateDataTypeDef(type, params, enumeration) {
        if (['date', 'datetime', 'time', 'sequence'].includes(type)) {
            return 'bigint ';
        }
        if (['object', 'array'].includes(type)) {
            return 'json ';
        }
        if (['image', 'function'].includes(type)) {
            return 'text ';
        }
        if (type === 'ref') {
            return 'char(36)';
        }
        if (type === 'money') {
            return 'bigint';
        }
        if (type === 'enum') {
            (0, assert_1.default)(enumeration);
            return `enum(${enumeration.map(ele => `'${ele}'`).join(',')})`;
        }
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
    translateObjectPredicate(predicate, alias, attr) {
        const translateInner = (o, p) => {
            let stmt2 = '';
            if (o instanceof Array) {
                o.forEach((ele, idx) => {
                    if (ele !== undefined && ele !== null) {
                        const part = translateInner(ele, `${p}[${idx}]`);
                        if (stmt2) {
                            stmt2 += ' and ';
                        }
                        stmt2 += `${part}`;
                    }
                });
            }
            else if (typeof o === 'object') {
                for (const attr2 in o) {
                    if (attr2 === '$and') {
                        o[attr2].forEach((ele) => {
                            const part = translateInner(ele, p);
                            if (stmt2) {
                                stmt2 += ' and ';
                            }
                            stmt2 += `${part}`;
                        });
                    }
                    else if (attr2 === '$or') {
                        let stmtOr = '';
                        o[attr2].forEach((ele) => {
                            const part = translateInner(ele, p);
                            if (stmtOr) {
                                stmtOr += ' or ';
                            }
                            stmtOr += `${part}`;
                        });
                        if (stmt2) {
                            stmt2 += ' and ';
                        }
                        stmt2 += `(${stmtOr})`;
                    }
                    else if (attr2 === '$contains') {
                        // json_contains，多值的包含关系
                        const value = JSON.stringify(o[attr2]);
                        if (stmt2) {
                            stmt2 += ' and ';
                        }
                        if (p) {
                            stmt2 += `(JSON_CONTAINS(${alias}.${attr}->>"$${p}", CAST('${value}' AS JSON)))`;
                        }
                        else {
                            stmt2 += `(JSON_CONTAINS(${alias}.${attr}, CAST('${value}' AS JSON)))`;
                        }
                    }
                    else if (attr2 === '$overlaps') {
                        // json_overlaps，多值的交叉关系
                        const value = JSON.stringify(o[attr2]);
                        if (stmt2) {
                            stmt2 += ' and ';
                        }
                        if (p) {
                            stmt2 += `(JSON_OVERLAPS(${alias}.${attr}->>"$${p}", CAST('${value}' AS JSON)))`;
                        }
                        else {
                            stmt2 += `(JSON_OVERLAPS(${alias}.${attr}, CAST('${value}' AS JSON)))`;
                        }
                    }
                    else if (attr2.startsWith('$')) {
                        if (stmt2) {
                            stmt2 += ' and ';
                        }
                        if (p) {
                            stmt2 += `(${alias}.${attr}->>"$${p}" ${this.translatePredicate(attr2, o[attr2])})`;
                        }
                        else {
                            stmt2 += `(${alias}.${attr} ${this.translatePredicate(attr2, o[attr2])})`;
                        }
                    }
                    else {
                        // 继续子对象解构
                        const attr3 = attr2.startsWith('.') ? attr2.slice(1) : attr2;
                        const part = translateInner(o[attr2], `${p}.${attr3}`);
                        if (stmt2) {
                            stmt2 += ' and ';
                        }
                        stmt2 += `${part}`;
                    }
                }
            }
            else {
                // 直接的属性处理
                if (stmt2) {
                    stmt2 += ' and ';
                }
                if (typeof o === 'string') {
                    if (p) {
                        stmt2 += `(${alias}.${attr}->>"$${p}" = '${o}')`;
                    }
                    else {
                        //  对根对象的字符串比较
                        stmt2 += `(${alias}.${attr} = '${o}')`;
                    }
                }
                else {
                    (0, assert_1.default)(p);
                    stmt2 += `(${alias}.${attr}->>"$${p}" = ${o})`;
                }
            }
            return stmt2;
        };
        return translateInner(predicate, '');
    }
    translateObjectProjection(projection, alias, attr, prefix) {
        let stmt = '';
        const translateInner = (o, p) => {
            if (o instanceof Array) {
                o.forEach((item, idx) => {
                    const p2 = `${p}[${idx}]`;
                    if (typeof item === 'number') {
                        if (stmt) {
                            stmt += ', ';
                        }
                        stmt += `${alias}.${attr}->>"$${p2}"`;
                        stmt += prefix ? ` as \`${prefix}.${attr}${p2}\`` : ` as \`${attr}${p2}\``;
                    }
                    else if (typeof item === 'object') {
                        translateInner(item, p2);
                    }
                });
            }
            else {
                for (const key in o) {
                    const p2 = `${p}.${key}`;
                    if (typeof o[key] === 'number') {
                        if (stmt) {
                            stmt += ', ';
                        }
                        stmt += `${alias}.${attr}->>"$${p2}"`;
                        stmt += prefix ? ` as \`${prefix}.${attr}${p2}\`` : ` as \`${attr}${p2}\``;
                    }
                    else {
                        translateInner(o[key], p2);
                    }
                }
            }
        };
        translateInner(projection, '');
        return stmt;
    }
    translateAttrValue(dataType, value) {
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
                    return `${value.valueOf()}`;
                }
                else if (typeof value === 'number') {
                    return `${value}`;
                }
                return `'${(new Date(value)).valueOf()}'`;
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
        let hasSequence = false;
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
                const { type, params, default: defaultValue, unique, notNull, sequenceStart, enumeration, } = attrDef;
                sql += `\`${attr}\` `;
                sql += this.populateDataTypeDef(type, params, enumeration);
                if (notNull || type === 'geometry') {
                    sql += ' not null ';
                }
                if (unique) {
                    sql += ' unique ';
                }
                if (sequenceStart) {
                    if (hasSequence) {
                        throw new Error(`「${entity}」只能有一个sequence列`);
                    }
                    hasSequence = sequenceStart;
                    sql += ' auto_increment unique ';
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
                    // 因为有deleteAt的存在，这里的unique没意义，只能框架自己去建立checker来处理
                    /* if (unique) {
                        sql += ' unique ';
                    }
                    else */ if (type === 'fulltext') {
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
        if (typeof hasSequence === 'number') {
            sql += `auto_increment = ${hasSequence}`;
        }
        if (!replace) {
            return [sql];
        }
        return [`drop ${entityType}  if exists \`${storageName || entity}\`;`, sql];
    }
    translateFnName(fnName, argumentNumber) {
        switch (fnName) {
            case '$add': {
                let result = '%s';
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
                let result = '%s';
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
            case '$mod': {
                return 'MOD(%s, %s)';
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
            case '$concat': {
                let result = ' concat(%s';
                while (--argumentNumber > 0) {
                    result += ', %s';
                }
                result += ')';
                return result;
            }
            default: {
                throw new Error(`unrecoganized function ${fnName}`);
            }
        }
    }
    translateAttrInExpression(entity, attr, exprText) {
        const { attributes } = this.schema[entity];
        const { type } = attributes[attr];
        if (['date', 'time', 'datetime'].includes(type)) {
            // 从unix时间戵转成date类型参加expr的运算
            return `from_unixtime(${exprText} / 1000)`;
        }
        return exprText;
    }
    translateExpression(entity, alias, expression, refDict) {
        const translateConstant = (constant) => {
            if (constant instanceof Date) {
                return ` from_unixtime(${constant.valueOf()}/1000)`;
            }
            else if (typeof constant === 'string') {
                return ` '${constant}'`;
            }
            else {
                (0, assert_1.default)(typeof constant === 'number');
                return ` ${constant}`;
            }
        };
        const translateInner = (expr) => {
            const k = Object.keys(expr);
            let result;
            if (k.includes('#attr')) {
                const attrText = `\`${alias}\`.\`${(expr)['#attr']}\``;
                result = this.translateAttrInExpression(entity, (expr)['#attr'], attrText);
            }
            else if (k.includes('#refId')) {
                const refId = (expr)['#refId'];
                const refAttr = (expr)['#refAttr'];
                (0, assert_1.default)(refDict[refId]);
                const attrText = `\`${refDict[refId][0]}\`.\`${refAttr}\``;
                result = this.translateAttrInExpression(entity, (expr)['#refAttr'], attrText);
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
    populateSelectStmt(projectionText, fromText, aliasDict, filterText, sorterText, groupByText, indexFrom, count, option) {
        // todo hint of use index
        let sql = `select ${projectionText} from ${fromText}`;
        if (filterText) {
            sql += ` where ${filterText}`;
        }
        if (sorterText) {
            sql += ` order by ${sorterText}`;
        }
        if (groupByText) {
            sql += ` group by ${groupByText}`;
        }
        if (typeof indexFrom === 'number') {
            (0, assert_1.default)(typeof count === 'number');
            sql += ` limit ${indexFrom}, ${count}`;
        }
        if (option?.forUpdate) {
            sql += ' for update';
        }
        return sql;
    }
    populateUpdateStmt(updateText, fromText, aliasDict, filterText, sorterText, indexFrom, count, option) {
        // todo using index
        (0, assert_1.default)(updateText);
        let sql = `update ${fromText} set ${updateText}`;
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
    populateRemoveStmt(updateText, fromText, aliasDict, filterText, sorterText, indexFrom, count, option) {
        // todo using index
        const alias = aliasDict['./'];
        if (option?.deletePhysically) {
            (0, assert_1.default)(!updateText, 'physically delete does not support setting trigger data');
            let sql = `delete ${alias} from ${fromText} `;
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
        const now = Date.now();
        const updateText2 = updateText ? `${updateText}, \`${alias}\`.\`$$deleteAt$$\` = '${now}'` : `\`${alias}\`.\`$$deleteAt$$\` = '${now}'`;
        let sql = `update ${fromText} set ${updateText2}`;
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
