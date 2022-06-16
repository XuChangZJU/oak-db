import assert from 'assert';
import { format } from 'util';
import { assign } from 'lodash';
import { DateTime } from 'luxon';
import { EntityDict, Geo, Q_FullTextValue, RefOrExpression, Ref, StorageSchema, Index, RefAttr } from "oak-domain/lib/types";
import { DataType, DataTypeParams } from "oak-domain/lib/types/schema/DataTypes";
import { SelectParams, SqlTranslator } from "../sqlTranslator";
import { isDateExpression } from 'oak-domain/lib/types/Expression';

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

function transformGeoData(data: Geo): string {
    if (data instanceof Array) {
        const element = data[0];
        if (element instanceof Array) {
            return ` GeometryCollection(${data.map(
                ele => transformGeoData(ele)
            ).join(',')})`
        }
        else {
            const geoType = GeoTypes.find(
                ele => ele.type === element.type
            );
            if (!geoType) {
                throw new Error(`${element.type} is not supported in MySQL`);
            }
            const multiGeoType = GeoTypes.find(
                ele => ele.element === geoType.type && ele.multiple
            );
            return ` ${multiGeoType!.name}(${data.map(
                ele => transformGeoData(ele)
            ).join(',')})`;
        }
    }
    else {
        const { type, coordinate } = data;
        const geoType = GeoTypes.find(
            ele => ele.type === type
        );
        if (!geoType) {
            throw new Error(`${data.type} is not supported in MySQL`);
        }
        const { element, name } = geoType;
        if (!element) {
            // Point
            return ` ${name}(${coordinate.join(',')})`;
        }
        // Polygon or Linestring
        return ` ${name}(${coordinate.map(
            (ele) => transformGeoData({
                type: element as any,
                coordinate: ele as any,
            })
        )})`;
    }
}

type IndexHint = {
    $force?: string;
    $ignore?: string;
} & {
    [k: string]: IndexHint;
}

export interface MySqlSelectParams extends SelectParams {
    indexHint?: IndexHint;
}

export class MySqlTranslator<ED extends EntityDict> extends SqlTranslator<ED> {
    private modifySchema() {
        for (const entity in this.schema) {
            const { attributes, indexes } = this.schema[entity];
            const geoIndexes: Index<ED[keyof ED]['OpSchema']>[] = [];
            for (const attr in attributes) {
                if (attributes[attr].type === 'geometry') {
                    const geoIndex = indexes?.find(
                        (idx) => idx.config?.type === 'spatial' && idx.attributes.find(
                            (attrDef) => attrDef.name === attr
                        )
                    );
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
                    assign(this.schema[entity], {
                        indexes: geoIndexes,
                    });
                }
            }
        }
    }

    constructor(schema: StorageSchema<ED>) {
        super(schema);
        // MySQL为geometry属性默认创建索引
        this.modifySchema();
    }
    static supportedDataTypes: DataType[] = [
        // numeric types
        "bit",
        "int",
        "integer",          // synonym for int
        "tinyint",
        "smallint",
        "mediumint",
        "bigint",
        "float",
        "double",
        "double precision", // synonym for double
        "real",             // synonym for double
        "decimal",
        "dec",              // synonym for decimal
        "numeric",          // synonym for decimal
        "fixed",            // synonym for decimal
        "bool",             // synonym for tinyint
        "boolean",          // synonym for tinyint
        // date and time types
        "date",
        "datetime",
        "timestamp",
        "time",
        "year",
        // string types
        "char",
        "nchar",            // synonym for national char
        "national char",
        "varchar",
        "nvarchar",         // synonym for national varchar
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

    static spatialTypes: DataType[] = [
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection"
    ];

    static withLengthDataTypes: DataType[] = [
        "char",
        "varchar",
        "nvarchar",
        "binary",
        "varbinary"
    ];

    static withPrecisionDataTypes: DataType[] = [
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

    static withScaleDataTypes: DataType[] = [
        "decimal",
        "dec",
        "numeric",
        "fixed",
        "float",
        "double",
        "double precision",
        "real"
    ];

    static unsignedAndZerofillTypes: DataType[] = [
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

    static withWidthDataTypes: DataType[] = [
        'int',
    ]

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
    private populateDataTypeDef(type: DataType | Ref, params?: DataTypeParams): string{
        if (MySqlTranslator.withLengthDataTypes.includes(type as DataType)) {
            if (params) {
                const { length } = params;
                return `${type}(${length}) `;
            }
            else {
                const { length } = (MySqlTranslator.dataTypeDefaults as any)[type];
                return `${type}(${length}) `;
            }
        }

        if (MySqlTranslator.withPrecisionDataTypes.includes(type as DataType)) {
            if (params) {
                const { precision, scale } = params;
                if (typeof scale === 'number') {
                    return `${type}(${precision}, ${scale}) `;
                }
                return `${type}(${precision})`;
            }
            else {
                const { precision, scale } = (MySqlTranslator.dataTypeDefaults as any)[type];
                if (typeof scale === 'number') {
                    return `${type}(${precision}, ${scale}) `;
                }
                return `${type}(${precision})`;
            }
        }

        if (MySqlTranslator.withWidthDataTypes.includes(type as DataType)) {
            assert(type === 'int');
            const { width } = params!;
            switch(width!) {
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

    protected translateAttrProjection(dataType: DataType, alias: string, attr: string): string {
        switch(dataType) {
            case 'geometry': {
                return ` st_astext(\`${alias}\`.\`${attr}\`)`;
            }
            default:{
                return ` \`${alias}\`.\`${attr}\``;
            }            
        }
    }

    protected translateAttrValue(dataType: DataType, value: any): string {
        if (value === null) {
            return 'null';
        }
        switch (dataType) {
            case 'geometry': {
                return transformGeoData(value);
            }
            case 'date': {
                if (value instanceof Date) {
                    return DateTime.fromJSDate(value).toFormat('yyyy-LL-dd HH:mm:ss');
                }
                else if (typeof value === 'number') {
                    return DateTime.fromMillis(value).toFormat('yyyy-LL-dd HH:mm:ss');
                }
                return value as string;
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
                return value as string;
            }
        }
    }
    protected translateFullTextSearch<T extends keyof ED>(value: Q_FullTextValue, entity: T, alias: string): string {
        const { $search } = value;
        const { indexes } = this.schema[entity];

        const ftIndex = indexes && indexes.find(
            (ele) => {
                const { config } = ele;
                return config && config.type === 'fulltext';
            }
        );
        assert(ftIndex);
        const { attributes } = ftIndex;
        const columns2 = attributes.map(
            ({ name }) => `${alias}.${name as string}`
        );
        return ` match(${columns2.join(',')}) against ('${$search}' in natural language mode)`;
    }
    translateCreateEntity<T extends keyof ED>(entity: T, options?: { replace?: boolean; }): string[] {
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
            sql += `\`${entity as string}\` `;
        }

        if (view) {
            throw new Error(' view unsupported yet');
        }
        else {
            sql += '(';
            // 翻译所有的属性
            Object.keys(attributes).forEach(
                (attr, idx) => {
                    const attrDef = attributes[attr];
                    const {
                        type,
                        params,
                        default: defaultValue,
                        unique,
                        notNull,
                    } = attrDef;
                    sql += `\`${attr}\` `
                    sql += this.populateDataTypeDef(type, params) as string;

                    if (notNull || type === 'geometry') {
                        sql += ' not null ';
                    }
                    if (unique) {
                        sql += ' unique ';
                    }
                    if (defaultValue !== undefined) {
                        assert(type !== 'ref');
                        sql += ` default ${this.translateAttrValue(type, defaultValue)}`;
                    }
                    if (attr === 'id') {
                        sql += ' primary key'
                    }
                    if (idx < Object.keys(attributes).length - 1) {
                        sql += ',\n';
                    }
                }
            );

            // 翻译索引信息
            if (indexes) {
                sql += ',\n';
                indexes.forEach(
                    ({ name, attributes, config }, idx) => {
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
                        attributes.forEach(
                            ({ name, size, direction }, idx2) => {
                                sql += `\`${name as string}\``;
                                if (size) {
                                    sql += ` (${size})`;
                                }
                                if (direction) {
                                    sql += ` ${direction}`;
                                }
                                if (idx2 < attributes.length - 1) {
                                    sql += ','
                                }
                                if (name === '$$deleteAt$$') {
                                    includeDeleteAt = true;
                                }
                            }
                        );
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
                    }
                );
            }
        }
        
        
        sql += ')';
        
        if (!replace) {
            return [sql];
        }
        return [`drop ${entityType} \`${storageName || entity as string}\`;`, sql];
    }

    private translateFnName(fnName: string, argumentNumber: number): string {
        switch(fnName) {
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
                for (let iter = 0; iter < argumentNumber; iter ++) {
                    result += '%s';
                    if (iter < argumentNumber - 1) {
                        result += ' and ';
                    }
                }
                return result;
            }
            case '$or': {
                let result = '';
                for (let iter = 0; iter < argumentNumber; iter ++) {
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

    protected translateExpression<T extends keyof ED>(alias: string, expression: RefOrExpression<keyof ED[T]["OpSchema"]>, refDict: Record<string, string>): string {
        const translateConstant = (constant: number | string | Date): string => {
            if (typeof constant === 'string') {
                return `'${constant}'`;
            }
            else if (constant instanceof Date) {
                return `'${DateTime.fromJSDate(constant).toFormat('yyyy-LL-dd HH:mm:ss')}'`;
            }
            else {
                assert(typeof constant === 'number');
                return `${constant}`;
            }
        };
        const translateInner = (expr: any): string => {
            const k = Object.keys(expr);
            let result: string;
            if (k.includes('#attr')) {
                const attrText = `\`${alias}\`.\`${(expr)['#attr']}\``;
                result = attrText;
            }
            else if (k.includes('#refId')) {
                const refId = (expr)['#refId'];
                const refAttr = (expr)['#refAttr'];
                
                assert(refDict[refId]);
                const attrText = `\`${refDict[refId]}\`.\`${refAttr}\``;
                result = attrText;
            }
            else {
                assert (k.length === 1);
                if ((expr)[k[0]] instanceof Array) {
                    const fnName = this.translateFnName(k[0], (expr)[k[0]].length);
                    const args = [fnName];
                    args.push(...(expr)[k[0]].map(
                        (ele: any) => {
                            if (['string', 'number'].includes(typeof ele) || ele instanceof Date) {
                                return translateConstant(ele);
                            }
                            else {
                                return translateInner(ele);
                            }
                        }
                    ));

                    result = format.apply(null, args);
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

                    result = format.apply(null, args);
                }
            }
            return result;
        };

        return translateInner(expression);
    }    

    protected populateSelectStmt(projectionText: string, fromText: string, aliasDict: Record<string, string>, filterText: string, sorterText?: string, indexFrom?: number, count?: number, params?: MySqlSelectParams): string {
        // todo using index
        let sql = `select ${projectionText} from ${fromText}`;
        if (filterText) {
            sql += ` where ${filterText}`;
        }
        if (sorterText) {
            sql += ` order by ${sorterText}`;
        }
        if (typeof indexFrom === 'number') {
            assert (typeof count === 'number');
            sql += ` limit ${indexFrom}, ${count}`;
        }
        if (params?.forUpdate) {
            sql += ' for update';
        }
        sql += ';';

        return sql;
    }
    protected populateUpdateStmt(updateText: string, fromText: string, aliasDict: Record<string, string>, filterText: string, sorterText?: string, indexFrom?: number, count?: number, params?: MySqlSelectParams): string {
        // todo using index
        const alias = aliasDict['./'];
        const now = DateTime.now().toFormat('yyyy-LL-dd HH:mm:ss');
        let sql = `update ${fromText} set ${updateText}, \`${alias}\`.\`$$updateAt$$\` = '${now}'`;
        if (filterText) {
            sql += ` where ${filterText}`;
        }
        if (sorterText) {
            sql += ` order by ${sorterText}`;
        }
        if (typeof indexFrom === 'number') {
            assert (typeof count === 'number');
            sql += ` limit ${indexFrom}, ${count}`;
        }
        sql += ';';

        return sql;
    }
    protected populateRemoveStmt(removeText: string, fromText: string, aliasDict: Record<string, string>, filterText: string, sorterText?: string, indexFrom?: number, count?: number, params?: MySqlSelectParams): string {
        // todo using index
        const alias = aliasDict['./'];
        const now = DateTime.now().toFormat('yyyy-LL-dd HH:mm:ss');
        let sql = `update ${fromText} set \`${alias}\`.\`$$deleteAt$$\` = '${now}'`;
        if (filterText) {
            sql += ` where ${filterText}`;
        }
        if (sorterText) {
            sql += ` order by ${sorterText}`;
        }
        if (typeof indexFrom === 'number') {
            assert (typeof count === 'number');
            sql += ` limit ${indexFrom}, ${count}`;
        }
        sql += ';';

        return sql;
    }
}