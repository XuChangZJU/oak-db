import assert from 'assert';
import { format } from 'util';
import { assign } from 'lodash';
import { EntityDict, Geo, Q_FullTextValue, RefOrExpression, Ref, StorageSchema, Index, RefAttr } from "oak-domain/lib/types";
import { DataType, DataTypeParams } from "oak-domain/lib/types/schema/DataTypes";
import { SqlOperateOption, SqlSelectOption, SqlTranslator } from "../sqlTranslator";
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

export interface MySqlSelectOption extends SqlSelectOption {
}

export interface MysqlOperateOption extends SqlOperateOption {

}

export class MySqlTranslator<ED extends EntityDict> extends SqlTranslator<ED> {
    protected getDefaultSelectFilter(alias: string, option?: MySqlSelectOption): string {
        if (option?.includedDeleted) {
            return '';
        }
        return ` (\`${alias}\`.\`$$deleteAt$$\` is null)`;
    }
    private makeUpSchema() {
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
        this.makeUpSchema();
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

    protected translateAttrValue(dataType: DataType | Ref, value: any): string {
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

        let hasSequence: boolean | number = false;
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
                        sequenceStart,
                    } = attrDef;
                    sql += `\`${attr}\` `
                    sql += this.populateDataTypeDef(type, params) as string;

                    if (notNull || type === 'geometry') {
                        sql += ' not null ';
                    }
                    if (unique) {
                        sql += ' unique ';
                    }
                    if (sequenceStart) {
                        if (hasSequence) {
                            throw new Error(`「${entity as string}」只能有一个sequence列`);
                        }
                        hasSequence = sequenceStart;
                        sql += ' auto_increment unique ';                        
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
        if (typeof hasSequence === 'number') {
            sql += `auto_increment = ${hasSequence}`;
        }
        
        if (!replace) {
            return [sql];
        }
        return [`drop ${entityType}  if exists \`${storageName || entity as string}\`;`, sql];
    }

    private translateFnName(fnName: string, argumentNumber: number): string {
        switch(fnName) {
            case '$add': {
                let result = '%s';
                while (--argumentNumber > 0) {
                    result += ' + %s';
                }
                return result;
            }
            case '$subtract': {
                assert(argumentNumber === 2);
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
                assert(argumentNumber === 2);
                return '%s / %s';
            }
            case '$abs': {
                return 'ABS(%s)';
            }
            case '$round': {
                assert(argumentNumber === 2);
                return 'ROUND(%s, %s)';
            }
            case '$ceil': {
                return 'CEIL(%s)';
            }
            case '$floor': {
                return 'FLOOR(%s)';
            }
            case '$pow': {
                assert(argumentNumber === 2);
                return 'POW(%s, %s)';
            }
            case '$gt': {
                assert(argumentNumber === 2);
                return '%s > %s';
            }
            case '$gte': {
                assert(argumentNumber === 2);
                return '%s >= %s';
            }
            case '$lt': {
                assert(argumentNumber === 2);
                return '%s < %s';
            }
            case '$lte': {
                return '%s <= %s';
            }
            case '$eq': {
                assert(argumentNumber === 2);
                return '%s = %s';
            }
            case '$ne': {
                assert(argumentNumber === 2);
                return '%s <> %s';
            }
            case '$startsWith': {
                assert(argumentNumber === 2);
                return '%s like CONCAT(%s, \'%\')';
            }
            case '$endsWith': {
                assert(argumentNumber === 2);
                return '%s like CONCAT(\'%\', %s)';
            }
            case '$includes': {
                assert(argumentNumber === 2);
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
                assert(argumentNumber === 3);
                return 'DATEDIFF(%s, %s, %s)';
            }
            case '$contains': {
                assert(argumentNumber === 2);
                return 'ST_CONTAINS(%s, %s)';
            }
            case '$distance': {
                assert(argumentNumber === 2);
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

    private translateAttrInExpression<T extends keyof ED>(entity: T, attr: string, exprText: string) {
        const { attributes } = this.schema[entity];
        const { type } = attributes[attr];
        if (['date', 'time', 'datetime'].includes(type)) {
            // 从unix时间戵转成date类型参加expr的运算
            return `from_unixtime(${exprText} / 1000)`;
        }
        return exprText
    }

    protected translateExpression<T extends keyof ED>(entity: T, alias: string, expression: RefOrExpression<keyof ED[T]["OpSchema"]>, refDict: Record<string, [string, keyof ED]>): string {
        const translateConstant = (constant: number | string | Date): string => {
            if (constant instanceof Date) {
                return ` from_unixtime(${constant.valueOf()}/1000)`;
            }
            else if (typeof constant === 'string') {
                return ` '${constant}'`;
            }
            else {
                assert(typeof constant === 'number');
                return ` ${constant}`;
            }
        };
        const translateInner = (expr: any): string => {
            const k = Object.keys(expr);
            let result: string;
            if (k.includes('#attr')) {
                const attrText = `\`${alias}\`.\`${(expr)['#attr']}\``;
                result = this.translateAttrInExpression(entity, (expr)['#attr'], attrText);
            }
            else if (k.includes('#refId')) {
                const refId = (expr)['#refId'];
                const refAttr = (expr)['#refAttr'];
                
                assert(refDict[refId]);
                const attrText = `\`${refDict[refId][0]}\`.\`${refAttr}\``;
                result = this.translateAttrInExpression(entity, (expr)['#refAttr'], attrText);
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

    protected populateSelectStmt<T extends keyof ED>(
        projectionText: string,
        fromText: string,
        aliasDict: Record<string, string>,
        filterText: string,
        sorterText?: string,
        groupByText?: string,
        indexFrom?: number,
        count?: number,
        option?: MySqlSelectOption): string {
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
            assert (typeof count === 'number');
            sql += ` limit ${indexFrom}, ${count}`;
        }
        if (option?.forUpdate) {
            sql += ' for update';
        }

        return sql;
    }
    protected populateUpdateStmt(updateText: string, fromText: string, aliasDict: Record<string, string>, filterText: string, sorterText?: string, indexFrom?: number, count?: number, option?: MysqlOperateOption): string {
        // todo using index
        assert(updateText);
        let sql = `update ${fromText} set ${updateText}`;
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

        return sql;
    }
    protected populateRemoveStmt(removeText: string, fromText: string, aliasDict: Record<string, string>, filterText: string, sorterText?: string, indexFrom?: number, count?: number, option?: MysqlOperateOption): string {
        // todo using index
        const alias = aliasDict['./'];
        const now = Date.now();
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

        return sql;
    }
}