import assert from 'assert';
import { assign, cloneDeep, identity, intersection, keys, set } from 'lodash';
import { Attribute, EntityDict, EXPRESSION_PREFIX, Index, OperateOption, 
    Q_FullTextValue, Ref, RefOrExpression, SelectOption, StorageSchema, SubQueryPredicateMetadata } from "oak-domain/lib/types";
import { EntityDict as BaseEntityDict } from 'oak-domain/lib/base-app-domain';
import { DataType } from "oak-domain/lib/types/schema/DataTypes";
import { judgeRelation } from 'oak-domain/lib/store/relation';
import { combineFilters } from 'oak-domain/lib/store/filter';

export interface SqlSelectOption extends SelectOption {
};
export interface SqlOperateOption extends OperateOption {
};

export abstract class SqlTranslator<ED extends EntityDict & BaseEntityDict> {
    readonly schema: StorageSchema<ED>;
    constructor(schema: StorageSchema<ED>) {
        this.schema = this.makeFullSchema(schema);
    }

    private makeFullSchema(schema2: StorageSchema<ED>) {
        const schema = cloneDeep(schema2);
        for (const entity in schema) {
            const { attributes, indexes } = schema[entity];
            // 增加默认的属性
            assign(attributes, {
                id: {
                    type: 'char',
                    params: {
                        length: 36,
                    },
                } as Attribute,
                $$seq$$: {
                    type: 'sequence',
                    sequenceStart: 10000,
                } as Attribute,
                $$createAt$$: {
                    type: 'datetime',
                    notNull: true,
                } as Attribute,
                $$updateAt$$: {
                    type: 'datetime',
                    notNull: true,
                } as Attribute,
                $$deleteAt$$: {
                    type: 'datetime',
                } as Attribute,
                $$triggerData$$: {
                    type: 'object',
                } as Attribute,
                $$triggerTimestamp$$: {
                    type: 'datetime',
                } as Attribute,
            });

            // 增加默认的索引
            const intrinsticIndexes: Index<ED[keyof ED]['OpSchema']>[] = [
                {
                    name: `${entity}_create_at_auto_create`,
                    attributes: [{
                        name: '$$createAt$$',
                    }, {
                        name: '$$deleteAt$$',
                    }]
                }, {
                    name: `${entity}_update_at_auto_create`,
                    attributes: [{
                        name: '$$updateAt$$',
                    }, {
                        name: '$$deleteAt$$',
                    }],
                }, {
                    name: `${entity}_trigger_ts_auto_create`,
                    attributes: [{
                        name: '$$triggerTimestamp$$',
                    }, {
                        name: '$$deleteAt$$',
                    }],
                }
            ];

            // 增加外键等相关属性上的索引
            for (const attr in attributes) {
                if (attributes[attr].type === 'ref') {
                    if (!(indexes?.find(
                        ele => ele.attributes[0].name === attr
                    ))) {
                        intrinsticIndexes.push({
                            name: `${entity}_fk_${attr}_auto_create`,
                            attributes: [{
                                name: attr,
                            }, {
                                name: '$$deleteAt$$',
                            }]
                        });
                    }
                }

                if (attr === 'entity' && attributes[attr].type === 'varchar') {
                    const entityIdDef = attributes.entityId;
                    if (entityIdDef?.type === 'varchar') {
                        if (!(indexes?.find(
                            ele => ele.attributes[0].name === 'entity' && ele.attributes[1]?.name === 'entityId'
                        ))) {
                            intrinsticIndexes.push({
                                name: `${entity}_fk_entity_entityId_auto_create`,
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
                    if (!(indexes?.find(
                        ele => ele.attributes[0].name === attr
                    ))) {
                        intrinsticIndexes.push({
                            name: `${entity}_${attr}_auto_create`,
                            attributes: [{
                                name: attr,
                            }, {
                                name: '$$deleteAt$$',
                            }]
                        });
                    }
                }

                if (attr === 'expired' && attributes[attr].type === 'boolean') {
                    const expiresAtDef = attributes.expiresAt;
                    if (expiresAtDef?.type === 'datetime') {
                        if (!(indexes?.find(
                            ele => ele.attributes[0].name === 'expired' && ele.attributes[1]?.name === 'expiresAt'
                        ))) {
                            intrinsticIndexes.push({
                                name: `${entity}_expires_expiredAt_auto_create`,
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
            }

            if (indexes) {
                indexes.push(...intrinsticIndexes);
            }
            else {
                assign(schema[entity], {
                    indexes: intrinsticIndexes,
                });
            }
        }

        return schema;
    }

    protected abstract getDefaultSelectFilter<OP extends SqlSelectOption>(alias: string, option?: OP): string;

    protected abstract translateAttrProjection(dataType: DataType, alias: string, attr: string): string;

    protected abstract translateObjectProjection(projection: Record<string, any>, alias: string, attr: string, prefix: string): string;

    protected abstract translateAttrValue(dataType: DataType | Ref, value: any): string;

    protected abstract translateFullTextSearch<T extends keyof ED>(value: Q_FullTextValue, entity: T, alias: string): string;

    abstract translateCreateEntity<T extends keyof ED>(entity: T, option: { replace?: boolean }): string[];

    protected abstract translateObjectPredicate(predicate: Record<string, any>, alias: string, attr: string): string;

    protected abstract populateSelectStmt<T extends keyof ED, OP extends SqlSelectOption>(
        projectionText: string,
        fromText: string,
        aliasDict: Record<string, string>,
        filterText: string,
        sorterText?: string,
        groupByText?: string,
        indexFrom?: number,
        count?: number,
        option?: OP,
        selection?: ED[T]['Selection'],
        aggregation?: ED[T]['Aggregation']): string;

    protected abstract populateUpdateStmt<OP extends SqlOperateOption>(
        updateText: string,
        fromText: string,
        aliasDict: Record<string, string>,
        filterText: string,
        sorterText?: string,
        indexFrom?: number,
        count?: number,
        option?: OP): string;

    protected abstract populateRemoveStmt<OP extends SqlOperateOption>(
        removeText: string,
        fromText: string,
        aliasDict: Record<string, string>,
        filterText: string,
        sorterText?: string,
        indexFrom?: number,
        count?: number,
        option?: OP): string;

    protected abstract translateExpression<T extends keyof ED>(
        entity: T,
        alias: string,
        expression: RefOrExpression<keyof ED[T]['OpSchema']>,
        refDict: Record<string, [string, keyof ED]>): string;

    private getStorageName<T extends keyof ED>(entity: T) {
        const { storageName } = this.schema[entity];
        return (storageName || entity) as string;
    }

    translateInsert<T extends keyof ED>(entity: T, data: ED[T]['CreateMulti']['data']): string {
        const { schema } = this;
        const { attributes, storageName = entity } = schema[entity];

        let sql = `insert into \`${storageName as string}\`(`;

        /**
         * 这里的attrs要用所有行的union集合
         */
        const dataFull = data.reduce((prev, cur) => Object.assign({}, cur, prev), {});
        const attrs = Object.keys(dataFull).filter(
            ele => attributes.hasOwnProperty(ele)
        );
        attrs.forEach(
            (attr, idx) => {
                sql += ` \`${attr}\``;
                if (idx < attrs.length - 1) {
                    sql += ',';
                }
            }
        );

        sql += ') values ';

        data.forEach(
            (d, dataIndex) => {
                sql += '(';
                attrs.forEach(
                    (attr, attrIdx) => {
                        const attrDef = attributes[attr];
                        const { type: dataType } = attrDef;
                        const value = this.translateAttrValue(dataType as DataType, d[attr]);
                        sql += value;
                        if (attrIdx < attrs.length - 1) {
                            sql += ',';
                        }
                    }
                );
                if (dataIndex < data.length - 1) {
                    sql += '),';
                }
                else {
                    sql += ')'
                }
            }
        );

        return sql;
    }

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
    private analyzeJoin<T extends keyof ED>(entity: T, { projection, filter, sorter, aggregation }: {
        projection?: ED[T]['Selection']['data'];
        aggregation?: ED[T]['Aggregation']['data'];
        filter?: ED[T]['Selection']['filter'];
        sorter?: ED[T]['Selection']['sorter'];
    }, initialNumber?: number): {
        aliasDict: Record<string, string>;
        projectionRefAlias: Record<string, [string, keyof ED]>;
        filterRefAlias: Record<string, [string, keyof ED]>;
        from: string;
        currentNumber: number;
    } {
        const { schema } = this;
        let number = initialNumber || 1;
        const projectionRefAlias: Record<string, [string, keyof ED]> = {};
        const filterRefAlias: Record<string, [string, keyof ED]> = {};

        const alias = `${entity as string}_${number++}`;
        let from = ` \`${this.getStorageName(entity)}\` \`${alias}\` `;
        const aliasDict: Record<string, string> = {
            './': alias,
        };

        const analyzeFilterNode = <E extends keyof ED>({ node, path, entityName, alias }: {
            node: ED[E]['Selection']['filter'];
            path: string;
            entityName: E;
            alias: string,
        }): void => {
            Object.keys(node!).forEach(
                (op) => {
                    if (['$and', '$or'].includes(op)) {
                        (node![op] as ED[E]['Selection']['filter'][]).forEach(
                            (subNode) => analyzeFilterNode({
                                node: subNode,
                                path,
                                entityName,
                                alias,
                            })
                        );
                    }
                    else if (['$not'].includes(op)) {
                        analyzeFilterNode({
                            node: node![op],
                            path,
                            entityName,
                            alias,
                        })
                    }
                    else if (['$text'].includes(op)) {

                    }
                    else {
                        const rel = judgeRelation(this.schema, entityName, op);
                        if (typeof rel === 'string') {
                            let alias2: string;
                            const pathAttr = `${path}${op}/`;
                            if (!aliasDict.hasOwnProperty(pathAttr)) {
                                alias2 = `${rel}_${number++}`;
                                assign(aliasDict, {
                                    [pathAttr]: alias2,
                                });
                                from += ` left join \`${this.getStorageName(rel)}\` \`${alias2}\` on \`${alias}\`.\`${op}Id\` = \`${alias2}\`.\`id\``;
                            }
                            else {
                                alias2 = aliasDict[pathAttr];
                            }
                            analyzeFilterNode({
                                node: node![op],
                                path: pathAttr,
                                entityName: rel,
                                alias: alias2,
                            });
                        }
                        else if (rel === 2) {
                            let alias2: string;
                            const pathAttr = `${path}${op}/`;
                            if (!aliasDict.hasOwnProperty(pathAttr)) {
                                alias2 = `${op}_${number++}`;
                                assign(aliasDict, {
                                    [pathAttr]: alias2,
                                });
                                from += ` left join \`${this.getStorageName(op)}\` \`${alias2}\` on \`${alias}\`.\`entityId\` = \`${alias2}\`.\`id\` and \`${alias}\`.\`entity\` = '${op}'`;
                            }
                            else {
                                alias2 = aliasDict[pathAttr];
                            }
                            analyzeFilterNode({
                                node: node![op],
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
                }
            );
            if (node!['#id']) {
                assert(!filterRefAlias[node!['#id']]);
                assign(filterRefAlias, {
                    [node!['#id']]: [alias, entityName],
                });
            }
        };
        if (filter) {
            analyzeFilterNode({
                node: filter,
                path: './',
                entityName: entity,
                alias,
            });
        }

        const analyzeSortNode = <E extends keyof ED>({ node, path, entityName, alias }: {
            node: NonNullable<ED[E]['Selection']['sorter']>[number]['$attr'];
            path: string;
            entityName: E;
            alias: string;
        }): void => {
            const attr = keys(node)[0];

            const rel = judgeRelation(this.schema, entityName, attr);
            if (typeof rel === 'string') {
                const pathAttr = `${path}${attr}/`;
                let alias2: string;
                if (!aliasDict.hasOwnProperty(pathAttr)) {
                    alias2 = `${rel}_${number++}`;
                    assign(aliasDict, {
                        [pathAttr]: alias2,
                    });
                    from += ` left join \`${this.getStorageName(rel)}\` \`${alias2}\` on \`${alias}\`.\`${attr}Id\` = \`${alias2}\`.\`id\``;
                }
                else {
                    alias2 = aliasDict[pathAttr];
                }
                analyzeSortNode({
                    node: node[attr] as any,
                    path: pathAttr,
                    entityName: rel,
                    alias: alias2,
                });
            }
            else if (rel === 2) {
                const pathAttr = `${path}${attr}/`;
                let alias2: string;
                if (!aliasDict.hasOwnProperty(pathAttr)) {
                    alias2 = `${attr}_${number++}`;
                    assign(aliasDict, {
                        [pathAttr]: alias2,
                    });
                    from += ` left join \`${this.getStorageName(attr)}\` \`${alias2}\` on \`${alias}\`.\`entityId\` = \`${alias2}\`.\`id\` and \`${alias}\`.\`entity\` = '${attr}'`;
                }
                else {
                    alias2 = aliasDict[pathAttr];
                }
                analyzeSortNode({
                    node: node[attr] as any,
                    path: pathAttr,
                    entityName: attr,
                    alias: alias2,
                });
            }
            else {
                assert(rel === 0 || rel === 1);
            }
        };
        if (sorter) {
            sorter.forEach(
                (sortNode) => {
                    analyzeSortNode({
                        node: sortNode.$attr,
                        path: './',
                        entityName: entity,
                        alias,
                    });
                }
            );
        }

        const analyzeProjectionNode = <E extends keyof ED>({ node, path, entityName, alias }: {
            node: ED[E]['Selection']['data'];
            path: string;
            entityName: E;
            alias: string;
        }): void => {
            const { attributes } = schema[entityName];

            Object.keys(node).forEach(
                (attr) => {
                    const rel = judgeRelation(this.schema, entityName, attr);
                    if (typeof rel === 'string') {
                        const pathAttr = `${path}${attr}/`;

                        let alias2: string;
                        if (!aliasDict.hasOwnProperty(pathAttr)) {
                            alias2 = `${rel}_${number++}`;
                            assign(aliasDict, {
                                [pathAttr]: alias2,
                            });
                            from += ` left join \`${this.getStorageName(rel)}\` \`${alias2}\` on \`${alias}\`.\`${attr}Id\` = \`${alias2}\`.\`id\``;
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
                        const pathAttr = `${path}${attr}/`;

                        let alias2: string;
                        if (!aliasDict.hasOwnProperty(pathAttr)) {
                            alias2 = `${attr}_${number++}`;
                            assign(aliasDict, {
                                [pathAttr]: alias2,
                            });
                            from += ` left join \`${this.getStorageName(attr)}\` \`${alias2}\` on \`${alias}\`.\`entityId\` = \`${alias2}\`.\`id\` and \`${alias}\`.\`entity\` = '${attr}'`;
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
                }
            );
            if (node['#id']) {
                assert(!projectionRefAlias[node['#id']], `projection上有重复的#id定义「${node['#id']}」`);
                assign(projectionRefAlias, {
                    [node['#id']]: [alias, entityName],
                });
            }
        };

        if (projection) {
            analyzeProjectionNode({ node: projection, path: './', entityName: entity, alias });
        }
        else if (aggregation) {
            for (const k in aggregation) {
                analyzeProjectionNode<T>({
                    node: aggregation[k],
                    path: './',
                    entityName: entity,
                    alias,
                });
            }
        }

        return {
            aliasDict,
            from,
            projectionRefAlias,
            filterRefAlias,
            currentNumber: number,
        };
    }

    private translateComparison(attr: string, value: any, type?: DataType | Ref): string {
        const SQL_OP: {
            [op: string]: string,
        } = {
            $gt: '>',
            $lt: '<',
            $gte: '>=',
            $lte: '<=',
            $eq: '=',
            $ne: '<>',
        };

        if (Object.keys(SQL_OP).includes(attr)) {
            if (type) {
                return ` ${SQL_OP[attr]} ${this.translateAttrValue(type, value)}`;
            }
            else {
                return ` ${SQL_OP[attr]} ${value}`;
            }
        }

        switch (attr) {
            case '$startsWith': {
                return ` like '${value}%'`;
            }
            case '$endsWith': {
                return ` like '%${value}'`;
            }
            case '$includes': {
                return ` like '%${value}%'`;
            }
            default: {
                throw new Error(`unrecoganized comparison operator ${attr}`);
            }
        }
    }

    private translateEvaluation<T extends keyof ED>(attr: string, value: any, entity: T, alias: string, type: DataType | Ref, initialNumber: number, refAlias: Record<string, [string, keyof ED]>): {
        stmt: string;
        currentNumber: number;
    } {
        switch (attr) {
            case '$in':
            case '$nin': {
                const IN_OP = {
                    $in: 'in',
                    $nin: 'not in',
                };
                if (value instanceof Array) {
                    return {
                        stmt: this.translatePredicate(attr, value, type),
                        currentNumber: initialNumber,
                    }
                }
                else {
                    assert(false, '子查询已经改写为一对多的形式');
                    // sub query
                    /* const { stmt: subQueryStmt, currentNumber } = this.translateSelectInner(value.entity, value, initialNumber, refAlias, undefined);
                    return {
                        stmt: ` ${IN_OP[attr]}(${subQueryStmt})`,
                        currentNumber,
                    }; */
                }
            }
            default: {
                throw new Error(`${attr} is not evaluation predicate`);
            }
        }
    }

    protected translatePredicate(predicate: string, value: any, type?: DataType | Ref): string {
        if (['$gt', '$gte', '$lt', '$lte', '$eq', '$ne', '$startsWith', '$endsWith', '$includes'].includes(predicate)) {
            return this.translateComparison(predicate, value, type);
        }
        else if (['$in', '$nin'].includes(predicate)) {
            assert(value instanceof Array);
            const IN_OP = {
                $in: 'in',
                $nin: 'not in',
            };
            const values = value.map(
                (v: string | number) => {
                    if (type && ['varchar', 'char', 'text', 'nvarchar', 'ref', 'enum'].includes(type as string) || typeof v === 'string') {
                        return `'${v}'`;
                    }
                    else {
                        return `${v}`;
                    }
                }
            );
            if (values.length > 0) {
                return ` ${IN_OP[predicate as '$in']}(${values.join(',')})`;
            }
            if (predicate === '$in') {
                return ' in (null)';
            }
            return ' is not null';
        }
        else if (predicate === '$between') {
            const values = value.map(
                (v: string | number) => {
                    if (type && ['varchar', 'char', 'text', 'nvarchar', 'ref', 'enum'].includes(type as string) || typeof v === 'string') {
                        return `'${v}'`;
                    }
                    else {
                        return `${v}`;
                    }
                }
            );
            // between是所有数据库都支持的语法吗？
            return ` between ${values[0]} and ${values[1]}`;
        }
        else {
            assert(predicate === '$exists');
            if (value) {
                return ' is not null';
            }
            return ' is null';
        }
    }

    private translateFilter<T extends keyof ED, OP extends SqlSelectOption>(
        entity: T,
        filter: ED[T]['Selection']['filter'],
        aliasDict: Record<string, string>,
        filterRefAlias: Record<string, [string, keyof ED]>,
        initialNumber: number,
        option?: OP): {
            stmt: string;
            currentNumber: number;
        } {
        const { schema } = this;

        let currentNumber = initialNumber;
        const translateInner = <E extends keyof ED>(entity2: E, path: string, filter2?: ED[E]['Selection']['filter'], type?: DataType | Ref): string => {
            const alias = aliasDict[path];
            const { attributes } = schema[entity2];
            let whereText = type ? '' : this.getDefaultSelectFilter(alias, option);
            if (filter2) {
                const attrs = Object.keys(filter2).filter(
                    ele => !ele.startsWith('#')
                );
                attrs.forEach(
                    (attr) => {
                        if (whereText) {
                            whereText += ' and '
                        }
                        if (['$and', '$or', '$xor', '$not'].includes(attr)) {
                            whereText += '(';
                            switch (attr) {
                                case '$and':
                                case '$or':
                                case '$xor': {
                                    const logicQueries = filter2[attr];
                                    logicQueries.forEach(
                                        (logicQuery: ED[E]['Selection']['filter'], index: number) => {
                                            const sql = translateInner(entity2, path, logicQuery, 'ref'); // 只要传个值就行了，应该无所谓
                                            if (sql) {
                                                whereText += ` (${sql})`;
                                                if (index < logicQueries.length - 1) {
                                                    whereText += ` ${attr.slice(1)}`;
                                                }
                                            }
                                        }
                                    );
                                    break;
                                }
                                default: {
                                    assert(attr === '$not');
                                    const logicQuery = filter2[attr];
                                    const sql = translateInner(entity2, path, logicQuery, 'ref'); // 只要传个值就行了，应该无所谓
                                    if (sql) {
                                        whereText += ` not (${sql})`;
                                        break;
                                    }
                                }
                            }
                            whereText += ')';
                        }
                        else if (attr === '$text') {
                            whereText += `(${this.translateFullTextSearch(filter2[attr], entity, alias)})`;
                        }
                        else if (attr.toLowerCase().startsWith(EXPRESSION_PREFIX)) {
                            // expression
                            whereText += ` (${this.translateExpression(entity2, alias, filter2[attr], filterRefAlias)})`;
                        }
                        else {
                            const rel = judgeRelation(this.schema, entity2, attr);
                            if (rel === 2) {
                                whereText += ` (${translateInner(attr, `${path}${attr}/`, filter2[attr])})`;
                            }
                            else if (typeof rel === 'string') {
                                whereText += ` (${translateInner(rel, `${path}${attr}/`, filter2[attr])})`;
                            }
                            else if (rel instanceof Array) {
                                const [subEntity, foreignKey] = rel;
                                const predicate = (filter2[attr]['#sqp'] || 'in') as NonNullable<SubQueryPredicateMetadata['#sqp']>;
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
                                const refAlia = Object.keys(filterRefAlias).find(
                                    ele => filterRefAlias[ele][0] === alias
                                );

                                const refAlia2 = refAlia || alias;      // alias一定是唯一的，可以用来作为node id
                                if (!refAlia) {
                                    assert(!filterRefAlias[refAlia2]);
                                    Object.assign(filterRefAlias, {
                                        [refAlia2]: [alias, entity2],
                                    });
                                }
                                const fk = foreignKey || 'entityId';
                                const joinFilter = ['not in', 'in'].includes(predicate) ? {
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
                                const { stmt, currentNumber: ct2 } = this.translateSelectInner(subEntity, {
                                    data: {
                                        id: 1,
                                    },
                                    filter: combineFilters([joinFilter, filter2[attr]]),
                                    indexFrom: 0,
                                    count: 1,
                                }, currentNumber, filterRefAlias, option);
                                currentNumber = ct2;
                                
                                const PREDICATE_DICT = {
                                    'in': 'exists',
                                    'not in': 'not exists',
                                    'all': 'not exists',
                                    'not all': 'exists',
                                };
                                whereText += ` ${PREDICATE_DICT[predicate]} (${stmt})`;
                            }
                            else {
                                assert(attributes.hasOwnProperty(attr), `非法的属性${attr}`);
                                const { type: type2 } = attributes[attr];
                                // assert (type2 !== 'ref');
                                if (typeof filter2[attr] === 'object') {
                                    if (['object', 'array'].includes(type2)) {
                                        // 对object数据的深层次查询，这里调用数据库所支持的属性对象级查询，如mysql中的json查询
                                        whereText += `(${this.translateObjectPredicate(filter2[attr], alias, attr)})`;
                                    }
                                    else {
                                        assert(Object.keys(filter2[attr]).length === 1);
                                        const predicate = Object.keys(filter2[attr])[0];
                                        assert(predicate.startsWith('$'));
                                        // 对属性上的谓词处理
                                        whereText += ` (\`${alias}\`.\`${attr}\` ${this.translatePredicate(predicate, filter2[attr][predicate], type2)})`;      
                                    }
                                }
                                else {
                                    whereText += ` (\`${alias}\`.\`${attr}\` = ${this.translateAttrValue(type2, filter2[attr])})`;
                                }
                            }
                        }
                    }
                );
            }
            if (!whereText) {
                whereText = 'true';     // 如果为空就赋一个永真条件，以便处理and
            }
            return whereText;
        };

        const where = translateInner(entity, './', filter);
        
        return {
            stmt: where,
            currentNumber,
        };
    }

    private translateSorter<T extends keyof ED>(entity: T, sorter: ED[T]['Selection']['sorter'], aliasDict: Record<string, string>): string {
        const translateInner = <E extends keyof ED>(entity2: E, sortAttr: NonNullable<ED[E]['Selection']['sorter']>[number]['$attr'], path: string): string => {
            assert(Object.keys(sortAttr).length === 1);
            const attr = Object.keys(sortAttr)[0];
            const alias = aliasDict[path];

            if (attr.toLocaleLowerCase().startsWith(EXPRESSION_PREFIX)) {
                return this.translateExpression(entity2, alias, sortAttr[attr] as any, {});
            }
            else if (sortAttr[attr] === 1) {
                return `\`${alias}\`.\`${attr}\``;
            }
            else {
                const rel = judgeRelation(this.schema, entity2, attr);
                if (typeof rel === 'string') {
                    return translateInner(rel, sortAttr[attr] as any, `${path}${attr}/`);
                }
                else {
                    assert(rel === 2);
                    return translateInner(attr, sortAttr[attr] as any, `${path}${attr}/`);
                }
            }
        };

        let sortText = '';
        sorter!.forEach(
            (sortNode, index) => {
                const { $attr, $direction } = sortNode;
                sortText += translateInner(entity, $attr, './');
                if ($direction) {
                    sortText += ` ${$direction}`;
                }

                if (index < sorter!.length - 1) {
                    sortText += ',';
                }
            }
        );

        return sortText;
    }

    private translateProjection<T extends keyof ED>(
        entity: T,
        projection: ED[T]['Selection']['data'],
        aliasDict: Record<string, string>,
        projectionRefAlias: Record<string, [string, keyof ED]>,
        commonPrefix?: string,
        disableAs?: boolean): {
            projText: string,
            as: string,
        } {
        const { schema } = this;
        let as = '';
        const translateInner = <E extends keyof ED>(entity2: E, projection2: ED[E]['Selection']['data'], path: string): string => {
            const alias = aliasDict[path];
            const { attributes } = schema[entity2];
            let projText = '';

            let prefix = path.slice(2).replace(/\//g, '.');
            const attrs = Object.keys(projection2).filter(
                (attr) => {
                    if (attr.toLowerCase().startsWith(EXPRESSION_PREFIX)) {
                        return true;
                    }
                    const rel = judgeRelation(this.schema, entity2, attr);
                    return [1, 2].includes(rel as number) || typeof rel === 'string';
                }
            );
            attrs.forEach(
                (attr, idx) => {
                    const prefix2 = commonPrefix ? `${commonPrefix}.${prefix}` : prefix;
                    if (attr.toLowerCase().startsWith(EXPRESSION_PREFIX)) {
                        const exprText = this.translateExpression(entity2, alias, projection2[attr], projectionRefAlias);
                        if (disableAs) {
                            projText += ` ${exprText}`;
                        }
                        else {
                            projText += ` ${exprText} as \`${prefix2}${attr}\``;
                            if (!as) {
                                as = `\`${prefix2}${attr}\``;
                            }
                            else {
                                as += `, \`${prefix2}${attr}\``;
                            }
                        }
                    }
                    else {
                        const rel = judgeRelation(this.schema, entity2, attr);
                        if (typeof rel === 'string') {
                            projText += translateInner(rel, projection2[attr], `${path}${attr}/`);
                        }
                        else if (rel === 2) {
                            projText += translateInner(attr, projection2[attr], `${path}${attr}/`);
                        }
                        else if (rel === 1) {
                            const { type } = attributes[attr];
                            if (projection2[attr] === 1) {
                                if (disableAs) {
                                    projText += ` ${this.translateAttrProjection(type as DataType, alias, attr)}`;
                                }
                                else {
                                    projText += ` ${this.translateAttrProjection(type as DataType, alias, attr)} as \`${prefix2}${attr}\``;
                                    if (!as) {
                                        as = `\`${prefix2}${attr}\``;
                                    }
                                    else {
                                        as += `, \`${prefix2}${attr}\``;
                                    }
                                }
                            }
                            else if (typeof projection2[attr] === 'object') {
                                // 对JSON对象的取值
                                assert(!disableAs);
                                assert(['object', 'array'].includes(type));
                                projText += ` ${this.translateObjectProjection(projection2[attr], alias, attr, prefix2)}`;
                            }
                            else {
                                assert(typeof projection2 === 'string');
                                if (disableAs) {
                                    projText += ` ${this.translateAttrProjection(type as DataType, alias, attr)}`;
                                }
                                else {
                                    projText += ` ${this.translateAttrProjection(type as DataType, alias, attr)} as \`${prefix2}${projection2[attr]}\``;
                                    if (!as) {
                                        as = `\`${prefix2}${projection2[attr]}\``;
                                    }
                                    else {
                                        as += `\`${prefix2}${projection2[attr]}\``;
                                    }
                                }
                            }
                        }
                    }
                    if (idx < attrs.length - 1) {
                        projText += ',';
                    }
                }
            );

            return projText;
        };

        return {
            projText: translateInner(entity, projection, './'),
            as,
        };
    }

    private translateSelectInner<T extends keyof ED, OP extends SqlSelectOption>(entity: T, selection: ED[T]['Selection'], initialNumber: number, refAlias: Record<string, [string, keyof ED]>, option?: OP): {
        filterStmt: string;
        stmt: string;
        currentNumber: number;
    } {
        const { data, filter, sorter, indexFrom, count } = selection;
        const { from: fromText, aliasDict, projectionRefAlias, filterRefAlias, currentNumber } = this.analyzeJoin(entity, {
            projection: data,
            filter,
            sorter,
        }, initialNumber);
        assert(intersection(keys(refAlias), keys(filterRefAlias)).length === 0, 'filter中的#node结点定义有重复');
        assign(refAlias, filterRefAlias);

        const { projText } = this.translateProjection(entity, data, aliasDict, projectionRefAlias);

        const { stmt: filterText, currentNumber: currentNumber2 } = this.translateFilter(entity, filter, aliasDict, refAlias, currentNumber, option);

        const sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);

        return {
            stmt: this.populateSelectStmt(projText, fromText, aliasDict, filterText, sorterText, undefined, indexFrom, count, option, selection),
            currentNumber: currentNumber2,
            filterStmt: filterText,
        };
    }

    translateSelect<T extends keyof ED, OP extends SqlSelectOption>(entity: T, selection: ED[T]['Selection'], option?: OP): string {
        const { stmt } = this.translateSelectInner(entity, selection, 1, {}, option);
        return stmt;
    }

    translateWhere<T extends keyof ED, OP extends SqlSelectOption>(entity: T, selection: ED[T]['Selection'], option?: OP): string {
        const { filterStmt } = this.translateSelectInner(entity, selection, 1, {}, option);
        return filterStmt;
    }

    translateAggregate<T extends keyof ED, OP extends SqlSelectOption>(entity: T, aggregation: ED[T]['Aggregation'], option?: OP): string {
        const { data, filter, sorter, indexFrom, count } = aggregation;
        const { from: fromText, aliasDict, projectionRefAlias, filterRefAlias, currentNumber } = this.analyzeJoin(entity, {
            aggregation: data,
            filter,
            sorter,
        }, 1);

        let projText = '';
        let groupByText = '';
        for (const k in data) {
            if (k === '#aggr') {
                const { projText: projSubText, as } = this.translateProjection(entity, data[k]!, aliasDict, projectionRefAlias, '#data');
                if (!projText) {
                    projText = projSubText;
                }
                else {
                    projText += `, ${projSubText}`;
                }
                groupByText = as;
            }
            else {
                const { projText: projSubText } = this.translateProjection(entity, (data as any)[k]!, aliasDict, projectionRefAlias, undefined, true);
                let projSubText2 = '';
                if (k.startsWith('#max')) {
                    projSubText2 = `max(${projSubText}) as \`${k}\``;
                }
                else if (k.startsWith('#min')) {
                    projSubText2 = `min(${projSubText}) as \`${k}\``;
                }
                else if (k.startsWith('#count')) {
                    projSubText2 = `count(${projSubText}) as \`${k}\``;
                }
                else if (k.startsWith('#sum')) {
                    projSubText2 = `sum(${projSubText}) as \`${k}\``;
                }
                else {
                    assert(k.startsWith('#avg'));
                    projSubText2 = `avg(${projSubText}) as \`${k}\``;
                }
                if (!projText) {
                    projText = projSubText2;
                }
                else {
                    projText += `, ${projSubText2}`;
                }
            }
        }

        const { stmt: filterText } = this.translateFilter(entity, filter, aliasDict, {}, currentNumber, option);

        const sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);

        return this.populateSelectStmt(projText, fromText, aliasDict, filterText, sorterText, groupByText, indexFrom, count, option, undefined, aggregation);
    }

    translateCount<T extends keyof ED, OP extends SqlSelectOption>(entity: T, selection: Pick<ED[T]['Selection'], 'filter' | 'count'>, option?: OP): string {
        const { filter, count } = selection;
        const { from: fromText, aliasDict, filterRefAlias, currentNumber } = this.analyzeJoin(entity, {
            filter,
        });

        const projText = 'count(1) cnt';

        const { stmt: filterText } = this.translateFilter(entity, filter, aliasDict, filterRefAlias, currentNumber, option);

        if (count) {
            const subQuerySql = this.populateSelectStmt('1', fromText, aliasDict, filterText, undefined, undefined, undefined, undefined, option, Object.assign({}, selection, { indexFrom: 0, count }) as ED[T]['Selection']);
            return `select count(1) cnt from (${subQuerySql}) __tmp`;
        }
        return this.populateSelectStmt(projText, fromText, aliasDict, filterText, undefined, undefined, undefined, undefined, option, selection as ED[T]['Selection']);
    }

    translateRemove<T extends keyof ED, OP extends SqlOperateOption>(entity: T, operation: ED[T]['Remove'], option?: OP): string {
        const { filter, sorter, indexFrom, count } = operation;
        assert(!sorter, '当前remove不支持sorter行为');
        const { aliasDict, filterRefAlias, from: fromText, currentNumber } = this.analyzeJoin(entity, { filter, sorter });

        const alias = aliasDict['./'];

        const { stmt: filterText } = this.translateFilter(entity, filter, aliasDict, filterRefAlias, currentNumber, { includedDeleted: true });

        // const sorterText = sorter && sorter.length > 0 ? this.translateSorter(entity, sorter, aliasDict) : undefined;

        return this.populateRemoveStmt(alias, fromText, aliasDict, filterText, /* sorterText */ undefined, indexFrom, count, option);
    }

    translateUpdate<T extends keyof ED, OP extends SqlOperateOption>(entity: T, operation: ED[T]['Update'], option?: OP): string {
        const { attributes } = this.schema[entity];
        const { filter, sorter, indexFrom, count, data } = operation;
        assert(!sorter, '当前update不支持sorter行为');
        const { aliasDict, filterRefAlias, from: fromText, currentNumber } = this.analyzeJoin(entity, { filter, sorter });

        const alias = aliasDict['./'];

        let updateText = '';
        for (const attr in data) {
            if (updateText) {
                updateText += ',';
            }
            assert(attributes.hasOwnProperty(attr));
            const value = this.translateAttrValue(attributes[attr].type as DataType, data[attr]);
            updateText += `\`${alias}\`.\`${attr}\` = ${value}`;
        }

        const { stmt: filterText } = this.translateFilter(entity, filter, aliasDict, filterRefAlias, currentNumber);
        // const sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);

        return this.populateUpdateStmt(updateText, fromText, aliasDict, filterText, /* sorterText */ undefined, indexFrom, count, option);
    }

    translateDestroyEntity(entity: string, truncate?: boolean): string {
        const { schema } = this;
        const { storageName = entity, view } = schema[entity];

        let sql: string;
        if (view) {
            sql = `drop view if exists \`${storageName}\``;
        }
        else {
            sql = truncate ? `truncate table \`${storageName}\`` : `drop table if exists \`${storageName}\``;
        }

        return sql;
    }


    escapeStringValue(value: string): string {
        const result = `'${value.replace(/'/g, '\\\'').replace(/"/g, '\\\"')}'`;
        return result;
    }
}