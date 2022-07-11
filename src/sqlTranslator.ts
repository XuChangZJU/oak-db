import assert from 'assert';
import { assign, cloneDeep, intersection, keys, set } from 'lodash';
import { DateTime } from 'luxon';
import { Attribute, DeduceCreateOperationData, DeduceSorterAttr, DeduceSorterItem, EntityDict, Expression, EXPRESSION_PREFIX, Index, Q_FullTextValue, Ref, RefOrExpression, StorageSchema } from "oak-domain/lib/types";
import { DataType } from "oak-domain/lib/types/schema/DataTypes";
import { judgeRelation } from 'oak-domain/lib/store/relation';

export type SelectParams = {
};

export abstract class SqlTranslator<ED extends EntityDict> {
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
                $$createAt$$: {
                    type: 'date',
                    notNull: true,
                } as Attribute,
                $$updateAt$$: {
                    type: 'date',
                    notNull: true,
                } as Attribute,
                $$deleteAt$$: {
                    type: 'date',
                } as Attribute,
                $$triggerData$$: {
                    type: 'object',
                } as Attribute,
                $$triggerTimestamp$$: {
                    type: 'date',
                } as Attribute,
            });

            // 增加默认的索引
            const intrinsticIndexes: Index<ED[keyof ED]['OpSchema']>[] = [
                {
                    name: `${entity}_create_at`,
                    attributes: [{
                        name: '$$createAt$$',
                    }]
                }, {
                    name: `${entity}_update_at`,
                    attributes: [{
                        name: '$$updateAt$$',
                    }],
                }, {
                    name: `${entity}_trigger_ts`,
                    attributes: [{
                        name: '$$triggerTimestamp$$',
                    }],
                }
            ];

            // 增加外键上的索引
            for (const attr in attributes) {
                if (attributes[attr].type === 'ref') {
                    if (!(indexes?.find(
                        ele => ele.attributes[0].name === attr
                    ))) {
                        intrinsticIndexes.push({
                            name: `${entity}_fk_${attr}`,
                            attributes: [{
                                name: attr,
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
                                name: `${entity}_fk_entity_entityId`,
                                attributes: [{
                                    name: 'entity',
                                }, {
                                    name: 'entityId',
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

    protected abstract getDefaultSelectFilter<T extends keyof ED>(alias: string, hint: ED[T]['Selection']['hint']): string;

    protected abstract translateAttrProjection(dataType: DataType, alias: string, attr: string): string;

    protected abstract translateAttrValue(dataType: DataType | Ref, value: any): string;

    protected abstract translateFullTextSearch<T extends keyof ED>(value: Q_FullTextValue, entity: T, alias: string): string;

    abstract translateCreateEntity<T extends keyof ED>(entity: T, option: { replace?: boolean }): string[];

    protected abstract populateSelectStmt<T extends keyof ED>(
        projectionText: string,
        fromText: string,
        selection: ED[T]['Selection'],
        aliasDict: Record<string, string>,
        filterText: string,
        sorterText?: string,
        indexFrom?: number,
        count?: number): string;

    protected abstract populateUpdateStmt(
        updateText: string,
        fromText: string,
        aliasDict: Record<string, string>,
        filterText: string,
        sorterText?: string,
        indexFrom?: number,
        count?: number,
        params?: any): string;

    protected abstract populateRemoveStmt(
        removeText: string,
        fromText: string,
        aliasDict: Record<string, string>,
        filterText: string,
        sorterText?: string,
        indexFrom?: number,
        count?: number,
        params?: any): string;

    protected abstract translateExpression<T extends keyof ED>(alias: string, expression: RefOrExpression<keyof ED[T]['OpSchema']>, refDict: Record<string, string>): string;

    private getStorageName<T extends keyof ED>(entity: T) {
        const { storageName } = this.schema[entity];
        return (storageName || entity) as string;
    }

    translateInsert<T extends keyof ED>(entity: T, data: DeduceCreateOperationData<ED[T]['OpSchema']>[]): string {
        const { schema } = this;
        const { attributes, storageName = entity } = schema[entity];

        let sql = `insert into \`${storageName as string}\`(`;

        const attrs = Object.keys(data[0]).filter(
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

        sql += ', `$$createAt$$`, `$$updateAt$$`) values ';

        const now = DateTime.now().toFormat('yyyy-LL-dd HH:mm:ss');
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
                sql += `, '${now}', '${now}')`;
                if (dataIndex < data.length - 1) {
                    sql += ',';
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
    private analyzeJoin<T extends keyof ED>(entity: T, { projection, filter, sorter, isStat }: {
        projection?: ED[T]['Selection']['data'];
        filter?: ED[T]['Selection']['filter'];
        sorter?: ED[T]['Selection']['sorter'];
        isStat?: true;
    }, initialNumber?: number): {
        aliasDict: Record<string, string>;
        projectionRefAlias: Record<string, string>;
        filterRefAlias: Record<string, string>;
        from: string;
        extraWhere: string;
        currentNumber: number;
    } {
        const { schema } = this;
        let number = initialNumber || 1;
        const projectionRefAlias: Record<string, string> = {};
        const filterRefAlias: Record<string, string> = {};
        let extraWhere = '';

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
                                from += ` left join \`${this.getStorageName(op)}\` \`${alias2}\` on \`${alias}\`.\`entityId\` = \`${alias2}\`.\`id\``;
                                extraWhere += `\`${alias}\`.\`entity\` = '${op}'`;
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
                        else {
                            // 不支持一对多
                            assert(rel === 0 || rel === 1);
                        }
                    }
                }
            );
            if (node!['#id']) {
                assert(!filterRefAlias[node!['#id']]);
                assign(filterRefAlias, {
                    [node!['#id']]: alias,
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
            node: DeduceSorterAttr<ED[E]['Schema']>;
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
                    from += ` left join \`${this.getStorageName(attr)}\` \`${alias2}\` on \`${alias}\`.\`entityId\` = \`${alias2}\`.\`id\``;
                    extraWhere += `\`${alias}\`.\`entity\` = '${attr}'`;
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
                            from += ` left join \`${this.getStorageName(attr)}\` \`${alias2}\` on \`${alias}\`.\`entityId\` = \`${alias2}\`.\`id\``;
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
                        extraWhere += `\`${alias}\`.\`entity\` = '${attr}'`;
                    }
                }
            );
            if (node['#id']) {
                assert(!projectionRefAlias[node['#id']]);
                assign(projectionRefAlias, {
                    [node['#id']]: alias,
                });
            }
        };

        if (projection) {
            analyzeProjectionNode({ node: projection, path: './', entityName: entity, alias });
        }

        return {
            aliasDict,
            from,
            projectionRefAlias,
            filterRefAlias,
            extraWhere,
            currentNumber: number,
        };
    }

    private translateComparison(attr: string, value: any, type: DataType | Ref): string {
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
            return ` ${SQL_OP[attr]} ${this.translateAttrValue(type, value)}`;
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

    private translateElement(attr: string, value: boolean): string {
        assert(attr === '$exists');      // only support one operator now
        if (value) {
            return ' is not null';
        }
        return ' is null';
    }

    private translateEvaluation<T extends keyof ED>(attr: string, value: any, entity: T, alias: string, type: DataType | Ref, initialNumber: number, refAlias: Record<string, string>): {
        stmt: string;
        currentNumber: number;
    } {
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
                const IN_OP = {
                    $in: 'in',
                    $nin: 'not in',
                };
                if (value instanceof Array) {
                    const values = value.map(
                        (v) => {
                            if (['varchar', 'char', 'text', 'nvarchar', 'ref'].includes(type as string)) {
                                return `'${v}'`;
                            }
                            else {
                                return `${v}`;
                            }
                        }
                    );
                    if (values.length > 0) {
                        return {
                            stmt: ` ${IN_OP[attr]}(${values.join(',')})`,
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
                    const {stmt: subQueryStmt, currentNumber } = this.translateSelectInner(value.entity, value, initialNumber, refAlias, undefined);
                    return {
                        stmt: ` ${IN_OP[attr]}(${subQueryStmt})`,
                        currentNumber,
                    };
                }
            }
            default: {
                assert('$between' === attr);
                const values = value.map(
                    (v: string | number) => {
                        if (['varchar', 'char', 'text', 'nvarchar', 'ref'].includes(type as string)) {
                            return `'${v}'`;
                        }
                        else {
                            return `${v}`;
                        }
                    }
                );
                return {
                    stmt: ` between ${values[0]} and ${values[1]}`,
                    currentNumber: initialNumber,
                };
            }
        }
    }

    private translateFilter<T extends keyof ED>(
        entity: T,
        selection: Pick<ED[T]['Selection'], 'filter' | 'hint'>,
        aliasDict: Record<string, string>,
        filterRefAlias: Record<string, string>,
        initialNumber: number,
        extraWhere?: string): {
            stmt: string;
            currentNumber: number;
        } {
        const { schema } = this;
        const { filter, hint } = selection;

        let currentNumber = initialNumber;
        const translateInner = <E extends keyof ED>(entity2: E, path: string, filter2?: ED[E]['Selection']['filter'], type?: DataType | Ref): string => {
            const alias = aliasDict[path];
            const { attributes } = schema[entity2];
            let whereText = type ? '' : this.getDefaultSelectFilter(alias, hint);
            if (filter2) {
                const attrs = Object.keys(filter2).filter(
                    ele => !ele.startsWith('#')
                );
                attrs.forEach(
                    (attr) => {
                        if (whereText) {
                            whereText += ' and '
                        }
                        whereText + '(';
                        if (['$and', '$or', '$xor', '$not'].includes(attr)) {
                            let result = '';
                            switch (attr) {
                                case '$and':
                                case '$or':
                                case '$xor': {
                                    const logicQueries = filter2[attr];
                                    logicQueries.forEach(
                                        (logicQuery: ED[E]['Selection']['filter'], index: number) => {
                                            const sql = translateInner(entity2, path, logicQuery);
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
                                    const sql = translateInner(entity2, path, logicQuery);
                                    if (sql) {
                                        whereText += ` not (${translateInner(entity2, path, logicQuery)})`;
                                        break;
                                    }
                                }
                            }
                        }
                        else if (attr.toLowerCase().startsWith(EXPRESSION_PREFIX)) {
                            // expression
                            whereText += ` (${this.translateExpression(alias, filter2[attr], filterRefAlias)})`;
                        }
                        else if (['$gt', '$gte', '$lt', '$lte', '$eq', '$ne', '$startsWith', '$endsWith', '$includes'].includes(attr)) {
                            whereText += this.translateComparison(attr, filter2[attr], type!);
                        }
                        else if (['$exists'].includes(attr)) {
                            whereText += this.translateElement(attr, filter2[attr]);
                        }
                        else if (['$text', '$in', '$nin', '$between'].includes(attr)) {
                            const { stmt, currentNumber: cn2 } = this.translateEvaluation(attr, filter2[attr], entity2, alias, type!, initialNumber, filterRefAlias);
                            whereText += stmt;
                            currentNumber = cn2;
                        }
                        else {
                            const rel = judgeRelation(this.schema, entity, attr);
                            if (rel === 2) {
                                whereText += ` ${translateInner(attr, `${path}${attr}/`, filter2[attr])}`;
                            }
                            else if (typeof rel === 'string') {
                                whereText += ` ${translateInner(rel, `${path}${attr}/`, filter2[attr])}`;
                            }
                            else {
                                assert(attributes.hasOwnProperty(attr), `非法的属性${attr}`);
                                const { type: type2 } = attributes[attr];
//                                 assert (type2 !== 'ref');
                                if (typeof filter2[attr] === 'object' && Object.keys(filter2[attr])[0] && Object.keys(filter2[attr])[0].startsWith('$')) {
                                    whereText += ` \`${alias}\`.\`${attr}\` ${translateInner(entity2, path, filter2[attr], type2)}`
                                }
                                else {
                                    whereText += ` \`${alias}\`.\`${attr}\` = ${this.translateAttrValue(type2, filter2[attr])}`;
                                }
                            }
                        }

                        whereText + ')';
                    }
                );
            }
            if (!whereText) {
                whereText = 'true';     // 如果为空就赋一个永真条件，以便处理and
            }
            return whereText;
        };

        const where = translateInner(entity, './', filter);
        if (extraWhere && where) {
            return {
                stmt: `${extraWhere} and ${where}`,
                currentNumber,
            };
        }
        return {
            stmt: extraWhere || where,
            currentNumber,
        };
    }

    private translateSorter<T extends keyof ED>(entity: T, sorter: ED[T]['Selection']['sorter'], aliasDict: Record<string, string>): string {
        const translateInner = <E extends keyof ED>(entity2: E, sortAttr: DeduceSorterAttr<ED[E]['Schema']>, path: string): string => {
            assert(Object.keys(sortAttr).length === 1);
            const attr = Object.keys(sortAttr)[0];
            const alias = aliasDict[path];

            if (attr.toLocaleLowerCase().startsWith(EXPRESSION_PREFIX)) {
                return this.translateExpression(alias, sortAttr[attr] as any, {});
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
        projectionRefAlias: Record<string, string>): string {
        const { schema } = this;
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
                    if (attr.toLowerCase().startsWith(EXPRESSION_PREFIX)) {
                        const exprText = this.translateExpression(alias, projection2[attr], projectionRefAlias);
                        projText += ` ${exprText} as ${prefix}${attr}`;
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
                                projText += ` ${this.translateAttrProjection(type as DataType, alias, attr)} as \`${prefix}${attr}\``;
                            }
                            else {
                                assert(typeof projection2 === 'string');
                                projText += ` ${this.translateAttrProjection(type as DataType, alias, attr)} as \`${prefix}${projection2[attr]}\``;
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

        return translateInner(entity, projection, './');
    }

    private translateSelectInner<T extends keyof ED>(entity: T, selection: ED[T]['Selection'], initialNumber: number, refAlias: Record<string, string>, params?: SelectParams): {
        stmt: string;
        currentNumber: number;
    } {
        const { data, filter, sorter, indexFrom, count } = selection;
        const { from: fromText, aliasDict, projectionRefAlias, extraWhere, filterRefAlias, currentNumber } = this.analyzeJoin(entity, {
            projection: data,
            filter,
            sorter,
        }, initialNumber);
        assert(intersection(keys(refAlias), keys(filterRefAlias)).length === 0, 'filter中的#node结点定义有重复');
        assign(refAlias, filterRefAlias);

        const projText = this.translateProjection(entity, data, aliasDict, projectionRefAlias);

        const { stmt: filterText, currentNumber: currentNumber2 } = this.translateFilter(entity, selection, aliasDict, refAlias, currentNumber, extraWhere);

        const sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);

        return {
            stmt: this.populateSelectStmt(projText, fromText, selection, aliasDict, filterText, sorterText, indexFrom, count),
            currentNumber: currentNumber2,
        };
    }

    translateSelect<T extends keyof ED>(entity: T, selection: ED[T]['Selection'], params?: SelectParams): string {
        const { stmt } = this.translateSelectInner(entity, selection, 1, {}, params);
        return stmt;
    }

    translateCount<T extends keyof ED>(entity: T, selection: Pick<ED[T]['Selection'], 'filter'>, params?: SelectParams): string {
        const { filter } = selection;
        const { from: fromText, aliasDict, extraWhere, filterRefAlias, currentNumber } = this.analyzeJoin(entity, {
            filter,
        });

        const projText = 'count(1)';

        const { stmt: filterText } = this.translateFilter(entity, selection as ED[T]['Selection'], aliasDict, filterRefAlias, currentNumber, extraWhere);


        return this.populateSelectStmt(projText, fromText, selection as ED[T]['Selection'], aliasDict, filterText, undefined, undefined, undefined);
    }

    translateRemove<T extends keyof ED>(entity: T, operation: ED[T]['Remove'], params?: SelectParams): string {
        const { filter, sorter, indexFrom, count } = operation;
        const { aliasDict, filterRefAlias, extraWhere, from: fromText, currentNumber } = this.analyzeJoin(entity, { filter, sorter });

        const alias = aliasDict['./'];

        const { stmt: filterText } = this.translateFilter(entity, operation, aliasDict, filterRefAlias, currentNumber, extraWhere);

        const sorterText = sorter && sorter.length > 0 ? this.translateSorter(entity, sorter, aliasDict) : undefined;

        return this.populateRemoveStmt(alias, fromText, aliasDict, filterText, sorterText, indexFrom, count, params);
    }

    translateUpdate<T extends keyof ED>(entity: T, operation: ED[T]['Update'], params?: any): string {
        const { attributes } = this.schema[entity];
        const { filter, sorter, indexFrom, count, data } = operation;
        const { aliasDict, filterRefAlias, extraWhere, from: fromText, currentNumber } = this.analyzeJoin(entity, { filter, sorter });

        const alias = aliasDict['./'];

        let updateText = '';
        for (const attr in data) {
            if (updateText) {
                updateText += ',';
            }
            assert(attributes.hasOwnProperty(attr) && attributes[attr].type !== 'ref');
            const value = this.translateAttrValue(attributes[attr].type as DataType, data[attr]);
            updateText += `\`${alias}\`.\`${attr}\` = ${value}`;
        }

        const { stmt: filterText } = this.translateFilter(entity, operation, aliasDict, filterRefAlias, currentNumber, extraWhere);
        const sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);

        return this.populateUpdateStmt(updateText, fromText, aliasDict, filterText, sorterText, indexFrom, count, params);
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
        const result = `'${value.replace(/'/g, '\\\'')}'`;
        return result;
    }
}