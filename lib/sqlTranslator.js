"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SqlTranslator = void 0;
const tslib_1 = require("tslib");
const assert_1 = tslib_1.__importDefault(require("assert"));
const sqlstring_1 = tslib_1.__importDefault(require("sqlstring"));
const lodash_1 = require("lodash");
const types_1 = require("oak-domain/lib/types");
const relation_1 = require("oak-domain/lib/store/relation");
const filter_1 = require("oak-domain/lib/store/filter");
;
;
class SqlTranslator {
    schema;
    constructor(schema) {
        this.schema = this.makeFullSchema(schema);
    }
    makeFullSchema(schema2) {
        const schema = (0, lodash_1.cloneDeep)(schema2);
        for (const entity in schema) {
            const { attributes, indexes } = schema[entity];
            // 增加默认的属性
            (0, lodash_1.assign)(attributes, {
                id: {
                    type: 'char',
                    params: {
                        length: 36,
                    },
                },
                [types_1.SeqAttribute]: {
                    type: 'sequence',
                    sequenceStart: 10000,
                },
                [types_1.CreateAtAttribute]: {
                    type: 'datetime',
                    notNull: true,
                },
                [types_1.UpdateAtAttribute]: {
                    type: 'datetime',
                    notNull: true,
                },
                [types_1.DeleteAtAttribute]: {
                    type: 'datetime',
                },
                [types_1.TriggerDataAttribute]: {
                    type: 'object',
                },
                [types_1.TriggerUuidAttribute]: {
                    type: 'char',
                    params: {
                        length: 36,
                    },
                },
            });
            // 增加默认的索引
            const intrinsticIndexes = [
                {
                    name: `${entity}_create_at_auto_create`,
                    attributes: [{
                            name: types_1.CreateAtAttribute,
                        }, {
                            name: types_1.DeleteAtAttribute,
                        }]
                }, {
                    name: `${entity}_update_at_auto_create`,
                    attributes: [{
                            name: types_1.UpdateAtAttribute,
                        }, {
                            name: types_1.DeleteAtAttribute,
                        }],
                }, {
                    name: `${entity}_trigger_uuid`,
                    attributes: [{
                            name: types_1.TriggerUuidAttribute,
                        }]
                },
            ];
            // 增加外键等相关属性上的索引
            for (const attr in attributes) {
                if (attributes[attr].type === 'ref') {
                    if (!(indexes?.find(ele => ele.attributes[0].name === attr))) {
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
                        if (!(indexes?.find(ele => ele.attributes[0].name === 'entity' && ele.attributes[1]?.name === 'entityId'))) {
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
                    if (!(indexes?.find(ele => ele.attributes[0].name === attr))) {
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
                        if (!(indexes?.find(ele => ele.attributes[0].name === 'expired' && ele.attributes[1]?.name === 'expiresAt'))) {
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
                (0, lodash_1.assign)(schema[entity], {
                    indexes: intrinsticIndexes,
                });
            }
        }
        return schema;
    }
    getStorageName(entity) {
        const { storageName } = this.schema[entity];
        return (storageName || entity);
    }
    translateInsert(entity, data) {
        const { schema } = this;
        const { attributes, storageName = entity } = schema[entity];
        let sql = `insert into \`${storageName}\`(`;
        /**
         * 这里的attrs要用所有行的union集合
         */
        const dataFull = data.reduce((prev, cur) => Object.assign({}, cur, prev), {});
        const attrs = Object.keys(dataFull).filter(ele => attributes.hasOwnProperty(ele));
        attrs.forEach((attr, idx) => {
            sql += ` \`${attr}\``;
            if (idx < attrs.length - 1) {
                sql += ',';
            }
        });
        sql += ') values ';
        data.forEach((d, dataIndex) => {
            sql += '(';
            attrs.forEach((attr, attrIdx) => {
                const attrDef = attributes[attr];
                const { type: dataType } = attrDef;
                const value = this.translateAttrValue(dataType, d[attr]);
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
    analyzeJoin(entity, { projection, filter, sorter, aggregation }, initialNumber) {
        const { schema } = this;
        let number = initialNumber || 1;
        const projectionRefAlias = {};
        const filterRefAlias = {};
        const alias = `${entity}_${number++}`;
        let from = ` \`${this.getStorageName(entity)}\` \`${alias}\` `;
        const aliasDict = {
            './': alias,
        };
        const analyzeFilterNode = ({ node, path, entityName, alias }) => {
            Object.keys(node).forEach((op) => {
                if (['$and', '$or'].includes(op)) {
                    node[op].forEach((subNode) => analyzeFilterNode({
                        node: subNode,
                        path,
                        entityName,
                        alias,
                    }));
                }
                else if (['$not'].includes(op)) {
                    analyzeFilterNode({
                        node: node[op],
                        path,
                        entityName,
                        alias,
                    });
                }
                else if (['$text'].includes(op)) {
                }
                else {
                    const rel = (0, relation_1.judgeRelation)(this.schema, entityName, op);
                    if (typeof rel === 'string') {
                        let alias2;
                        const pathAttr = `${path}${op}/`;
                        if (!aliasDict.hasOwnProperty(pathAttr)) {
                            alias2 = `${rel}_${number++}`;
                            (0, lodash_1.assign)(aliasDict, {
                                [pathAttr]: alias2,
                            });
                            from += ` left join \`${this.getStorageName(rel)}\` \`${alias2}\` on \`${alias}\`.\`${op}Id\` = \`${alias2}\`.\`id\``;
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
                        let alias2;
                        const pathAttr = `${path}${op}/`;
                        if (!aliasDict.hasOwnProperty(pathAttr)) {
                            alias2 = `${op}_${number++}`;
                            (0, lodash_1.assign)(aliasDict, {
                                [pathAttr]: alias2,
                            });
                            from += ` left join \`${this.getStorageName(op)}\` \`${alias2}\` on \`${alias}\`.\`entityId\` = \`${alias2}\`.\`id\` and \`${alias}\`.\`entity\` = '${op}'`;
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
                (0, lodash_1.assign)(filterRefAlias, {
                    [node['#id']]: [alias, entityName],
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
        const analyzeSortNode = ({ node, path, entityName, alias }) => {
            const attr = (0, lodash_1.keys)(node)[0];
            const rel = (0, relation_1.judgeRelation)(this.schema, entityName, attr);
            if (typeof rel === 'string') {
                const pathAttr = `${path}${attr}/`;
                let alias2;
                if (!aliasDict.hasOwnProperty(pathAttr)) {
                    alias2 = `${rel}_${number++}`;
                    (0, lodash_1.assign)(aliasDict, {
                        [pathAttr]: alias2,
                    });
                    from += ` left join \`${this.getStorageName(rel)}\` \`${alias2}\` on \`${alias}\`.\`${attr}Id\` = \`${alias2}\`.\`id\``;
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
                const pathAttr = `${path}${attr}/`;
                let alias2;
                if (!aliasDict.hasOwnProperty(pathAttr)) {
                    alias2 = `${attr}_${number++}`;
                    (0, lodash_1.assign)(aliasDict, {
                        [pathAttr]: alias2,
                    });
                    from += ` left join \`${this.getStorageName(attr)}\` \`${alias2}\` on \`${alias}\`.\`entityId\` = \`${alias2}\`.\`id\` and \`${alias}\`.\`entity\` = '${attr}'`;
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
            sorter.forEach((sortNode) => {
                analyzeSortNode({
                    node: sortNode.$attr,
                    path: './',
                    entityName: entity,
                    alias,
                });
            });
        }
        const analyzeProjectionNode = ({ node, path, entityName, alias }) => {
            const { attributes } = schema[entityName];
            Object.keys(node).forEach((attr) => {
                const rel = (0, relation_1.judgeRelation)(this.schema, entityName, attr);
                if (typeof rel === 'string') {
                    const pathAttr = `${path}${attr}/`;
                    let alias2;
                    if (!aliasDict.hasOwnProperty(pathAttr)) {
                        alias2 = `${rel}_${number++}`;
                        (0, lodash_1.assign)(aliasDict, {
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
                    let alias2;
                    if (!aliasDict.hasOwnProperty(pathAttr)) {
                        alias2 = `${attr}_${number++}`;
                        (0, lodash_1.assign)(aliasDict, {
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
            });
            if (node['#id']) {
                (0, assert_1.default)(!projectionRefAlias[node['#id']], `projection上有重复的#id定义「${node['#id']}」`);
                (0, lodash_1.assign)(projectionRefAlias, {
                    [node['#id']]: [alias, entityName],
                });
            }
        };
        if (projection) {
            analyzeProjectionNode({ node: projection, path: './', entityName: entity, alias });
        }
        else if (aggregation) {
            for (const k in aggregation) {
                analyzeProjectionNode({
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
    translateComparison(attr, value, type) {
        const SQL_OP = {
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
    translateEvaluation(attr, value, entity, alias, type, initialNumber, refAlias) {
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
                throw new Error(`${attr} is not evaluation predicate`);
            }
        }
    }
    translatePredicate(predicate, value, type) {
        if (['$gt', '$gte', '$lt', '$lte', '$eq', '$ne', '$startsWith', '$endsWith', '$includes'].includes(predicate)) {
            return this.translateComparison(predicate, value, type);
        }
        else if (['$in', '$nin'].includes(predicate)) {
            (0, assert_1.default)(value instanceof Array);
            const IN_OP = {
                $in: 'in',
                $nin: 'not in',
            };
            const values = value.map((v) => {
                if (type && ['varchar', 'char', 'text', 'nvarchar', 'ref', 'enum'].includes(type) || typeof v === 'string') {
                    return `'${v}'`;
                }
                else {
                    return `${v}`;
                }
            });
            if (values.length > 0) {
                return ` ${IN_OP[predicate]}(${values.join(',')})`;
            }
            if (predicate === '$in') {
                return ' in (null)';
            }
            return ' is not null';
        }
        else if (predicate === '$between') {
            const values = value.map((v) => {
                if (type && ['varchar', 'char', 'text', 'nvarchar', 'ref', 'enum'].includes(type) || typeof v === 'string') {
                    return `'${v}'`;
                }
                else {
                    return `${v}`;
                }
            });
            // between是所有数据库都支持的语法吗？
            return ` between ${values[0]} and ${values[1]}`;
        }
        else if (predicate === '$mod') {
            // %是所有数据库都支持的语法吗？
            return ` % ${value[0]} = ${value[1]}`;
        }
        else {
            (0, assert_1.default)(predicate === '$exists');
            if (value) {
                return ' is not null';
            }
            return ' is null';
        }
    }
    translateFilter(entity, filter, aliasDict, filterRefAlias, initialNumber, option) {
        const { schema } = this;
        let currentNumber = initialNumber;
        const translateInner = (entity2, path, filter2, type) => {
            const alias = aliasDict[path];
            const { attributes } = schema[entity2];
            let whereText = type ? '' : this.getDefaultSelectFilter(alias, option);
            if (filter2) {
                const attrs = Object.keys(filter2).filter(ele => !ele.startsWith('#'));
                attrs.forEach((attr) => {
                    if (whereText) {
                        whereText += ' and ';
                    }
                    if (['$and', '$or', '$xor', '$not'].includes(attr)) {
                        whereText += '(';
                        switch (attr) {
                            case '$and':
                            case '$or':
                            case '$xor': {
                                const logicQueries = filter2[attr];
                                logicQueries.forEach((logicQuery, index) => {
                                    const sql = translateInner(entity2, path, logicQuery, 'ref'); // 只要传个值就行了，应该无所谓
                                    if (sql) {
                                        whereText += ` (${sql})`;
                                        if (index < logicQueries.length - 1) {
                                            whereText += ` ${attr.slice(1)}`;
                                        }
                                    }
                                });
                                break;
                            }
                            default: {
                                (0, assert_1.default)(attr === '$not');
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
                        whereText += `(${this.translateFullTextSearch(filter2[attr], entity2, alias)})`;
                    }
                    else if (attr.toLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                        // expression
                        whereText += ` (${this.translateExpression(entity2, alias, filter2[attr], filterRefAlias)})`;
                    }
                    else {
                        const rel = (0, relation_1.judgeRelation)(this.schema, entity2, attr);
                        if (rel === 2) {
                            whereText += ` (${translateInner(attr, `${path}${attr}/`, filter2[attr])})`;
                        }
                        else if (typeof rel === 'string') {
                            whereText += ` (${translateInner(rel, `${path}${attr}/`, filter2[attr])})`;
                        }
                        else if (rel instanceof Array) {
                            const [subEntity, foreignKey] = rel;
                            const predicate = (filter2[attr]['#sqp'] || 'in');
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
                            const refAlia = Object.keys(filterRefAlias).find(ele => filterRefAlias[ele][0] === alias);
                            const refAlia2 = refAlia || alias; // alias一定是唯一的，可以用来作为node id
                            if (!refAlia) {
                                (0, assert_1.default)(!filterRefAlias[refAlia2]);
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
                                filter: (0, filter_1.combineFilters)(subEntity, this.schema, [joinFilter, filter2[attr]]),
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
                            (0, assert_1.default)(attributes.hasOwnProperty(attr), `非法的属性${attr}`);
                            const { type: type2 } = attributes[attr];
                            // assert (type2 !== 'ref');
                            if (typeof filter2[attr] === 'object') {
                                if (['object', 'array'].includes(type2)) {
                                    // 对object数据的深层次查询，这里调用数据库所支持的属性对象级查询，如mysql中的json查询
                                    whereText += `(${this.translateObjectPredicate(filter2[attr], alias, attr)})`;
                                }
                                else {
                                    (0, assert_1.default)(Object.keys(filter2[attr]).length === 1);
                                    const predicate = Object.keys(filter2[attr])[0];
                                    (0, assert_1.default)(predicate.startsWith('$'));
                                    // 对属性上的谓词处理
                                    whereText += ` (\`${alias}\`.\`${attr}\` ${this.translatePredicate(predicate, filter2[attr][predicate], type2)})`;
                                }
                            }
                            else {
                                whereText += ` (\`${alias}\`.\`${attr}\` = ${this.translateAttrValue(type2, filter2[attr])})`;
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
        const where = translateInner(entity, './', filter);
        return {
            stmt: where,
            currentNumber,
        };
    }
    translateSorter(entity, sorter, aliasDict) {
        const translateInner = (entity2, sortAttr, path) => {
            (0, assert_1.default)(Object.keys(sortAttr).length === 1);
            const attr = Object.keys(sortAttr)[0];
            const alias = aliasDict[path];
            if (attr.toLocaleLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                return this.translateExpression(entity2, alias, sortAttr[attr], {});
            }
            else if (sortAttr[attr] === 1) {
                return `\`${alias}\`.\`${attr}\``;
            }
            else {
                const rel = (0, relation_1.judgeRelation)(this.schema, entity2, attr);
                if (typeof rel === 'string') {
                    return translateInner(rel, sortAttr[attr], `${path}${attr}/`);
                }
                else {
                    (0, assert_1.default)(rel === 2);
                    return translateInner(attr, sortAttr[attr], `${path}${attr}/`);
                }
            }
        };
        let sortText = '';
        sorter.forEach((sortNode, index) => {
            const { $attr, $direction } = sortNode;
            sortText += translateInner(entity, $attr, './');
            if ($direction) {
                sortText += ` ${$direction}`;
            }
            if (index < sorter.length - 1) {
                sortText += ',';
            }
        });
        return sortText;
    }
    translateProjection(entity, projection, aliasDict, projectionRefAlias, commonPrefix, disableAs) {
        const { schema } = this;
        let as = '';
        const translateInner = (entity2, projection2, path) => {
            const alias = aliasDict[path];
            const { attributes } = schema[entity2];
            let projText = '';
            let prefix = path.slice(2).replace(/\//g, '.');
            const attrs = Object.keys(projection2).filter((attr) => {
                if (attr.toLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                    return true;
                }
                const rel = (0, relation_1.judgeRelation)(this.schema, entity2, attr);
                return [1, 2].includes(rel) || typeof rel === 'string';
            });
            attrs.forEach((attr, idx) => {
                const prefix2 = commonPrefix ? `${commonPrefix}.${prefix}` : prefix;
                if (attr.toLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
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
                    const rel = (0, relation_1.judgeRelation)(this.schema, entity2, attr);
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
                                projText += ` ${this.translateAttrProjection(type, alias, attr)}`;
                            }
                            else {
                                projText += ` ${this.translateAttrProjection(type, alias, attr)} as \`${prefix2}${attr}\``;
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
                            (0, assert_1.default)(!disableAs);
                            (0, assert_1.default)(['object', 'array'].includes(type));
                            projText += ` ${this.translateObjectProjection(projection2[attr], alias, attr, prefix2)}`;
                        }
                        else {
                            (0, assert_1.default)(typeof projection2 === 'string');
                            if (disableAs) {
                                projText += ` ${this.translateAttrProjection(type, alias, attr)}`;
                            }
                            else {
                                projText += ` ${this.translateAttrProjection(type, alias, attr)} as \`${prefix2}${projection2[attr]}\``;
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
            });
            return projText;
        };
        return {
            projText: translateInner(entity, projection, './'),
            as,
        };
    }
    translateSelectInner(entity, selection, initialNumber, refAlias, option) {
        const { data, filter, sorter, indexFrom, count } = selection;
        const { from: fromText, aliasDict, projectionRefAlias, filterRefAlias, currentNumber } = this.analyzeJoin(entity, {
            projection: data,
            filter,
            sorter,
        }, initialNumber);
        (0, assert_1.default)((0, lodash_1.intersection)((0, lodash_1.keys)(refAlias), (0, lodash_1.keys)(filterRefAlias)).length === 0, 'filter中的#node结点定义有重复');
        (0, lodash_1.assign)(refAlias, filterRefAlias);
        const { projText } = this.translateProjection(entity, data, aliasDict, projectionRefAlias);
        const { stmt: filterText, currentNumber: currentNumber2 } = this.translateFilter(entity, filter, aliasDict, refAlias, currentNumber, option);
        const sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);
        return {
            stmt: this.populateSelectStmt(projText, fromText, aliasDict, filterText, sorterText, undefined, indexFrom, count, option, selection),
            currentNumber: currentNumber2,
            filterStmt: filterText,
        };
    }
    translateSelect(entity, selection, option) {
        const { stmt } = this.translateSelectInner(entity, selection, 1, {}, option);
        return stmt;
    }
    translateWhere(entity, selection, option) {
        const { filterStmt } = this.translateSelectInner(entity, selection, 1, {}, option);
        return filterStmt;
    }
    translateAggregate(entity, aggregation, option) {
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
                const { projText: projSubText, as } = this.translateProjection(entity, data[k], aliasDict, projectionRefAlias, '#data');
                if (!projText) {
                    projText = projSubText;
                }
                else {
                    projText += `, ${projSubText}`;
                }
                groupByText = as;
            }
            else {
                const { projText: projSubText } = this.translateProjection(entity, data[k], aliasDict, projectionRefAlias, undefined, true);
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
                    (0, assert_1.default)(k.startsWith('#avg'));
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
    translateCount(entity, selection, option) {
        const { filter, count } = selection;
        const { from: fromText, aliasDict, filterRefAlias, currentNumber } = this.analyzeJoin(entity, {
            filter,
        });
        const projText = 'count(1) cnt';
        const { stmt: filterText } = this.translateFilter(entity, filter, aliasDict, filterRefAlias, currentNumber, option);
        if (count && count > 0) {
            const subQuerySql = this.populateSelectStmt('1', fromText, aliasDict, filterText, undefined, undefined, 0, count, option, Object.assign({}, selection, { indexFrom: 0, count }));
            return `select count(1) cnt from (${subQuerySql}) __tmp`;
        }
        return this.populateSelectStmt(projText, fromText, aliasDict, filterText, undefined, undefined, undefined, undefined, option, selection);
    }
    translateRemove(entity, operation, option) {
        const { data, filter, sorter, indexFrom, count } = operation;
        (0, assert_1.default)(!sorter, '当前remove不支持sorter行为');
        const { aliasDict, filterRefAlias, from: fromText, currentNumber } = this.analyzeJoin(entity, { filter, sorter });
        const alias = aliasDict['./'];
        const { stmt: filterText } = this.translateFilter(entity, filter, aliasDict, filterRefAlias, currentNumber, { includedDeleted: true });
        // const sorterText = sorter && sorter.length > 0 ? this.translateSorter(entity, sorter, aliasDict) : undefined;
        const { attributes } = this.schema[entity];
        let updateText = '';
        for (const attr in data) {
            if (updateText) {
                updateText += ',';
            }
            // delete只支持对volatile trigger的metadata域赋值
            (0, assert_1.default)([types_1.TriggerDataAttribute, types_1.TriggerUuidAttribute].includes(attr));
            const value = this.translateAttrValue(attributes[attr].type, data[attr]);
            updateText += `\`${alias}\`.\`${attr}\` = ${value}`;
        }
        return this.populateRemoveStmt(updateText, fromText, aliasDict, filterText, /* sorterText */ undefined, indexFrom, count, option);
    }
    translateUpdate(entity, operation, option) {
        const { attributes } = this.schema[entity];
        const { filter, sorter, indexFrom, count, data } = operation;
        (0, assert_1.default)(!sorter, '当前update不支持sorter行为');
        const { aliasDict, filterRefAlias, from: fromText, currentNumber } = this.analyzeJoin(entity, { filter, sorter });
        const alias = aliasDict['./'];
        let updateText = '';
        for (const attr in data) {
            if (updateText) {
                updateText += ',';
            }
            (0, assert_1.default)(attributes.hasOwnProperty(attr));
            const value = this.translateAttrValue(attributes[attr].type, data[attr]);
            updateText += `\`${alias}\`.\`${attr}\` = ${value}`;
        }
        const { stmt: filterText } = this.translateFilter(entity, filter, aliasDict, filterRefAlias, currentNumber, option);
        // const sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);
        return this.populateUpdateStmt(updateText, fromText, aliasDict, filterText, /* sorterText */ undefined, indexFrom, count, option);
    }
    translateDestroyEntity(entity, truncate) {
        const { schema } = this;
        const { storageName = entity, view } = schema[entity];
        let sql;
        if (view) {
            sql = `drop view if exists \`${storageName}\``;
        }
        else {
            sql = truncate ? `truncate table \`${storageName}\`` : `drop table if exists \`${storageName}\``;
        }
        return sql;
    }
    escapeStringValue(value) {
        const result = sqlstring_1.default.escape(value);
        return result;
    }
}
exports.SqlTranslator = SqlTranslator;
