"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SqlTranslator = void 0;
const assert_1 = __importDefault(require("assert"));
const lodash_1 = require("lodash");
const luxon_1 = require("luxon");
const types_1 = require("oak-domain/lib/types");
const relation_1 = require("oak-domain/lib/store/relation");
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
                $$createAt$$: {
                    type: 'date',
                    notNull: true,
                },
                $$updateAt$$: {
                    type: 'date',
                    notNull: true,
                },
                $$deleteAt$$: {
                    type: 'date',
                },
                $$triggerData$$: {
                    type: 'object',
                },
                $$triggerTimestamp$$: {
                    type: 'date',
                },
            });
            // 增加默认的索引
            const intrinsticIndexes = [
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
                    if (!(indexes?.find(ele => ele.attributes[0].name === attr))) {
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
                        if (!(indexes?.find(ele => ele.attributes[0].name === 'entity' && ele.attributes[1]?.name === 'entityId'))) {
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
        const attrs = Object.keys(data[0]).filter(ele => attributes.hasOwnProperty(ele));
        attrs.forEach((attr, idx) => {
            sql += ` \`${attr}\``;
            if (idx < attrs.length - 1) {
                sql += ',';
            }
        });
        sql += ', `$$createAt$$`, `$$updateAt$$`) values ';
        const now = luxon_1.DateTime.now().toFormat('yyyy-LL-dd HH:mm:ss');
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
            sql += `, '${now}', '${now}')`;
            if (dataIndex < data.length - 1) {
                sql += ',';
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
    analyzeJoin(entity, { projection, filter, sorter, isStat }, initialNumber) {
        const { schema } = this;
        let number = initialNumber || 1;
        const projectionRefAlias = {};
        const filterRefAlias = {};
        let extraWhere = '';
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
                            from += ` left join \`${this.getStorageName(op)}\` \`${alias2}\` on \`${alias}\`.\`entityId\` = \`${alias2}\`.\`id\``;
                            extraWhere += `\`${alias}\`.\`entity\` = '${op}'`;
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
                (0, lodash_1.assign)(filterRefAlias, {
                    [node['#id']]: alias,
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
                    from += ` left join \`${this.getStorageName(attr)}\` \`${alias2}\` on \`${alias}\`.\`entityId\` = \`${alias2}\`.\`id\``;
                    extraWhere += `\`${alias}\`.\`entity\` = '${attr}'`;
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
            });
            if (node['#id']) {
                (0, assert_1.default)(!projectionRefAlias[node['#id']]);
                (0, lodash_1.assign)(projectionRefAlias, {
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
    translateElement(attr, value) {
        (0, assert_1.default)(attr === '$exists'); // only support one operator now
        if (value) {
            return ' is not null';
        }
        return ' is null';
    }
    translateEvaluation(attr, value, entity, alias, type, initialNumber, refAlias) {
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
                    const values = value.map((v) => {
                        if (['varchar', 'char', 'text', 'nvarchar', 'ref'].includes(type)) {
                            return `'${v}'`;
                        }
                        else {
                            return `${v}`;
                        }
                    });
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
                    const { stmt: subQueryStmt, currentNumber } = this.translateSelectInner(value.entity, value, initialNumber, refAlias, undefined);
                    return {
                        stmt: ` ${IN_OP[attr]}(${subQueryStmt})`,
                        currentNumber,
                    };
                }
            }
            default: {
                (0, assert_1.default)('$between' === attr);
                const values = value.map((v) => {
                    if (['varchar', 'char', 'text', 'nvarchar', 'ref'].includes(type)) {
                        return `'${v}'`;
                    }
                    else {
                        return `${v}`;
                    }
                });
                return {
                    stmt: ` between ${values[0]} and ${values[1]}`,
                    currentNumber: initialNumber,
                };
            }
        }
    }
    translateFilter(entity, selection, aliasDict, filterRefAlias, initialNumber, extraWhere) {
        const { schema } = this;
        const { filter, hint } = selection;
        let currentNumber = initialNumber;
        const translateInner = (entity2, path, filter2, type) => {
            const alias = aliasDict[path];
            const { attributes } = schema[entity2];
            let whereText = type ? '' : this.getDefaultSelectFilter(alias, hint);
            if (filter2) {
                const attrs = Object.keys(filter2).filter(ele => !ele.startsWith('#'));
                attrs.forEach((attr) => {
                    if (whereText) {
                        whereText += ' and ';
                    }
                    whereText + '(';
                    if (['$and', '$or', '$xor', '$not'].includes(attr)) {
                        let result = '';
                        switch (attr) {
                            case '$and':
                            case '$or':
                            case '$xor': {
                                const logicQueries = filter2[attr];
                                logicQueries.forEach((logicQuery, index) => {
                                    const sql = translateInner(entity2, path, logicQuery);
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
                                const sql = translateInner(entity2, path, logicQuery);
                                if (sql) {
                                    whereText += ` not (${translateInner(entity2, path, logicQuery)})`;
                                    break;
                                }
                            }
                        }
                    }
                    else if (attr.toLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                        // expression
                        whereText += ` (${this.translateExpression(alias, filter2[attr], filterRefAlias)})`;
                    }
                    else if (['$gt', '$gte', '$lt', '$lte', '$eq', '$ne', '$startsWith', '$endsWith', '$includes'].includes(attr)) {
                        whereText += this.translateComparison(attr, filter2[attr], type);
                    }
                    else if (['$exists'].includes(attr)) {
                        whereText += this.translateElement(attr, filter2[attr]);
                    }
                    else if (['$text', '$in', '$nin', '$between'].includes(attr)) {
                        const { stmt, currentNumber: cn2 } = this.translateEvaluation(attr, filter2[attr], entity2, alias, type, initialNumber, filterRefAlias);
                        whereText += stmt;
                        currentNumber = cn2;
                    }
                    else {
                        const rel = (0, relation_1.judgeRelation)(this.schema, entity, attr);
                        if (rel === 2) {
                            whereText += ` ${translateInner(attr, `${path}${attr}/`, filter2[attr])}`;
                        }
                        else if (typeof rel === 'string') {
                            whereText += ` ${translateInner(rel, `${path}${attr}/`, filter2[attr])}`;
                        }
                        else {
                            (0, assert_1.default)(attributes.hasOwnProperty(attr), `非法的属性${attr}`);
                            const { type: type2 } = attributes[attr];
                            //                                 assert (type2 !== 'ref');
                            if (typeof filter2[attr] === 'object' && Object.keys(filter2[attr])[0] && Object.keys(filter2[attr])[0].startsWith('$')) {
                                whereText += ` \`${alias}\`.\`${attr}\` ${translateInner(entity2, path, filter2[attr], type2)}`;
                            }
                            else {
                                whereText += ` \`${alias}\`.\`${attr}\` = ${this.translateAttrValue(type2, filter2[attr])}`;
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
    translateSorter(entity, sorter, aliasDict) {
        const translateInner = (entity2, sortAttr, path) => {
            (0, assert_1.default)(Object.keys(sortAttr).length === 1);
            const attr = Object.keys(sortAttr)[0];
            const alias = aliasDict[path];
            if (attr.toLocaleLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                return this.translateExpression(alias, sortAttr[attr], {});
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
    translateProjection(entity, projection, aliasDict, projectionRefAlias) {
        const { schema } = this;
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
                if (attr.toLowerCase().startsWith(types_1.EXPRESSION_PREFIX)) {
                    const exprText = this.translateExpression(alias, projection2[attr], projectionRefAlias);
                    projText += ` ${exprText} as ${prefix}${attr}`;
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
                            projText += ` ${this.translateAttrProjection(type, alias, attr)} as \`${prefix}${attr}\``;
                        }
                        else {
                            (0, assert_1.default)(typeof projection2 === 'string');
                            projText += ` ${this.translateAttrProjection(type, alias, attr)} as \`${prefix}${projection2[attr]}\``;
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
    }
    translateSelectInner(entity, selection, initialNumber, refAlias, params) {
        const { data, filter, sorter, indexFrom, count } = selection;
        const { from: fromText, aliasDict, projectionRefAlias, extraWhere, filterRefAlias, currentNumber } = this.analyzeJoin(entity, {
            projection: data,
            filter,
            sorter,
        }, initialNumber);
        (0, assert_1.default)((0, lodash_1.intersection)((0, lodash_1.keys)(refAlias), (0, lodash_1.keys)(filterRefAlias)).length === 0, 'filter中的#node结点定义有重复');
        (0, lodash_1.assign)(refAlias, filterRefAlias);
        const projText = this.translateProjection(entity, data, aliasDict, projectionRefAlias);
        const { stmt: filterText, currentNumber: currentNumber2 } = this.translateFilter(entity, selection, aliasDict, refAlias, currentNumber, extraWhere);
        const sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);
        return {
            stmt: this.populateSelectStmt(projText, fromText, selection, aliasDict, filterText, sorterText, indexFrom, count),
            currentNumber: currentNumber2,
        };
    }
    translateSelect(entity, selection, params) {
        const { stmt } = this.translateSelectInner(entity, selection, 1, {}, params);
        return stmt;
    }
    translateCount(entity, selection, params) {
        const { filter } = selection;
        const { from: fromText, aliasDict, extraWhere, filterRefAlias, currentNumber } = this.analyzeJoin(entity, {
            filter,
        });
        const projText = 'count(1)';
        const { stmt: filterText } = this.translateFilter(entity, selection, aliasDict, filterRefAlias, currentNumber, extraWhere);
        return this.populateSelectStmt(projText, fromText, selection, aliasDict, filterText, undefined, undefined, undefined);
    }
    translateRemove(entity, operation, params) {
        const { filter, sorter, indexFrom, count } = operation;
        const { aliasDict, filterRefAlias, extraWhere, from: fromText, currentNumber } = this.analyzeJoin(entity, { filter, sorter });
        const alias = aliasDict['./'];
        const { stmt: filterText } = this.translateFilter(entity, operation, aliasDict, filterRefAlias, currentNumber, extraWhere);
        const sorterText = sorter && sorter.length > 0 ? this.translateSorter(entity, sorter, aliasDict) : undefined;
        return this.populateRemoveStmt(alias, fromText, aliasDict, filterText, sorterText, indexFrom, count, params);
    }
    translateUpdate(entity, operation, params) {
        const { attributes } = this.schema[entity];
        const { filter, sorter, indexFrom, count, data } = operation;
        const { aliasDict, filterRefAlias, extraWhere, from: fromText, currentNumber } = this.analyzeJoin(entity, { filter, sorter });
        const alias = aliasDict['./'];
        let updateText = '';
        for (const attr in data) {
            if (updateText) {
                updateText += ',';
            }
            (0, assert_1.default)(attributes.hasOwnProperty(attr) && attributes[attr].type !== 'ref');
            const value = this.translateAttrValue(attributes[attr].type, data[attr]);
            updateText += `\`${alias}\`.\`${attr}\` = ${value}`;
        }
        const { stmt: filterText } = this.translateFilter(entity, operation, aliasDict, filterRefAlias, currentNumber, extraWhere);
        const sorterText = sorter && this.translateSorter(entity, sorter, aliasDict);
        return this.populateUpdateStmt(updateText, fromText, aliasDict, filterText, sorterText, indexFrom, count, params);
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
        const result = `'${value.replace(/'/g, '\\\'')}'`;
        return result;
    }
}
exports.SqlTranslator = SqlTranslator;
