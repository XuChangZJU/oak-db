"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MysqlStore = void 0;
const tslib_1 = require("tslib");
const CascadeStore_1 = require("oak-domain/lib/store/CascadeStore");
const connector_1 = require("./connector");
const translator_1 = require("./translator");
const lodash_1 = require("lodash");
const assert_1 = tslib_1.__importDefault(require("assert"));
const relation_1 = require("oak-domain/lib/store/relation");
function convertGeoTextToObject(geoText) {
    if (geoText.startsWith('POINT')) {
        const coord = geoText.match((/(\d|\.)+(?=\)|\s)/g));
        return {
            type: 'Point',
            coordinate: coord.map(ele => parseFloat(ele)),
        };
    }
    else {
        throw new Error('only support Point now');
    }
}
class MysqlStore extends CascadeStore_1.CascadeStore {
    aggregateSync(entity, aggregation, context, option) {
        throw new Error('MySQL store不支持同步取数据，不应该跑到这儿');
    }
    selectAbjointRow(entity, selection, context, option) {
        throw new Error('MySQL store不支持同步取数据，不应该跑到这儿');
    }
    updateAbjointRow(entity, operation, context, option) {
        throw new Error('MySQL store不支持同步更新数据，不应该跑到这儿');
    }
    exec(script, txnId) {
        return this.connector.exec(script, txnId);
    }
    connector;
    translator;
    constructor(storageSchema, configuration) {
        super(storageSchema);
        this.connector = new connector_1.MySqlConnector(configuration);
        this.translator = new translator_1.MySqlTranslator(storageSchema);
    }
    async aggregateAsync(entity, aggregation, context, option) {
        const sql = this.translator.translateAggregate(entity, aggregation, option);
        const result = await this.connector.exec(sql, context.getCurrentTxnId());
        return this.formResult(entity, result);
    }
    aggregate(entity, aggregation, context, option) {
        return this.aggregateAsync(entity, aggregation, context, option);
    }
    supportManyToOneJoin() {
        return true;
    }
    supportMultipleCreate() {
        return true;
    }
    formResult(entity, result) {
        const schema = this.getSchema();
        /*  function resolveObject(r: Record<string, any>, path: string, value: any) {
             const i = path.indexOf(".");
             const bs = path.indexOf('[');
             const be = path.indexOf(']');
             if (i === -1 && bs === -1) {
                 r[i] = value;
             }
             else if (i === -1) {
 
             }
             else if (bs === -1) {
                 const attrHead = path.slice(0, i);
                 const attrTail = path.slice(i + 1);
                 if (!r[attrHead]) {
                     r[attrHead] = {};
                 }
                 resolveObject(r[attrHead], attrTail, value);
             }
         } */
        function resolveAttribute(entity2, r, attr, value) {
            const { attributes, view } = schema[entity2];
            if (!view) {
                const i = attr.indexOf(".");
                if (i !== -1) {
                    const attrHead = attr.slice(0, i);
                    const attrTail = attr.slice(i + 1);
                    const rel = (0, relation_1.judgeRelation)(schema, entity2, attrHead);
                    if (rel === 1) {
                        (0, lodash_1.set)(r, attr, value);
                    }
                    else {
                        if (!r[attrHead]) {
                            r[attrHead] = {};
                        }
                        if (rel === 0) {
                            resolveAttribute(entity2, r[attrHead], attrTail, value);
                        }
                        else if (rel === 2) {
                            resolveAttribute(attrHead, r[attrHead], attrTail, value);
                        }
                        else {
                            (0, assert_1.default)(typeof rel === 'string');
                            resolveAttribute(rel, r[attrHead], attrTail, value);
                        }
                    }
                }
                else if (attributes[attr]) {
                    const { type } = attributes[attr];
                    switch (type) {
                        case 'date':
                        case 'time': {
                            if (value instanceof Date) {
                                r[attr] = value.valueOf();
                            }
                            else {
                                r[attr] = value;
                            }
                            break;
                        }
                        case 'geometry': {
                            if (typeof value === 'string') {
                                r[attr] = convertGeoTextToObject(value);
                            }
                            else {
                                r[attr] = value;
                            }
                            break;
                        }
                        case 'object':
                        case 'array': {
                            if (typeof value === 'string') {
                                r[attr] = JSON.parse(value.replace(/[\r]/g, '\\r').replace(/[\n]/g, '\\n'));
                            }
                            else {
                                r[attr] = value;
                            }
                            break;
                        }
                        case 'function': {
                            if (typeof value === 'string') {
                                // 函数的执行环境需要的参数只有创建函数者知悉，只能由上层再创建Function
                                r[attr] = `return ${Buffer.from(value, 'base64').toString()}`;
                            }
                            else {
                                r[attr] = value;
                            }
                            break;
                        }
                        case 'bool':
                        case 'boolean': {
                            if (value === 0) {
                                r[attr] = false;
                            }
                            else if (value === 1) {
                                r[attr] = true;
                            }
                            else {
                                r[attr] = value;
                            }
                            break;
                        }
                        case 'decimal': {
                            // mysql内部取回decimal是字符串
                            if (typeof value === 'string') {
                                r[attr] = parseFloat(value);
                            }
                            else {
                                (0, assert_1.default)(value === null || typeof value === 'number');
                                r[attr] = value;
                            }
                            break;
                        }
                        default: {
                            r[attr] = value;
                        }
                    }
                }
                else {
                    r[attr] = value;
                }
            }
            else {
                (0, lodash_1.assign)(r, {
                    [attr]: value,
                });
            }
        }
        function removeNullObjects(r, e) {
            // assert(r.id && typeof r.id === 'string', `对象${<string>e}取数据时发现id为非法值${r.id},rowId是${r.id}`)
            for (let attr in r) {
                const rel = (0, relation_1.judgeRelation)(schema, e, attr);
                if (rel === 2) {
                    // 边界，如果是toModi的对象，这里的外键确实有可能为空
                    (0, assert_1.default)(schema[e].toModi || r.entity !== attr || r.entityId === r[attr].id, `对象${e}取数据时，发现entityId与连接的对象的主键不一致，rowId是${r.id}，其entityId值为${r.entityId}，连接的对象的主键为${r[attr].id}`);
                    if (r[attr].id === null) {
                        (0, assert_1.default)(schema[e].toModi || r.entity !== attr);
                        delete r[attr];
                        continue;
                    }
                    (0, assert_1.default)(r.entity === attr, `对象${e}取数据时，发现entity值与连接的外键对象不一致，rowId是${r.id}，其entity值为${r.entity}，连接的对象为${attr}`);
                    removeNullObjects(r[attr], attr);
                }
                else if (typeof rel === 'string') {
                    // 边界，如果是toModi的对象，这里的外键确实有可能为空
                    (0, assert_1.default)(schema[e].toModi || r[`${attr}Id`] === r[attr].id, `对象${e}取数据时，发现其外键与连接的对象的主键不一致，rowId是${r.id}，其${attr}Id值为${r[`${attr}Id`]}，连接的对象的主键为${r[attr].id}`);
                    if (r[attr].id === null) {
                        (0, assert_1.default)(schema[e].toModi || r[`${attr}Id`] === null);
                        delete r[attr];
                        continue;
                    }
                    removeNullObjects(r[attr], rel);
                }
            }
        }
        function formSingleRow(r) {
            let result2 = {};
            for (let attr in r) {
                const value = r[attr];
                resolveAttribute(entity, result2, attr, value);
            }
            removeNullObjects(result2, entity);
            return result2;
        }
        if (result instanceof Array) {
            return result.map(r => formSingleRow(r));
        }
        return formSingleRow(result);
    }
    async selectAbjointRowAsync(entity, selection, context, option) {
        const sql = this.translator.translateSelect(entity, selection, option);
        const result = await this.connector.exec(sql, context.getCurrentTxnId());
        return this.formResult(entity, result);
    }
    async updateAbjointRowAsync(entity, operation, context, option) {
        const { translator, connector } = this;
        const { action } = operation;
        const txn = context.getCurrentTxnId();
        switch (action) {
            case 'create': {
                const { data } = operation;
                const sql = translator.translateInsert(entity, data instanceof Array ? data : [data]);
                await connector.exec(sql, txn);
                return data instanceof Array ? data.length : 1;
            }
            case 'remove': {
                const sql = translator.translateRemove(entity, operation, option);
                await connector.exec(sql, txn);
                // todo 这里对sorter和indexfrom/count的支持不完整
                return 1;
            }
            default: {
                (0, assert_1.default)(!['select', 'download', 'stat'].includes(action));
                const sql = translator.translateUpdate(entity, operation, option);
                await connector.exec(sql, txn);
                // todo 这里对sorter和indexfrom/count的支持不完整
                return 1;
            }
        }
    }
    async operate(entity, operation, context, option) {
        const { action } = operation;
        (0, assert_1.default)(!['select', 'download', 'stat'].includes(action), '现在不支持使用select operation');
        return await super.operateAsync(entity, operation, context, option);
    }
    async select(entity, selection, context, option) {
        const result = await super.selectAsync(entity, selection, context, option);
        return result;
    }
    async count(entity, selection, context, option) {
        const sql = this.translator.translateCount(entity, selection, option);
        const result = await this.connector.exec(sql, context.getCurrentTxnId());
        return result[0].cnt;
    }
    async begin(option) {
        const txn = await this.connector.startTransaction(option);
        return txn;
    }
    async commit(txnId) {
        await this.connector.commitTransaction(txnId);
    }
    async rollback(txnId) {
        await this.connector.rollbackTransaction(txnId);
    }
    connect() {
        this.connector.connect();
    }
    disconnect() {
        this.connector.disconnect();
    }
    async initialize(dropIfExists) {
        const schema = this.getSchema();
        for (const entity in schema) {
            const sqls = this.translator.translateCreateEntity(entity, { replace: dropIfExists });
            for (const sql of sqls) {
                await this.connector.exec(sql);
            }
        }
    }
}
exports.MysqlStore = MysqlStore;
