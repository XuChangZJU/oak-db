"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MysqlStore = void 0;
const CascadeStore_1 = require("oak-domain/lib/store/CascadeStore");
const connector_1 = require("./connector");
const translator_1 = require("./translator");
const lodash_1 = require("lodash");
const assert_1 = __importDefault(require("assert"));
const relation_1 = require("oak-domain/lib/store/relation");
function convertGeoTextToObject(geoText) {
    if (geoText.startsWith('POINT')) {
        const coord = geoText.match((/(\d|\.)+(?=\)|\s)/g));
        return {
            type: 'Point',
            coordinates: coord.map(ele => parseFloat(ele)),
        };
    }
    else {
        throw new Error('only support Point now');
    }
}
class MysqlStore extends CascadeStore_1.CascadeStore {
    connector;
    translator;
    constructor(storageSchema, configuration) {
        super(storageSchema);
        this.connector = new connector_1.MySqlConnector(configuration);
        this.translator = new translator_1.MySqlTranslator(storageSchema);
    }
    supportManyToOneJoin() {
        return true;
    }
    supportMultipleCreate() {
        return true;
    }
    formResult(entity, result) {
        const schema = this.getSchema();
        function resolveAttribute(entity2, r, attr, value) {
            const { attributes, view } = schema[entity2];
            if (!view) {
                const i = attr.indexOf(".");
                if (i !== -1) {
                    const attrHead = attr.slice(0, i);
                    const attrTail = attr.slice(i + 1);
                    if (!r[attrHead]) {
                        r[attrHead] = {};
                    }
                    const rel = (0, relation_1.judgeRelation)(schema, entity2, attrHead);
                    (0, assert_1.default)(rel === 2 || typeof rel === 'string');
                    resolveAttribute(typeof rel === 'string' ? rel : attrHead, r[attrHead], attrTail, value);
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
        function formalizeNullObject(r, e) {
            const { attributes: a2 } = schema[e];
            let allowFormalize = true;
            for (let attr in r) {
                if (typeof r[attr] === 'object' && a2[attr] && a2[attr].type === 'ref') {
                    if (formalizeNullObject(r[attr], a2[attr].ref)) {
                        r[attr] = null;
                    }
                    else {
                        allowFormalize = false;
                    }
                }
                else if (r[attr] !== null) {
                    allowFormalize = false;
                }
            }
            return allowFormalize;
        }
        function formSingleRow(r) {
            let result2 = {};
            for (let attr in r) {
                const value = r[attr];
                resolveAttribute(entity, result2, attr, value);
            }
            formalizeNullObject(result2, entity);
            return result2;
        }
        if (result instanceof Array) {
            return result.map(r => formSingleRow(r));
        }
        return formSingleRow(result);
    }
    async selectAbjointRow(entity, selection, context, params) {
        const sql = this.translator.translateSelect(entity, selection, params);
        const result = await this.connector.exec(sql, context.getCurrentTxnId());
        return this.formResult(entity, result);
    }
    async updateAbjointRow(entity, operation, context, params) {
        const { translator, connector } = this;
        const { action } = operation;
        const txn = context.getCurrentTxnId();
        switch (action) {
            case 'create': {
                const { data } = operation;
                const sql = translator.translateInsert(entity, data instanceof Array ? data : [data]);
                await connector.exec(sql, txn);
                context.opRecords.push({
                    a: 'c',
                    d: data,
                    e: entity,
                });
                return data instanceof Array ? data.length : 1;
            }
            case 'remove': {
                const sql = translator.translateRemove(entity, operation, params);
                await connector.exec(sql, txn);
                // todo 这里对sorter和indexfrom/count的支持不完整
                context.opRecords.push({
                    a: 'r',
                    e: entity,
                    f: operation.filter,
                });
                return 1;
            }
            default: {
                (0, assert_1.default)(!['select', 'download', 'stat'].includes(action));
                const sql = translator.translateUpdate(entity, operation, params);
                await connector.exec(sql, txn);
                // todo 这里对sorter和indexfrom/count的支持不完整
                context.opRecords.push({
                    a: 'u',
                    e: entity,
                    d: operation.data,
                    f: operation.filter,
                });
                return 1;
            }
        }
    }
    async operate(entity, operation, context, params) {
        const { action } = operation;
        (0, assert_1.default)(!['select', 'download', 'stat'].includes(action), '现在不支持使用select operation');
        return await this.cascadeUpdate(entity, operation, context, params);
    }
    async select(entity, selection, context, params) {
        const result = await this.cascadeSelect(entity, selection, context, params);
        return {
            result,
        };
    }
    async count(entity, selection, context, params) {
        const sql = this.translator.translateCount(entity, selection, params);
        const result = await this.connector.exec(sql, context.getCurrentTxnId());
        return result.count;
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
