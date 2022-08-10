"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MysqlStore = void 0;
var CascadeStore_1 = require("oak-domain/lib/store/CascadeStore");
var connector_1 = require("./connector");
var translator_1 = require("./translator");
var lodash_1 = require("lodash");
var assert_1 = __importDefault(require("assert"));
var relation_1 = require("oak-domain/lib/store/relation");
function convertGeoTextToObject(geoText) {
    if (geoText.startsWith('POINT')) {
        var coord = geoText.match((/(\d|\.)+(?=\)|\s)/g));
        return {
            type: 'Point',
            coordinates: coord.map(function (ele) { return parseFloat(ele); }),
        };
    }
    else {
        throw new Error('only support Point now');
    }
}
var MysqlStore = /** @class */ (function (_super) {
    __extends(MysqlStore, _super);
    function MysqlStore(storageSchema, configuration) {
        var _this = _super.call(this, storageSchema) || this;
        _this.connector = new connector_1.MySqlConnector(configuration);
        _this.translator = new translator_1.MySqlTranslator(storageSchema);
        return _this;
    }
    MysqlStore.prototype.supportManyToOneJoin = function () {
        return true;
    };
    MysqlStore.prototype.supportMultipleCreate = function () {
        return true;
    };
    MysqlStore.prototype.formResult = function (entity, result) {
        var schema = this.getSchema();
        function resolveAttribute(entity2, r, attr, value) {
            var _a;
            var _b = schema[entity2], attributes = _b.attributes, view = _b.view;
            if (!view) {
                var i = attr.indexOf(".");
                if (i !== -1) {
                    var attrHead = attr.slice(0, i);
                    var attrTail = attr.slice(i + 1);
                    if (!r[attrHead]) {
                        r[attrHead] = {};
                    }
                    var rel = (0, relation_1.judgeRelation)(schema, entity2, attrHead);
                    (0, assert_1.default)(rel === 2 || typeof rel === 'string');
                    resolveAttribute(typeof rel === 'string' ? rel : attrHead, r[attrHead], attrTail, value);
                }
                else if (attributes[attr]) {
                    var type = attributes[attr].type;
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
                                r[attr] = "return ".concat(Buffer.from(value, 'base64').toString());
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
                (0, lodash_1.assign)(r, (_a = {},
                    _a[attr] = value,
                    _a));
            }
        }
        function formalizeNullObject(r, e) {
            var a2 = schema[e].attributes;
            var allowFormalize = true;
            for (var attr in r) {
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
            var result2 = {};
            for (var attr in r) {
                var value = r[attr];
                resolveAttribute(entity, result2, attr, value);
            }
            formalizeNullObject(result2, entity);
            return result2;
        }
        if (result instanceof Array) {
            return result.map(function (r) { return formSingleRow(r); });
        }
        return formSingleRow(result);
    };
    MysqlStore.prototype.selectAbjointRow = function (entity, selection, context, option) {
        return __awaiter(this, void 0, void 0, function () {
            var sql, result;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        sql = this.translator.translateSelect(entity, selection, option);
                        return [4 /*yield*/, this.connector.exec(sql, context.getCurrentTxnId())];
                    case 1:
                        result = _a.sent();
                        return [2 /*return*/, this.formResult(entity, result)];
                }
            });
        });
    };
    MysqlStore.prototype.updateAbjointRow = function (entity, operation, context, option) {
        return __awaiter(this, void 0, void 0, function () {
            var _a, translator, connector, action, txn, _b, data, sql, sql, sql;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        _a = this, translator = _a.translator, connector = _a.connector;
                        action = operation.action;
                        txn = context.getCurrentTxnId();
                        _b = action;
                        switch (_b) {
                            case 'create': return [3 /*break*/, 1];
                            case 'remove': return [3 /*break*/, 3];
                        }
                        return [3 /*break*/, 5];
                    case 1:
                        data = operation.data;
                        sql = translator.translateInsert(entity, data instanceof Array ? data : [data]);
                        return [4 /*yield*/, connector.exec(sql, txn)];
                    case 2:
                        _c.sent();
                        if (!(option === null || option === void 0 ? void 0 : option.notCollect)) {
                            context.opRecords.push({
                                a: 'c',
                                d: data,
                                e: entity,
                            });
                        }
                        return [2 /*return*/, data instanceof Array ? data.length : 1];
                    case 3:
                        sql = translator.translateRemove(entity, operation, option);
                        return [4 /*yield*/, connector.exec(sql, txn)];
                    case 4:
                        _c.sent();
                        // todo 这里对sorter和indexfrom/count的支持不完整
                        if (!(option === null || option === void 0 ? void 0 : option.notCollect)) {
                            context.opRecords.push({
                                a: 'r',
                                e: entity,
                                f: operation.filter,
                            });
                        }
                        return [2 /*return*/, 1];
                    case 5:
                        (0, assert_1.default)(!['select', 'download', 'stat'].includes(action));
                        sql = translator.translateUpdate(entity, operation, option);
                        return [4 /*yield*/, connector.exec(sql, txn)];
                    case 6:
                        _c.sent();
                        // todo 这里对sorter和indexfrom/count的支持不完整
                        if (!(option === null || option === void 0 ? void 0 : option.notCollect)) {
                            context.opRecords.push({
                                a: 'u',
                                e: entity,
                                d: operation.data,
                                f: operation.filter,
                            });
                        }
                        return [2 /*return*/, 1];
                }
            });
        });
    };
    MysqlStore.prototype.operate = function (entity, operation, context, params) {
        return __awaiter(this, void 0, void 0, function () {
            var action;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        action = operation.action;
                        (0, assert_1.default)(!['select', 'download', 'stat'].includes(action), '现在不支持使用select operation');
                        return [4 /*yield*/, this.cascadeUpdate(entity, operation, context, params)];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    MysqlStore.prototype.select = function (entity, selection, context, option) {
        return __awaiter(this, void 0, void 0, function () {
            var result;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.cascadeSelect(entity, selection, context, option)];
                    case 1:
                        result = _a.sent();
                        return [2 /*return*/, {
                                result: result,
                            }];
                }
            });
        });
    };
    MysqlStore.prototype.count = function (entity, selection, context, option) {
        return __awaiter(this, void 0, void 0, function () {
            var sql, result;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        sql = this.translator.translateCount(entity, selection, option);
                        return [4 /*yield*/, this.connector.exec(sql, context.getCurrentTxnId())];
                    case 1:
                        result = _a.sent();
                        return [2 /*return*/, result.count];
                }
            });
        });
    };
    MysqlStore.prototype.begin = function (option) {
        return __awaiter(this, void 0, void 0, function () {
            var txn;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.connector.startTransaction(option)];
                    case 1:
                        txn = _a.sent();
                        return [2 /*return*/, txn];
                }
            });
        });
    };
    MysqlStore.prototype.commit = function (txnId) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.connector.commitTransaction(txnId)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    MysqlStore.prototype.rollback = function (txnId) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.connector.rollbackTransaction(txnId)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    MysqlStore.prototype.connect = function () {
        this.connector.connect();
    };
    MysqlStore.prototype.disconnect = function () {
        this.connector.disconnect();
    };
    MysqlStore.prototype.initialize = function (dropIfExists) {
        return __awaiter(this, void 0, void 0, function () {
            var schema, _a, _b, _i, entity, sqls, _c, sqls_1, sql;
            return __generator(this, function (_d) {
                switch (_d.label) {
                    case 0:
                        schema = this.getSchema();
                        _a = [];
                        for (_b in schema)
                            _a.push(_b);
                        _i = 0;
                        _d.label = 1;
                    case 1:
                        if (!(_i < _a.length)) return [3 /*break*/, 6];
                        entity = _a[_i];
                        sqls = this.translator.translateCreateEntity(entity, { replace: dropIfExists });
                        _c = 0, sqls_1 = sqls;
                        _d.label = 2;
                    case 2:
                        if (!(_c < sqls_1.length)) return [3 /*break*/, 5];
                        sql = sqls_1[_c];
                        return [4 /*yield*/, this.connector.exec(sql)];
                    case 3:
                        _d.sent();
                        _d.label = 4;
                    case 4:
                        _c++;
                        return [3 /*break*/, 2];
                    case 5:
                        _i++;
                        return [3 /*break*/, 1];
                    case 6: return [2 /*return*/];
                }
            });
        });
    };
    return MysqlStore;
}(CascadeStore_1.CascadeStore));
exports.MysqlStore = MysqlStore;
