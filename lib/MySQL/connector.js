"use strict";
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
exports.MySqlConnector = void 0;
var mysql2_1 = __importDefault(require("mysql2"));
var uuid_1 = require("uuid");
var assert_1 = __importDefault(require("assert"));
var MySqlConnector = /** @class */ (function () {
    function MySqlConnector(configuration) {
        this.configuration = configuration;
        this.txnDict = {};
    }
    MySqlConnector.prototype.connect = function () {
        this.pool = mysql2_1.default.createPool(this.configuration);
    };
    MySqlConnector.prototype.disconnect = function () {
        this.pool.end();
    };
    MySqlConnector.prototype.startTransaction = function (option) {
        var _this = this;
        return new Promise(function (resolve, reject) {
            _this.pool.getConnection(function (err, connection) {
                if (err) {
                    return reject(err);
                }
                var isolationLevel = (option || {}).isolationLevel;
                var startTxn = function () {
                    var sql = 'START TRANSACTION;';
                    connection.query(sql, function (err2) {
                        var _a;
                        if (err2) {
                            connection.release();
                            return reject(err2);
                        }
                        var id = (0, uuid_1.v4)();
                        Object.assign(_this.txnDict, (_a = {},
                            _a[id] = connection,
                            _a));
                        resolve(id);
                    });
                };
                if (isolationLevel) {
                    connection.query("SET TRANSACTION ISOLATION LEVEL ".concat(isolationLevel, ";"), function (err2) {
                        if (err2) {
                            connection.release();
                            return reject(err2);
                        }
                        startTxn();
                    });
                }
                else {
                    startTxn();
                }
            });
        });
    };
    MySqlConnector.prototype.exec = function (sql, txn) {
        return __awaiter(this, void 0, void 0, function () {
            var connection_1;
            var _this = this;
            return __generator(this, function (_a) {
                if (process.env.NODE_ENV === 'development') {
                    console.log(sql);
                }
                if (txn) {
                    connection_1 = this.txnDict[txn];
                    (0, assert_1.default)(connection_1);
                    return [2 /*return*/, new Promise(function (resolve, reject) {
                            connection_1.query(sql, function (err, result) {
                                if (err) {
                                    console.error("sql exec err: ".concat(sql), err);
                                    return reject(err);
                                }
                                resolve(result);
                            });
                        })];
                }
                else {
                    return [2 /*return*/, new Promise(function (resolve, reject) {
                            // if (process.env.DEBUG) {
                            //  console.log(sql);
                            //}
                            _this.pool.query(sql, function (err, result) {
                                if (err) {
                                    console.error("sql exec err: ".concat(sql), err);
                                    return reject(err);
                                }
                                resolve(result);
                            });
                        })];
                }
                return [2 /*return*/];
            });
        });
    };
    MySqlConnector.prototype.commitTransaction = function (txn) {
        var connection = this.txnDict[txn];
        (0, assert_1.default)(connection);
        return new Promise(function (resolve, reject) {
            connection.query('COMMIT;', function (err) {
                if (err) {
                    return reject(err);
                }
                connection.release();
                resolve();
            });
        });
    };
    MySqlConnector.prototype.rollbackTransaction = function (txn) {
        var connection = this.txnDict[txn];
        (0, assert_1.default)(connection);
        return new Promise(function (resolve, reject) {
            connection.query('ROLLBACK;', function (err) {
                if (err) {
                    return reject(err);
                }
                connection.release();
                resolve();
            });
        });
    };
    return MySqlConnector;
}());
exports.MySqlConnector = MySqlConnector;
