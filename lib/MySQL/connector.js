"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MySqlConnector = void 0;
var tslib_1 = require("tslib");
var mysql2_1 = tslib_1.__importDefault(require("mysql2"));
var uuid_1 = require("uuid");
var assert_1 = tslib_1.__importDefault(require("assert"));
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
        return tslib_1.__awaiter(this, void 0, void 0, function () {
            var connection_1;
            var _this = this;
            return tslib_1.__generator(this, function (_a) {
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
