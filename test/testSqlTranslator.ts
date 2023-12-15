import { describe, it } from 'mocha';
import { MySqlTranslator } from '../src/MySQL/translator';
import { EntityDict, storageSchema } from './test-app-domain';

describe('test MysqlTranslator', function() {
    this.timeout(100000);
    let translator: MySqlTranslator<EntityDict>;
    before(() => {
        translator = new MySqlTranslator(storageSchema);
    });

    it('test create', () => {
        const sql = translator.translateCreateEntity('token');
        // console.log(sql);
    });

    it('test insert', () => {
        const sql = translator.translateInsert('user', [{
            id: 'xcxc',
            name: 'xc',
            nickname: 'xcxcxc',
        }, {
            id: 'gggg',
            name: 'gg',
            nickname: 'gggggg',
        }]);
        // console.log(sql);
    });

    it('test select', () => {
        let sql = translator.translateSelect('token', {
            data: {
                id: 1,
                $$createAt$$: 1,
                userId: 1,
                mobile: {
                    id: 1,
                    mobile: 1,
                },
            },            
        });
        // console.log(sql);

        // distinct
        sql = translator.translateSelect('token', {
            data: {
                id: 1,
                $$createAt$$: 1,
                userId: 1,
                mobile: {
                    id: 1,
                    mobile: 1,
                },
            },
            distinct: true,
        });
        // console.log(sql);
    });
    
    it('test expression', () => {
        const sql = translator.translateSelect('token', {
            data: {
                id: 1,
                $expr: {
                    $dateDiff: [{
                        "#attr": '$$createAt$$',
                    }, new Date(), 'd'],
                }
            },
        });
        // console.log(sql);
    });

    it('test expression2', () => {  
        const sql = translator.translateSelect('token', {
            data: {
                id: 1,
                $expr: {
                    $concat: [
                        {
                            $year: {
                                '#attr': '$$createAt$$',
                            },
                        },
                        '-',
                        {
                            $month: {
                                '#attr': '$$createAt$$',
                            }
                        },
                        '-',
                        {
                            $dayOfMonth: {
                                '#attr': '$$createAt$$',
                            }
                        }
                    ],
                }
            },
        });
        // console.log(sql);
    })

    it('test aggregation', () => {
        let sql = translator.translateAggregate('token', {
            data: {
                '#max-1': {
                    user: {
                        nickname: 1,
                    },
                },
                '#aggr': {
                    email: {
                        email: 1,
                        $$createAt$$: 1,
                    },
                },
            },
            filter: {
                user: {
                    name: {
                        $includes: 'xc',
                    },
                },
            },
            indexFrom: 0,
            count: 10,
        });

        // distinct
        sql = translator.translateAggregate('token', {
            data: {
                '#count-1': {
                    user: {
                        nickname: 1,
                    },
                },
                distinct: true,
            },
            filter: {
                user: {
                    name: {
                        $includes: 'xc',
                    },
                },
            },
            indexFrom: 0,
            count: 10,
            distinct: true,
        });
        
        console.log(sql);
    });

    it('test or', () => {
        const sql = translator.translateSelect('token', {
            data: {
                id: 1,
                $$createAt$$: 1,
                userId: 1,
                mobile: {
                    id: 1,
                    mobile: 1,
                },
            },
            filter: {
                id: 'xc',
                $$createAt$$: 1,
                mobile: {
                    $or: [
                        {
                            id: 'mob',
                        },
                        {
                            mobile: '135',
                        }
                    ]
                },
            },
        });
         console.log(sql);
    })
});