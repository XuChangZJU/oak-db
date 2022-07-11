import assert from 'assert';
import { UniversalContext } from 'oak-domain/lib/store/UniversalContext';
import { v4 } from 'uuid';
import { MysqlStore } from '../src/MySQL/store';
import { EntityDict, storageSchema } from './test-app-domain';

describe('test mysqlstore', function() {
    this.timeout(100000);
    let store: MysqlStore<EntityDict, UniversalContext<EntityDict>>;

    before(async () => {
        store = new MysqlStore(storageSchema, {
            host: 'localhost',
            database: 'oakdb',
            user: 'root',
            password: '',
            charset: 'utf8mb4_general_ci',
            connectionLimit: 20,
        });
        store.connect();
        await store.initialize(true);
    });

    it('test insert', async () => {
        const context = new UniversalContext(store);
        await store.operate('user', {
            action: 'create',
            data: [
                {
                    id: v4(),
                    name: 'xc',
                    nickname: 'xc',
                },
                {
                    id: v4(),
                    name: 'zz',
                    nickname: 'zzz',
                }
            ]
        }, context);
    });

    it('test cascade insert', async () => {
        const context = new UniversalContext(store);
        await store.operate('user', {
            action: 'create',
            data: {
                id: v4(),
                name: 'xxxc',
                nickname: 'ddd',
                token$player: [{
                    action: 'create',
                    data: {
                        id:  v4(),
                        env: {
                            type: 'web',
                        },
                        applicationId: v4(),
                        userId: v4(),
                        entity: 'mobile',
                        entityId: v4(),
                    }
                }]
            }
        }, context);
    });

    it('test update', async () => {
        const context = new UniversalContext(store);
        const tokenId = v4();
        await store.operate('user', {
            action: 'create',
            data: {
                id: v4(),
                name: 'xxxc',
                nickname: 'ddd',
                token$player: [{
                    action: 'create',
                    data: {
                        id:  tokenId,
                        env: {
                            type: 'web',
                        },
                        applicationId: v4(),
                        userId: v4(),
                        entity: 'mobile',
                        entityId: v4(),
                    }
                }]
            }
        }, context);
        await store.operate('token', {
            action: 'update',
            filter: {
                id: tokenId,
            },
            data: {
                player: {
                    action: 'activate',
                    data: {
                        name: 'xcxcxc0903'
                    },
                }
            }
        }, context);
    });

    it('test delete', async () => {
        const context = new UniversalContext(store);
        const tokenId = v4();
        await store.operate('user', {
            action: 'create',
            data: {
                id: v4(),
                name: 'xxxc',
                nickname: 'ddd',
                token$player: [{
                    action: 'create',
                    data: {
                        id:  tokenId,
                        env: {
                            type: 'web',
                        },
                        applicationId: v4(),
                        userId: v4(),
                        entity: 'mobile',
                        entityId: v4(),
                    }
                }]
            }
        }, context);
        await store.operate('token', {
            action: 'remove',
            filter: {
                id: tokenId,
            },
            data: {
                player: {
                    action: 'update',
                    data: {
                        name: 'xcxcxc0902'
                    },
                }
            }
        }, context);
    });


    it('test delete2', async () => {
        // 这个例子暂在mysql上过不去，先放着吧
        const context = new UniversalContext(store);
        const tokenId = v4();
        await store.operate('user', {
            action: 'create',
            data: {
                id: v4(),
                name: 'xxxc',
                nickname: 'ddd',
                token$player: [{
                    action: 'create',
                    data: {
                        id:  tokenId,
                        env: {
                            type: 'web',
                        },
                        applicationId: v4(),
                        userId: v4(),
                        entity: 'mobile',
                        entityId: v4(),
                    }
                }]
            }
        }, context);
        await store.operate('user', {
            action: 'remove',
            filter: {
                id: tokenId,
            },
            data: {
                ref: {
                    action: 'remove',
                    data: {},
                }
            },
        }, context);
    });

    it('[1.1]子查询', async () => {
        const context = new UniversalContext(store);

        await store.operate('user', {
            action: 'create',
            data: {
                id: v4(),
                name: 'xc',
                nickname: 'xc',
            }
        }, context);

        /**
         * 这个子查询没有跨结点的表达式，所以应该可以提前计算子查询的值
         * 这个可以跟一下store.ts中translateAttribute函数里$in的分支代码
         * by Xc
         */
        process.env.NODE_ENV = 'development';
        const rows = await store.select('user', {
            data: {
                id: 1,
                name: 1,
                nickname: 1,
            },
            filter: {
                id: {
                    $in: {
                        entity: 'token',
                        data: {
                            userId: 1,
                        },
                        filter: {
                            entity: 'mobile',
                        }
                    },
                }
            },
        }, context);
        process.env.NODE_ENV = undefined;
        // console.log(rows);
        assert(rows.result.length === 0);
    });

    it('[1.2]行内属性上的表达式', async () => {
        const context = new UniversalContext(store);

        const id = v4();
        await store.operate('user', {
            action: 'create',
            data: {
                id,
                name: 'xc',
                nickname: 'xc',
            }
        }, context);

        process.env.NODE_ENV = 'development';
        const { result: users } = await store.select('user', {
            data: {
                id: 1,
                name: 1,
                nickname: 1,
            },
            filter: {
                // '#id': 'node-123',
                $expr: {
                    $eq: [{
                        '#attr': 'name',
                    }, {
                        "#attr": 'nickname',
                    }]
                },
                id,
            },
        }, context);
        process.env.NODE_ENV = undefined;

        assert(users.length === 1);
    });

    it('[1.3]跨filter结点的表达式', async () => {
        const context = new UniversalContext(store);

        const id1 = v4();
        const id2 = v4();
        await store.operate('application', {
            action: 'create',
            data: [{
                id: id1,
                name: 'test',
                description: 'ttttt',
                type: 'web',
                config: {
                    type: 'web',
                    domain: 'http://www.tt.com',
                },
                system: {
                    action: 'create',
                    data: {
                        id: 'bbb',
                        name: 'systest',
                        description: 'aaaaa',
                        config: {},
                    }
                }
            }, {
                id: id2,
                name: 'test2',
                description: 'ttttt2',
                type: 'web',
                config: {
                    type: 'web',
                    domain: 'http://www.tt.com',
                },
                system: {
                    action: 'create',
                    data: {
                        id: 'ccc',
                        name: 'test2',
                        description: 'aaaaa2',
                        config: {},
                    }
                }
            }]
        }, context);

        const { result: applications } = await store.select('application', {
            data: {
                id: 1,
                name: 1,
                systemId: 1,
                system: {
                    id: 1,
                    name: 1,
                }
            },
            filter: {
                $expr: {
                    $startsWith: [
                        {
                            "#refAttr": 'name',
                            "#refId": 'node-1',
                        },
                        {
                            "#attr": 'name',
                        }
                    ]
                },
                system: {
                    "#id": 'node-1',
                },
                id: id2,
            },
            sorter: [
                {
                    $attr: {
                        system: {
                            name: 1,
                        }
                    },
                    $direction: 'asc',
                }
            ]
        }, context);
        console.log(applications);
        assert(applications.length === 1 && applications[0].id === id2);
    });


    it('[1.4]跨filter子查询的表达式', async () => {
        const context = new UniversalContext(store);

        await store.operate('application', {
            action: 'create',
            data: [{
                id: 'aaa',
                name: 'test',
                description: 'ttttt',
                type: 'web',
                config: {
                    type: 'web',
                    domain: 'http://www.tt.com',
                },
                system: {
                    action: 'create',
                    data: {
                        id: 'bbb',
                        name: 'systest',
                        description: 'aaaaa',
                        config: {},
                    }
                }
            }, {
                id: 'aaa2',
                name: 'test2',
                description: 'ttttt2',
                type: 'web',
                config: {
                    type: 'web',
                    domain: 'http://www.tt.com',
                },
                system: {
                    action: 'create',
                    data: {
                        id: 'ccc',
                        name: 'test2',
                        description: 'aaaaa2',
                        config: {},
                    }
                }
            }]
        }, context);

        process.env.NODE_ENV = 'development';
        let systems = await store.select('system', {
            data: {
                id: 1,
                name: 1,
            },
            filter: {
                "#id": 'node-1',
                id: {
                    $nin: {
                        entity: 'application',
                        data: {
                            systemId: 1,
                        },
                        filter: {
                            $expr: {
                                $eq: [
                                    {
                                        "#attr": 'name',
                                    },
                                    {
                                        '#refId': 'node-1',
                                        "#refAttr": 'name',
                                    }
                                ]
                            },
                            '#id': 'node-2',
                        }
                    },
                }
            },
            sorter: [
                {
                    $attr: {
                        name: 1,
                    },
                    $direction: 'asc',
                }
            ]
        }, context);
        assert(systems.result.length === 1 && systems.result[0].id === 'bbb');
        systems = await store.select('system', {
            data: {
                id: 1,
                name: 1,
            },
            filter: {
                "#id": 'node-1',
                id: {
                    $in: {
                        entity: 'application',
                        data: {
                            systemId: 1,
                        },
                        filter: {
                            $expr: {
                                $eq: [
                                    {
                                        "#attr": 'name',
                                    },
                                    {
                                        '#refId': 'node-1',
                                        "#refAttr": 'name',
                                    }
                                ]
                            },
                        }
                    },
                }
            },
            sorter: [
                {
                    $attr: {
                        name: 1,
                    },
                    $direction: 'asc',
                }
            ]
        }, context);
        process.env.NODE_ENV = undefined;
        assert(systems.result.length === 1 && systems.result[0].id === 'ccc');
    });

    it('[1.5]projection中的跨结点表达式', async () => {
        const context = new UniversalContext(store);

        await store.operate('application', {
            action: 'create',
            data: [{
                id: 'aaa',
                name: 'test',
                description: 'ttttt',
                type: 'web',
                config: {
                    type: 'web',
                    domain: 'http://www.tt.com',
                },
                system: {
                    action: 'create',
                    data: {
                        id: 'bbb',
                        name: 'systest',
                        description: 'aaaaa',
                        config: {},
                    }
                }
            }, {
                id: 'aaa2',
                name: 'test2',
                description: 'ttttt2',
                type: 'web',
                config: {
                    type: 'web',
                    domain: 'http://www.tt.com',
                },
                system: {
                    action: 'create',
                    data: {
                        id: 'ccc',
                        name: 'test2',
                        description: 'aaaaa2',
                        config: {},
                    }
                }
            }]
        }, context);

        let applications = await store.select('application', {
            data: {
                "#id": 'node-1',
                id: 1,
                name: 1,
                system: {
                    id: 1,
                    name: 1,
                    $expr: {
                        $eq: [
                            {
                                "#attr": 'name',
                            },
                            {
                                '#refId': 'node-1',
                                "#refAttr": 'name',
                            }
                        ]
                    },
                }
            },
        }, context);
        // console.log(applications);
        assert(applications.result.length === 2);
        applications.result.forEach(
            (app) => {
                assert(app.id === 'aaa' && app.system!.$expr === false 
                    || app.id === 'aaa2' && app.system!.$expr === true);
            }
        );

        const applications2 = await store.select('application', {
            data: {
                $expr: {
                    $eq: [
                        {
                            "#attr": 'name',
                        },
                        {
                            '#refId': 'node-1',
                            "#refAttr": 'name',
                        }
                    ]
                },
                id: 1,
                name: 1,
                system: {
                    "#id": 'node-1',
                    id: 1,
                    name: 1,
                }
            },
        }, context);
        console.log(applications2);
        // assert(applications.length === 2);
        applications2.result.forEach(
            (app) => {
                assert(app.id === 'aaa' && app.$expr === false
                    || app.id === 'aaa2' && app.$expr === true);
            }
        );
    });

    // 这个貌似目前支持不了 by Xc
    it('[1.6]projection中的一对多跨结点表达式', async () => {
        const context = new UniversalContext(store);

        await store.operate('system', {
            action: 'create',
            data: {
                id: 'bbb',
                name: 'test2',
                description: 'aaaaa',
                config: {},
                application$system: [{
                    action: 'create',
                    data: [
                        {
                            id: 'aaa',
                            name: 'test',
                            description: 'ttttt',
                            type: 'web',
                            config: {
                                type: 'web',
                                domain: 'http://www.tt.com',
                            },
                        },
                        {

                            id: 'aaa2',
                            name: 'test2',
                            description: 'ttttt2',
                            type: 'wechatMp',
                            config: {
                                type: 'web',
                                domain: 'http://www.tt.com',
                            },
                        }
                    ]
                }]
            }
        }, context);

        const systems = await store.select('system', {
            data: {
                "#id": 'node-1',
                id: 1,
                name: 1,
                application$system: {
                    $entity: 'application',
                    data: {
                        id: 1,
                        name: 1,
                        $expr: {
                            $eq: [
                                {
                                    "#attr": 'name',
                                },
                                {
                                    '#refId': 'node-1',
                                    "#refAttr": 'name',
                                }
                            ]
                        },
                        $expr2: {
                            '#refId': 'node-1',
                            "#refAttr": 'id',
                        }
                    }
                },
            },
        }, context);
        // console.log(systems);
        assert(systems.result.length === 1);    
        const [ system ] = systems.result;
        const { application$system: applications }  = system;
        assert(applications!.length === 2);
        applications!.forEach(
            (ele) => {
                assert(ele.id === 'aaa' && ele.$expr === false && ele.$expr2 === 'bbb'
                    || ele.id === 'aaa2' && ele.$expr === true && ele.$expr2 === 'bbb');
            }
        );
    });

    it('[1.7]事务性测试', async () => {
        const context = new UniversalContext(store);

        await store.operate('system', {
            action: 'create',
            data: {
                id: 'bbb',
                name: 'test2',
                description: 'aaaaa',
                config: {},
                application$system: [{
                    action: 'create',
                    data: [
                        {
                            id: 'aaa',
                            name: 'test',
                            description: 'ttttt',
                            type: 'web',
                            config: {
                                type: 'web',
                                domain: 'http://www.tt.com',
                            },
                        },
                        {

                            id: 'aaa2',
                            name: 'test2',
                            description: 'ttttt2',
                            type: 'wechatMp',
                            config: {
                                type: 'web',
                                domain: 'http://www.tt.com',
                            },
                        }
                    ]
                }]
            }
        }, context);

        await context.begin();
        const systems = await store.select('system', {
            data: {
                id: 1,
                name: 1,
                application$system: {
                    $entity: 'application',
                    data: {
                        id: 1,
                        name: 1,
                    }
                },
            },
        }, context);
        assert(systems.result.length === 1 && systems.result[0].application$system!.length === 2);
        
        await store.operate('application', {
            action: 'remove',
            data: {},
            filter: {
                id: 'aaa',
            }
        }, context);

        const systems2 = await store.select('system', {
            data: {
                id: 1,
                name: 1,
                application$system: {
                    $entity: 'application',
                    data: {
                        id: 1,
                        name: 1,
                    }
                },
            },
        }, context);
        assert(systems2.result.length === 1 && systems2.result[0].application$system!.length === 1);
        await context.rollback();

        const systems3 = await store.select('system', {
            data: {
                id: 1,
                name: 1,
                application$system: {
                    $entity: 'application',
                    data: {
                        id: 1,
                        name: 1,
                    }
                },
            },
        }, context);
        assert(systems3.result.length === 1 && systems3.result[0].application$system!.length === 2);
    });

    after(() => {
        store.disconnect();
    });
});