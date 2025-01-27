import assert from 'assert';
import { describe, it } from 'mocha';
import { TestContext } from './Context';
import { v4 } from 'uuid';
import { MysqlStore } from '../src/MySQL/store';
import { EntityDict, storageSchema } from './test-app-domain';
import { filter } from 'lodash';
import { generateNewIdAsync } from 'oak-domain/lib/utils/uuid';
import { WebConfig } from './test-app-domain/Application/Schema';

describe('test mysqlstore', function () {
    this.timeout(100000);
    let store: MysqlStore<EntityDict, TestContext>;

    before(async () => {
        store = new MysqlStore(storageSchema, {
            host: 'localhost',
            database: 'oakdb',
            user: 'root',
            password: 'root',
            charset: 'utf8mb4_general_ci',
            connectionLimit: 20,
        });
        store.connect();
        await store.initialize(true);
    });

    it('test insert', async () => {
        const context = new TestContext(store);
        await context.begin();
        await store.operate('user', {
            id: v4(),
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
        }, context, {});
        await context.commit();
    });

    it('test cascade insert', async () => {
        const context = new TestContext(store);
        await store.operate('user', {
            id: v4(),
            action: 'create',
            data: {
                id: v4(),
                name: 'xxxc',
                nickname: 'ddd',
                token$player: [{
                    id: v4(),
                    action: 'create',
                    data: {
                        id: v4(),
                        env: {
                            type: 'web',
                        },
                        applicationId: v4(),
                        userId: v4(),
                        entity: 'mobile',
                        entityId: v4(),
                        refreshedAt: Date.now(),
                        value: v4(),
                    }
                }]
            } as EntityDict['user']['CreateSingle']['data']
        }, context, {});
    });

    it('test update', async () => {
        const context = new TestContext(store);
        const tokenId = v4();
        await context.begin();
        await store.operate('user', {
            id: v4(),
            action: 'create',
            data: {
                id: v4(),
                name: 'xxxc',
                nickname: 'ddd',
                token$player: [{
                    id: v4(),
                    action: 'create',
                    data: {
                        id: tokenId,
                        env: {
                            type: 'web',
                        },
                        applicationId: v4(),
                        userId: v4(),
                        entity: 'mobile',
                        entityId: v4(),
                        refreshedAt: Date.now(),
                        value: v4(),
                    }
                }]
            }
        } as EntityDict['user']['CreateSingle'], context, {});
        await store.operate('token', {
            id: v4(),
            action: 'update',
            filter: {
                id: tokenId,
            },
            data: {
                player: {
                    id: v4(),
                    action: 'activate',
                    data: {
                        name: 'xcxcxc0903'
                    },
                }
            }
        }, context, {});
        await context.commit();
    });

    it('test cascade update', async () => {
        const context = new TestContext(store);
        const userId = v4();
        const tokenId1 = v4();
        const tokenId2 = v4();
        await context.begin();
        await context.operate('user', {
            id: v4(),
            action: 'create',
            data: {
                id: userId,
                name: 'xxxc',
                nickname: 'ddd',
                token$player: [{
                    id: v4(),
                    action: 'create',
                    data: {
                        id: tokenId1,
                        env: {
                            type: 'web',
                        },
                        applicationId: v4(),
                        userId: v4(),
                        entity: 'mobile',
                        entityId: v4(),
                        refreshedAt: Date.now(),
                        value: v4(),
                    }
                }]
            }
        } as EntityDict['user']['CreateSingle'], {});

        await context.operate('token', {
            id: v4(),
            action: 'create',
            data: {
                id: tokenId2,
                env: {
                    type: 'web',
                },
                applicationId: v4(),
                userId: v4(),
                playerId: v4(),
                entity: 'mobile',
                entityId: v4(),
                refreshedAt: Date.now(),
                value: v4(),
            } as EntityDict['token']['CreateSingle']['data'],
        }, {});

        await context.commit();

        // cascade update token of userId
        await context.operate('user', {
            id: v4(),
            action: 'update',
            data: {
                name: 'xc',
                token$player: [{
                    id: v4(),
                    action: 'update',
                    data: {
                        entity: 'email',
                    }
                }]
            },
            filter: {
                id: userId,
            },
        }, {});

        const [row] = await context.select('token', {
            data: {
                id: 1,
                entity: 1,
                entityId: 1,
            },
            filter: {
                id: tokenId2,
            }
        }, {});

        assert(row.entity === 'mobile');
    });

    it('test delete', async () => {
        const context = new TestContext(store);
        const tokenId = v4();
        await context.begin();
        await store.operate('user', {
            id: v4(),
            action: 'create',
            data: {
                id: v4(),
                name: 'xxxc',
                nickname: 'ddd',
                token$player: [{
                    id: v4(),
                    action: 'create',
                    data: {
                        id: tokenId,
                        env: {
                            type: 'server',
                        },
                        applicationId: v4(),
                        userId: v4(),
                        entity: 'mobile',
                        entityId: v4(),
                        refreshedAt: Date.now(),
                        value: v4(),
                    }
                }]
            }
        }, context, {});
        await store.operate('token', {
            id: v4(),
            action: 'remove',
            filter: {
                id: tokenId,
            },
            data: {
                player: {
                    id: v4(),
                    action: 'update',
                    data: {
                        name: 'xcxcxc0902'
                    },
                }
            }
        }, context, {});
        await context.commit();
    });


    it('test delete2', async () => {
        // 这个例子暂在mysql上过不去，先放着吧
        const context = new TestContext(store);
        const tokenId = v4();
        await context.begin();
        await store.operate('user', {
            id: v4(),
            action: 'create',
            data: {
                id: v4(),
                name: 'xxxc',
                nickname: 'ddd',
                token$player: [{
                    id: v4(),
                    action: 'create',
                    data: {
                        id: tokenId,
                        env: {
                            type: 'server',
                        },
                        applicationId: v4(),
                        userId: v4(),
                        entity: 'mobile',
                        entityId: v4(),
                        refreshedAt: Date.now(),
                        value: v4(),
                    }
                }]
            }
        }, context, {});
        await store.operate('user', {
            id: v4(),
            action: 'remove',
            filter: {
                id: tokenId,
            },
            data: {
                ref: {
                    id: await generateNewIdAsync(),
                    action: 'remove',
                    data: {},
                }
            },
        }, context, {});
        await context.commit();
    });

    it('test decimal', async () => {
        const id = v4();
        const context = new TestContext(store);
        await context.begin();
        await store.operate('house', {
            id: v4(),
            action: 'create',
            data: [
                {
                    id,
                    areaId: 'xc',
                    ownerId: 'xc',
                    district: '杭州',
                    size: 77.5,
                },
            ]
        }, context, {});
        await context.commit();

        const [house] = await store.select('house', {
            data: {
                id: 1,
                size: 1,
            },
            filter: {
                id,
            },
        }, context, {});
        assert(typeof house.size === 'number');
    });


    it('[1.1]子查询', async () => {
        const context = new TestContext(store);
        await context.begin();
        await store.operate('user', {
            id: v4(),
            action: 'create',
            data: {
                id: v4(),
                name: 'xc',
                nickname: 'xc',
            }
        }, context, {});

        process.env.NODE_ENV = 'development';
        const rows = await store.select('user', {
            data: {
                id: 1,
                name: 1,
                nickname: 1,
            },
            filter: {
                token$user: {
                }
            },
        }, context, {});
        process.env.NODE_ENV = undefined;
        // console.log(rows);
        assert(rows.length === 0);
        await context.commit();
    });

    it('[1.2]行内属性上的表达式', async () => {
        const context = new TestContext(store);
        await context.begin();
        const id = v4();
        await store.operate('user', {
            id: v4(),
            action: 'create',
            data: {
                id,
                name: 'xc',
                nickname: 'xc',
            }
        }, context, {});

        const id2 = v4();
        await store.operate('user', {
            id: v4(),
            action: 'create',
            data: {
                id: id2,
                name: 'xzw',
                nickname: 'xzw22',
            }
        }, context, {});

        process.env.NODE_ENV = 'development';
        const users = await store.select('user', {
            data: {
                id: 1,
                name: 1,
                nickname: 1,
            },
            filter: {
                id: {
                    $in: [id, id2],
                },
                $expr: {
                    $eq: [{
                        '#attr': 'name',
                    }, {
                        "#attr": 'nickname',
                    }]
                },
            },
        }, context, {});
        const users2 = await store.select('user', {
            data: {
                id: 1,
                name: 1,
                nickname: 1,
            },
            filter: {
                id: {
                    $in: [id, id2],
                },
                $expr: {
                    $eq: [{
                        $mod: [
                            {
                                '#attr': '$$seq$$',
                            },
                            2,
                        ],
                    }, 0]
                },
            },
        }, context, {});
        process.env.NODE_ENV = undefined;

        assert(users.length === 1);
        assert(users2.length === 1);
        await context.commit();
    });

    it('[1.3]跨filter结点的表达式', async () => {
        const context = new TestContext(store);

        const id1 = v4();
        const id2 = v4();
        await context.begin();
        await store.operate('application', {
            id: v4(),
            action: 'create',
            data: [{
                id: id1,
                name: 'test',
                description: 'ttttt',
                type: 'web',
                config: {
                    type: 'web',
                    passport: [],
                },
                system: {
                    id: v4(),
                    action: 'create',
                    data: {
                        id: 'bbb',
                        name: 'systest',
                        description: 'aaaaa',
                        config: {},
                        folder: '/systest',
                        platformId: 'platform-111',
                    } as EntityDict['system']['CreateSingle']['data']
                }
            }, {
                id: id2,
                name: 'test2',
                description: 'ttttt2',
                type: 'web',
                config: {
                    type: 'web',
                    passport: [],
                },
                system: {
                    id: v4(),
                    action: 'create',
                    data: {
                        id: 'ccc',
                        name: 'test2',
                        description: 'aaaaa2',
                        config: {
                            App: {},
                        },
                        folder: '/test2',
                        platformId: 'platform-111',
                    }
                }
            }]
        }, context, {});

        const applications = await store.select('application', {
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
        }, context, {});
        console.log(applications);
        assert(applications.length === 1 && applications[0].id === id2);
        await context.commit();
    });


    it('[1.4]跨filter子查询的表达式', async () => {
        const context = new TestContext(store);
        await context.begin();

        await store.operate('application', {
            id: v4(),
            action: 'create',
            data: [{
                id: 'aaaa',
                name: 'test',
                description: 'ttttt',
                type: 'web',
                config: {
                    type: 'web',
                    passport: [],
                },
                system: {
                    id: v4(),
                    action: 'create',
                    data: {
                        id: 'bbbb',
                        name: 'systest',
                        description: 'aaaaa',
                        config: {
                            App: {},
                        },
                        folder: '/systest',
                        platformId: 'platform-111',
                    }
                }
            }, {
                id: 'aaaa2',
                name: 'test2',
                description: 'ttttt2',
                type: 'web',
                config: {
                    type: 'web',
                    passport: [],
                },
                system: {
                    id: v4(),
                    action: 'create',
                    data: {
                        id: 'cccc',
                        name: 'test2',
                        description: 'aaaaa2',
                        config: {
                            App: {},
                        },
                        folder: '/test2',
                        platformId: 'platform-111',
                    }
                }
            }]
        }, context, {});

        process.env.NODE_ENV = 'development';
        let systems = await store.select('system', {
            data: {
                id: 1,
                name: 1,
            },
            filter: {
                "#id": 'node-1',
                $and: [
                    {
                        application$system: {
                            '#sqp': 'not in',
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
                    }, {
                        id: {
                            $in: ['bbbb', 'cccc'],
                        }
                    }
                ]
            },
            sorter: [
                {
                    $attr: {
                        name: 1,
                    },
                    $direction: 'asc',
                }
            ]
        }, context, {});
        assert(systems.length === 1 && systems[0].id === 'bbbb');
        systems = await store.select('system', {
            data: {
                id: 1,
                name: 1,
            },
            filter: {
                "#id": 'node-1',
                $and: [
                    {
                        application$system: {
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
                        },
                    },
                    {
                        id: {
                            $in: ['bbbb', 'cccc'],
                        },
                    }
                ]
            },
            sorter: [
                {
                    $attr: {
                        name: 1,
                    },
                    $direction: 'asc',
                }
            ]
        }, context, {});
        process.env.NODE_ENV = undefined;
        assert(systems.length === 1 && systems[0].id === 'cccc');
        await context.commit();
    });

    it('[1.5]projection中的跨结点表达式', async () => {
        const context = new TestContext(store);
        await context.begin();

        await store.operate('application', {
            id: v4(),
            action: 'create',
            data: [{
                id: 'aaa5',
                name: 'test',
                description: 'ttttt',
                type: 'web',
                config: {
                    type: 'web',
                    passport: [],
                },
                system: {
                    id: v4(),
                    action: 'create',
                    data: {
                        id: 'bbb5',
                        name: 'systest',
                        description: 'aaaaa',
                        config: {
                            App: {},
                        },
                        folder: '/systest',
                        platformId: 'platform-111',
                    }
                }
            }, {
                id: 'aaa5-2',
                name: 'test2',
                description: 'ttttt2',
                type: 'web',
                config: {
                    type: 'web',
                    passport: [],
                },
                system: {
                    id: v4(),
                    action: 'create',
                    data: {
                        id: 'ccc5',
                        name: 'test2',
                        description: 'aaaaa2',
                        config: {
                            App: {},
                        },
                        folder: '/test2',
                        platformId: 'platform-111',
                    } as EntityDict['system']['CreateSingle']['data'],
                }
            }]
        }, context, {});

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
            filter: {
                id: {
                    $in: ['aaa5', 'aaa5-2'],
                },
            },
        }, context, {});
        // console.log(applications);
        assert(applications.length === 2);
        applications.forEach(
            (app) => {
                assert(app.id === 'aaa5' && !(app.system!.$expr)
                    || app.id === 'aaa5-2' && !!(app.system!.$expr));
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
            filter: {
                id: {
                    $in: ['aaa5', 'aaa5-2'],
                },
            },
        }, context, {});
        // console.log(applications2);
        // assert(applications.length === 2);
        applications2.forEach(
            (app) => {
                assert(app.id === 'aaa5' && !(app.$expr)
                    || app.id === 'aaa5-2' && !!(app.$expr));
            }
        );
        await context.commit();
    });

    // 这个貌似目前支持不了 by Xc
    it('[1.6]projection中的一对多跨结点表达式', async () => {
        const context = new TestContext(store);
        await context.begin();

        await store.operate('system', {
            id: v4(),
            action: 'create',
            data: {
                id: 'bbb6',
                name: 'test2',
                description: 'aaaaa',
                config: {
                    App: {},
                },
                folder: '/test2',
                platformId: 'platform-111',
                application$system: [{
                    id: v4(),
                    action: 'create',
                    data: {
                        id: 'aaa6',
                        name: 'test',
                        description: 'ttttt',
                        type: 'web',
                        config: {
                            type: 'web',
                            passport: [],
                        },
                    }
                },
                {
                    id: v4(),
                    action: 'create',
                    data: {
                        id: 'aaa6-2',
                        name: 'test2',
                        description: 'ttttt2',
                        type: 'wechatMp',
                        config: {
                            type: 'web',
                            passport: [],
                        },
                    }
                }]
            }
        } as EntityDict['system']['CreateSingle'], context, {});

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
        }, context, {});
        // console.log(systems);
        assert(systems.length === 1);
        const [system] = systems;
        const { application$system: applications } = system;
        assert(applications!.length === 2);
        applications!.forEach(
            (ele) => {
                assert(ele.id === 'aaa' && ele.$expr === false && ele.$expr2 === 'bbb'
                    || ele.id === 'aaa2' && ele.$expr === true && ele.$expr2 === 'bbb');
            }
        );
    });

    it('[1.7]事务性测试', async () => {
        const context = new TestContext(store);

        await context.begin();
        const systemId = v4();
        await store.operate('system', {
            id: v4(),
            action: 'create',
            data: {
                id: systemId,
                name: 'test2',
                description: 'aaaaa',
                config: {
                    App: {},
                },
                folder: '/test2',
                platformId: 'platform-111',
                application$system: [{
                    id: v4(),
                    action: 'create',
                    data: {
                        id: 'aaa7',
                        name: 'test',
                        description: 'ttttt',
                        type: 'web',
                        config: {
                            type: 'web',
                            passport: [],
                        },
                    }
                }, {
                    id: v4(),
                    action: 'create',
                    data: {
                        id: 'aaa7-2',
                        name: 'test2',
                        description: 'ttttt2',
                        type: 'wechatMp',
                        config: {
                            type: 'web',
                            passport: [],
                        },
                    }
                }]
            }
        }, context, {});
        await context.commit();

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
                        systemId: 1,
                    }
                },
            },
            filter: {
                id: systemId,
            },
        }, context, {});
        assert(systems.length === 1 && systems[0].application$system!.length === 2);

        await store.operate('application', {
            id: v4(),
            action: 'remove',
            data: {},
            filter: {
                id: 'aaa7',
            }
        }, context, {});

        const systems2 = await store.select('system', {
            data: {
                id: 1,
                name: 1,
                application$system: {
                    $entity: 'application',
                    data: {
                        id: 1,
                        name: 1,
                        systemId: 1,
                    }
                },
            },
            filter: {
                id: systemId,
            },
        }, context, {});
        assert(systems2.length === 1 && systems2[0].application$system!.length === 1);
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
                        systemId: 1,
                    }
                },
            },
            filter: {
                id: systemId,
            },
        }, context, {});
        assert(systems3.length === 1 && systems3[0].application$system!.length === 2);
    });

    it('[1.8]aggregation', async () => {
        const context = new TestContext(store);
        await context.begin();

        const systemId1 = v4();
        const systemId2 = v4();
        await store.operate('system', {
            id: v4(),
            action: 'create',
            data: [
                {
                    id: systemId1,
                    name: 'test2',
                    description: 'aaaaa',
                    config: {
                        App: {},
                    },
                    folder: '/test2',
                    platformId: 'platform-111',
                    application$system: [{
                        id: v4(),
                        action: 'create',
                        data: {
                            id: v4(),
                            name: 'test',
                            description: 'ttttt',
                            type: 'web',
                            config: {
                                type: 'web',
                                passport: [],
                            },
                        }
                    },
                    {
                        id: v4(),
                        action: 'create',
                        data: {
                            id: v4(),
                            name: 'test2',
                            description: 'ttttt2',
                            type: 'wechatMp',
                            config: {
                                type: 'web',
                                passport: [],
                            },
                        }
                    }]
                },
                {
                    id: systemId2,
                    name: 'test2',
                    description: 'aaaaa',
                    config: {
                        App: {},
                    },
                    folder: '/test2',
                    platformId: 'platform-111',
                    application$system: [{
                        id: v4(),
                        action: 'create',
                        data: {
                            id: v4(),
                            name: 'test',
                            description: 'ttttt',
                            type: 'web',
                            config: {
                                type: 'web',
                                passport: [],
                            },
                        }
                    },
                    {
                        id: v4(),
                        action: 'create',
                        data: {
                            id: v4(),
                            name: 'test2',
                            description: 'ttttt2',
                            type: 'wechatMp',
                            config: {
                                type: 'web',
                                passport: [],
                            },
                        }
                    }]
                }
            ]
        } as EntityDict['system']['CreateMulti'], context, {});
        await context.commit();

        await context.begin();
        const result = await store.aggregate('application', {
            data: {
                '#aggr': {
                    system: {
                        id: 1,
                    },
                },
                '#count-1': {
                    id: 1,
                }
            },
            filter: {
                systemId: {
                    $in: [systemId1, systemId2],
                },
            },
        }, context, {});
        await context.commit();
        // console.log(result);
        assert(result.length === 2);
        result.forEach(
            (row) => assert(row['#count-1'] === 2)
        );
    });

    it('[1.9]test + aggregation', async () => {
        const context = new TestContext(store);
        await context.begin();

        const systemId1 = v4();
        const systemId2 = v4();
        await store.operate('system', {
            id: v4(),
            action: 'create',
            data: [
                {
                    id: systemId1,
                    name: 'test2',
                    description: 'aaaaa',
                    config: {
                        App: {},
                    },
                    folder: '/test2',
                    platformId: 'platform-111',
                    application$system: [{
                        id: v4(),
                        action: 'create',
                        data: {
                            id: v4(),
                            name: 'test',
                            description: 'ttttt',
                            type: 'web',
                            config: {
                                type: 'web',
                                passport: [],
                            },
                        }
                    },
                    {
                        id: v4(),
                        action: 'create',
                        data: {
                            id: v4(),
                            name: 'test2',
                            description: 'ttttt2',
                            type: 'wechatMp',
                            config: {
                                type: 'web',
                                passport: [],
                            },
                        }
                    }]
                },
                {
                    id: systemId2,
                    name: 'test2',
                    description: 'aaaaa',
                    config: {
                        App: {},
                    },
                    folder: '/test2',
                    platformId: 'platform-111',
                    application$system: [{
                        id: v4(),
                        action: 'create',
                        data: {
                            id: v4(),
                            name: 'test',
                            description: 'ttttt',
                            type: 'web',
                            config: {
                                type: 'web',
                                passport: [],
                            },
                        }
                    },
                    {
                        id: v4(),
                        action: 'create',
                        data: {
                            id: v4(),
                            name: 'test2',
                            description: 'ttttt2',
                            type: 'wechatMp',
                            config: {
                                type: 'web',
                                passport: [],
                            },
                        }
                    }]
                }
            ]
        } as EntityDict['system']['CreateMulti'], context, {});
        await context.commit();

        await context.begin();
        const result = await store.select('system', {
            data: {
                id: 1,
                name: 1,
                application$system$$aggr: {
                    $entity: 'application',
                    data: {
                        '#aggr': {
                            system: {
                                id: 1,
                            },
                        },
                        '#count-1': {
                            id: 1,
                        }
                    },
                },
            },
            filter: {
                id: {
                    $in: [systemId1, systemId2],
                },
            },
        }, context, {});
        await context.commit();
        // console.log(result);
        assert(result.length === 2);
        result.forEach(
            (row) => assert(row.application$system$$aggr?.length === 1 && row.application$system$$aggr[0]['#count-1'] === 2)
        );
    });

    it('[1.10]json insert/select', async () => {
        const context = new TestContext(store);
        await context.begin();

        let id = await generateNewIdAsync();
        await context.operate('application', {
            id: await generateNewIdAsync(),
            action: 'create',
            data: {
                id,
                name: 'xuchang',
                description: 'tt',
                type: 'web',
                systemId: 'system',
                config: {
                    type: 'web',
                    passport: ['email', 'mobile'],
                },
            }
        }, {});

        let result = await context.select('application', {
            data: {
                id: 1,
                name: 1,
                config: {
                    passport: [undefined, 1],
                    wechat: {
                        appId: 1,
                    }
                },
            },
            filter: {
                id,
            },
        }, {});
        // console.log(JSON.stringify(result));

        id = await generateNewIdAsync();
        await context.operate('application', {
            id: await generateNewIdAsync(),
            action: 'create',
            data: {
                id,
                name: 'xuchang',
                description: 'tt',
                type: 'web',
                systemId: 'system',
                config: {
                    type: 'web',
                    wechat: {
                        appId: 'aaaa\\nddddd',
                        appSecret: '',
                    },
                    passport: ['email', 'mobile'],
                },
            }
        }, {});

        result = await context.select('application', {
            data: {
                id: 1,
                name: 1,
                config: {
                    passport: [undefined, 1],
                    wechat: {
                        appId: 1,
                    }
                },
            },
            filter: {
                id,
            },
        }, {});
        console.log((result[0].config as WebConfig)!.wechat!.appId);
    });

    it('[1.11]json as filter', async () => {
        const context = new TestContext(store);
        await context.begin();

        const id = await generateNewIdAsync();
        await store.operate('oper', {
            id: await generateNewIdAsync(),
            action: 'create',
            data: {
                id,
                action: 'test',
                data: {
                    name: 'xc',
                    books: [{
                        title: 'mathmatics',
                        price: 1,
                    }, {
                        title: 'english',
                        price: 2,
                    }],
                },
                targetEntity: 'bbb',
                bornAt: 111,
            }
        }, context, {});

        const row = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    books: [undefined, {
                        title: 1,
                        price: 1,
                    }],
                },
            },
            filter: {
                id,
                data: {
                    name: 'xc',
                }
            }
        }, context, {});
        const row2 = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    books: [undefined, {
                        title: 1,
                        price: 1,
                    }],
                },
            },
            filter: {
                id,
                data: {
                    name: 'xc2',
                }
            }
        }, context, {});

        await context.commit();
        // console.log(JSON.stringify(row));
        assert(row.length === 1, JSON.stringify(row));
        assert(row2.length === 0, JSON.stringify(row2));
    });


    it('[1.11.2]json filter on top level', async () => {
        const context = new TestContext(store);
        await context.begin();

        const id = await generateNewIdAsync();
        await store.operate('actionAuth', {
            id: await generateNewIdAsync(),
            action: 'create',
            data: {
                id,
                pathId: await generateNewIdAsync(),
                deActions: ['1.12'],
            }
        }, context, {});

        await context.commit();

        const row = await store.select('actionAuth', {
            data: {
                id: 1,
                deActions: 1,
            },
            filter: {
                id,
                deActions: {
                    $overlaps: '1.12',
                }
            }
        }, context, {});


        const row2 = await store.select('actionAuth', {
            data: {
                id: 1,
                deActions: 1,
            },
            filter: {
                id,
                deActions: {
                    $contains: ['1.13333'],
                }
            }
        }, context, {});
        // console.log(JSON.stringify(row));
        assert(row.length === 1, JSON.stringify(row));
        console.log(JSON.stringify(row));
        assert(row2.length === 0, JSON.stringify(row2));

        const row3 = await store.select('actionAuth', {
            data: {
                id: 1,
                deActions: 1,
            },
            filter: {
                id,
                deActions: {
                    $exists: true,
                }
            }
        }, context, {});

        assert(row3.length === 1);
        const row4 = await store.select('actionAuth', {
            data: {
                id: 1,
                deActions: 1,
            },
            filter: {
                id,
                deActions: {
                    $exists: false,
                }
            }
        }, context, {});

        assert(row4.length === 0);
    });

    it('[1.11.3]json escape', async () => {
        const context = new TestContext(store);
        await context.begin();

        const id = await generateNewIdAsync();
        await store.operate('oper', {
            id: await generateNewIdAsync(),
            action: 'create',
            data: {
                id,
                action: 'test',
                data: {
                    $or: [{
                        name: 'xc',
                    }, {
                        name: {
                            $includes: 'xc',
                        }
                    }],
                },
                targetEntity: 'bbb',
                bornAt: 123,
            }
        }, context, {});

        const row = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    price: 1,
                },
            },
            filter: {
                id,
                data: {
                    '.$or': {
                        $contains: {
                            name: 'xc',
                        },
                    },
                },
            }
        }, context, {});

        process.env.NODE_ENV = 'development';
        const row2 = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    price: 1,
                },
            },
            filter: {
                id,
                data: {
                    '.$or': [
                        {
                            name: 'xc',
                        },
                        {
                            name: {
                               '.$includes': 'xc',
                            }
                        }
                    ],
                },
            }
        }, context, {});        

        await context.commit();
        assert(row.length === 1);
        assert(row2.length === 1);
    });

    it('[1.12]complicated json filter', async () => {
        const context = new TestContext(store);
        await context.begin();

        const id = await generateNewIdAsync();
        await store.operate('oper', {
            id: await generateNewIdAsync(),
            action: 'create',
            data: {
                id,
                action: 'test',
                data: {
                    name: 'xcc',
                    price: [100, 400, 1000],
                },
                targetEntity: 'bbb',
                bornAt: 123,
            }
        }, context, {});

        const row = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    price: 1,
                },
            },
            filter: {
                data: {
                    price: [undefined, 400],
                }
            }
        }, context, {});

        const row2 = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    price: 1,
                },
            },
            filter: {
                data: {
                    price: [undefined, 200],
                }
            }
        }, context, {});

        const row3 = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    price: 1,
                },
            },
            filter: {
                data: {
                    price: [undefined, {
                        $gt: 300,
                    }],
                }
            }
        }, context, {});

        const row4 = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    price: 1,
                },
            },
            filter: {
                data: {
                    price: {
                        $contains: [200, 500],
                    },
                }
            }
        }, context, {});

        const row5 = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    price: 1,
                },
            },
            filter: {
                data: {
                    price: {
                        $contains: [100, 400],
                    },
                }
            }
        }, context, {});

        const row6 = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    price: 1,
                },
            },
            filter: {
                data: {
                    price: {
                        $contains: ['xc'],
                    },
                }
            }
        }, context, {});

        const row7 = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    price: 1,
                },
            },
            filter: {
                data: {
                    name: {
                        $includes: 'xc',
                    },
                    price: {
                        $overlaps: [200, 400, 800],
                    },
                }
            }
        }, context, {});

        /**
         * 带$or的查询
         */
        process.env.NODE_ENV = 'development';
        const row8 = await store.select('oper', {
            data: {
                id: 1,
                data: {
                    name: 1,
                    price: 1,
                },
            },
            filter: {
                data: {
                    $or: [
                        {
                            name: {
                                $includes: 'xc',
                            },
                        },
                        {
                            name: {
                                $includes: 'xzw',
                            }
                        }
                    ],
                    price: {
                        $overlaps: [200, 400, 800],
                    },
                }
            }
        }, context, {});

        await context.commit();
        assert(row.length === 1);
        assert(row2.length === 0);
        assert(row3.length === 1);
        assert(row4.length === 0);
        assert(row5.length === 1);
        assert(row6.length === 0);
        assert(row7.length === 1);
        assert(row8.length === 1);
        // console.log(JSON.stringify(row7));
    });

    it('[1.13]sub query predicate', async () => {
        const context = new TestContext(store);
        await context.begin();
        const id1 = await generateNewIdAsync();
        const id2 = await generateNewIdAsync();
        const id3 = await generateNewIdAsync();
        const systems: EntityDict['system']['CreateSingle']['data'][] = [
            {
                id: id1,
                name: 'test1.13-1',
                description: 'aaaaa',
                config: {
                    App: {},
                },
                folder: '/test2',
                platformId: 'platform-111',
                application$system: [{
                    id: v4(),
                    action: 'create',
                    data: {
                        id: v4(),
                        name: 'test1.13-1-1',
                        description: 'ttttt',
                        type: 'web',
                        config: {
                            type: 'web',
                            passport: [],
                        },
                    }
                },
                {
                    id: v4(),
                    action: 'create',
                    data: {
                        id: v4(),
                        name: 'test1.13-1-2',
                        description: 'ttttt2',
                        type: 'wechatMp',
                        config: {
                            type: 'web',
                            passport: [],
                        },
                    }
                }]
            },
            {
                id: id2,
                name: 'test1.13-2',
                description: 'aaaaa',
                config: {
                    App: {},
                },
                folder: '/test2',
                platformId: 'platform-111',
                application$system: [{
                    id: v4(),
                    action: 'create',
                    data: {
                        id: v4(),
                        name: 'test1.13-2-1',
                        description: 'ttttt',
                        type: 'web',
                        config: {
                            type: 'web',
                            passport: [],
                        },
                    }
                },
                {
                    id: v4(),
                    action: 'create',
                    data: {
                        id: v4(),
                        name: 'test1.13-2-2',
                        description: 'ttttt2',
                        type: 'wechatMp',
                        config: {
                            type: 'web',
                            passport: [],
                        },
                    }
                }]
            },
            {

                id: id3,
                name: 'test1.13-3',
                description: 'aaaaa',
                config: {
                    App: {},
                },
                folder: '/test2',
                platformId: 'platform-111',
            }
        ];

        await context.operate('system', {
            id: v4(),
            action: 'create',
            data: systems,
        }, {});

        await context.commit();

        const r1 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: id1,
                application$system: {
                }
            }
        }, {});
        assert(r1.length === 1);

        const r2 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: id1,
                application$system: {
                    '#sqp': 'not in',
                },
            },
        }, {});
        assert(r2.length === 0);

        const r22 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: id1,
                application$system: {
                    name: {
                        $startsWith: 'test1.13-2',
                    },
                    '#sqp': 'not in',
                },
            },
        }, {});
        assert(r22.length === 1);

        const r23 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: id1,
                application$system: {
                    name: {
                        $startsWith: 'test1.13-2',
                    },
                    '#sqp': 'all',
                },
            },
        }, {});
        assert(r23.length === 0);

        const r24 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: id1,
                application$system: {
                    name: {
                        $startsWith: 'test1.13-2',
                    },
                    '#sqp': 'not all',
                },
            },
        }, {});
        assert(r24.length === 1);

        const r3 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: id1,
                application$system: {
                    '#sqp': 'all',
                },
            },
        }, {});
        assert(r3.length === 0);


        const r4 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: id1,
                application$system: {
                    '#sqp': 'not all',
                },
            },
        }, {});
        assert(r4.length === 1);

        const r5 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: {
                    $in: [id1, id2, id3],
                },
                application$system: {
                },
            },
        }, {});
        assert(r5.length === 2);
        assert(r5.map(ele => ele.id).includes(id1));
        assert(r5.map(ele => ele.id).includes(id2));

        const r6 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: {
                    $in: [id1, id2, id3],
                },
                application$system: {
                    '#sqp': 'not in',
                },
            },
        }, {});
        assert(r6.length === 1);
        assert(r6.map(ele => ele.id).includes(id3));

        const r7 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: {
                    $in: [id1, id2, id3],
                },
                application$system: {
                    '#sqp': 'all',
                },
            },
        }, {});
        assert(r7.length === 0);

        const r8 = await context.select('system', {
            data: {
                id: 1,
            },
            filter: {
                id: {
                    $in: [id1, id2, id3],
                },
                application$system: {
                    '#sqp': 'not all',
                },
            },
        }, {});
        assert(r8.length === 3);
        assert(r8.map(ele => ele.id).includes(id1));
        assert(r8.map(ele => ele.id).includes(id2));
        assert(r8.map(ele => ele.id).includes(id3));
    });

    after(() => {
        store.disconnect();
    });
});