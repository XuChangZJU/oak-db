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
                        applicationId: await v4(),
                        userId: v4(),
                        entity: 'mobile',
                        entityId: v4(),
                    }
                }]
            } as EntityDict['user']['Create']['data']
        } as EntityDict['user']['Create'], context);
    });

    after(() => {
        store.disconnect();
    });
});