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
        console.log(sql);
    });
});