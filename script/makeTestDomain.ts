import {
    buildSchema,
    analyzeEntities,
} from 'oak-domain/lib/compiler/schemalBuilder';

analyzeEntities(`${process.cwd()}/node_modules/oak-domain/src/entities`, 'oak-domain/lib/entities')
analyzeEntities(`${process.cwd()}/node_modules/oak-general-business/src/entities`, 'oak-general-business/lib/entities');
analyzeEntities(`${process.cwd()}/test/entities`);
buildSchema(`${process.cwd()}/test/test-app-domain`);