import {
    buildSchema,
    analyzeEntities,
} from 'oak-domain/src/compiler/schemalBuilder';

analyzeEntities(`${process.cwd()}/node_modules/oak-general-business/src/entities`);
analyzeEntities(`${process.cwd()}/test/entities`);
buildSchema(`${process.cwd()}/test/test-app-domain`);