import { AsyncContext } from 'oak-domain/lib/store/AsyncRowStore';
import { EntityDict } from './test-app-domain';
export class TestContext extends AsyncContext<EntityDict> {
    refineOpRecords(): Promise<void> {
        throw new Error('Method not implemented.');
    }
    isRoot(): boolean {
        return true;
    }
    getCurrentUserId(allowUnloggedIn?: boolean | undefined): string | undefined {
        return 'test-root-id';
    }
    toString(): string {
        throw new Error('Method not implemented.');
    }
    allowUserUpdate(): boolean {
        return true;
    }    
}
