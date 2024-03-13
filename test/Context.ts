import { AsyncContext } from 'oak-domain/lib/store/AsyncRowStore';
import { EntityDict } from './test-app-domain';
export class TestContext extends AsyncContext<EntityDict> {
    initialize(data: any): Promise<void> {
        throw new Error('Method not implemented.');
    }
    openRootMode(): () => void {
        return () => undefined;
    }
    async refineOpRecords(): Promise<void> {
        return;
    }
    isRoot(): boolean {
        return true;
    }
    setCurrentUserId(userId: string | undefined): void {
        throw new Error('Method not implemented.');        
    }
    getCurrentUserId(allowUnloggedIn?: boolean | undefined): string | undefined {
        return 'test-root-id';
    }
    async toString(): Promise<string> {
        throw new Error('Method not implemented.');
    }
    allowUserUpdate(): boolean {
        return true;
    }    
}
