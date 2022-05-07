import type { Store } from './store';
export declare type StepFn<T> = (res: T) => void;
export declare function executeSteps<T>(store: Store<T>, shouldLogResults: boolean, ...steps: StepFn<T>[]): Promise<unknown>;
//# sourceMappingURL=test-helpers.d.ts.map