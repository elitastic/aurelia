import { Constructable } from '@aurelia/kernel';
import { IHydratedController } from '../../templating/controller';
/**
 * An optional interface describing the dynamic composition activate convention.
 */
export interface IDynamicComponentActivate<T> {
    /**
     * Implement this hook if you want to perform custom logic just before the component is is composed.
     * The returned value is not used.
     */
    activate?(model?: T): unknown;
}
type ChangeSource = keyof Pick<AuCompose, 'template' | 'component' | 'model' | 'scopeBehavior' | 'composing' | 'composition' | 'tag'>;
export declare class AuCompose {
    template?: string | Promise<string>;
    component?: Constructable | object | Promise<Constructable | object>;
    model?: unknown;
    /**
     * Control scoping behavior of the view created by the au-compose.
     * This only affects template-only composition. Does not have effects on custom element composition.
     *
     * auto = inherit parent scope
     * scoped = do not inherit parent scope
     */
    scopeBehavior: 'auto' | 'scoped';
    get composing(): Promise<void> | void;
    get composition(): ICompositionController | undefined;
    /**
     * The tag name of the element to be created for non custom element composition.
     *
     * `null`/`undefined` means containerless
     */
    tag: string | null | undefined;
    attaching(initiator: IHydratedController, _parent: IHydratedController): void | Promise<void>;
    detaching(initiator: IHydratedController): void | Promise<void>;
}
export interface ICompositionController {
    readonly controller: IHydratedController;
    readonly context: CompositionContext;
    activate(initiator?: IHydratedController): void | Promise<void>;
    deactivate(detachInitator?: IHydratedController): void | Promise<void>;
    update(model: unknown): unknown;
}
declare class LoadedChangeInfo {
    readonly _template: string | undefined;
    readonly _component: Constructable | object | undefined;
    readonly _model: unknown;
    readonly _src: ChangeSource | undefined;
    constructor(_template: string | undefined, _component: Constructable | object | undefined, _model: unknown, _src: ChangeSource | undefined);
}
declare class CompositionContext {
    readonly id: number;
    readonly change: LoadedChangeInfo;
    constructor(id: number, change: LoadedChangeInfo);
}
export {};
//# sourceMappingURL=au-compose.d.ts.map