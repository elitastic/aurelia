import { ILogger } from '@aurelia/kernel';
import { Scope } from '@aurelia/runtime';
import { INode, IRenderLocation } from '../../dom';
import { IPlatform } from '../../platform';
import { IInstruction } from '../../renderer';
import { ICustomAttributeController, ICustomAttributeViewModel, IHydratableController, IHydratedController, IHydratedParentController, ISyntheticView } from '../../templating/controller';
import { IViewFactory } from '../../templating/view';
import { AttrSyntax } from '../attribute-pattern';
export declare class PromiseTemplateController implements ICustomAttributeViewModel {
    readonly $controller: ICustomAttributeController<this>;
    private view;
    value: Promise<unknown>;
    pending?: PendingTemplateController;
    fulfilled?: FulfilledTemplateController;
    rejected?: RejectedTemplateController;
    private viewScope;
    private preSettledTask;
    private postSettledTask;
    private postSettlePromise;
    constructor(
    /** @internal */ _factory: IViewFactory, 
    /** @internal */ _location: IRenderLocation, 
    /** @internal */ _platform: IPlatform, logger: ILogger);
    link(_controller: IHydratableController, _childController: ICustomAttributeController, _target: INode, _instruction: IInstruction): void;
    attaching(initiator: IHydratedController, _parent: IHydratedParentController): void | Promise<void>;
    valueChanged(_newValue: boolean, _oldValue: boolean): void;
    private swap;
    detaching(initiator: IHydratedController, _parent: IHydratedParentController): void | Promise<void>;
    dispose(): void;
}
export declare class PendingTemplateController implements ICustomAttributeViewModel {
    readonly $controller: ICustomAttributeController<this>;
    value: Promise<unknown>;
    view: ISyntheticView | undefined;
    constructor(
    /** @internal */ _factory: IViewFactory, 
    /** @internal */ _location: IRenderLocation);
    link(controller: IHydratableController, _childController: ICustomAttributeController, _target: INode, _instruction: IInstruction): void;
    activate(initiator: IHydratedController | null, scope: Scope): void | Promise<void>;
    deactivate(_initiator: IHydratedController | null): void | Promise<void>;
    detaching(initiator: IHydratedController): void | Promise<void>;
    dispose(): void;
}
export declare class FulfilledTemplateController implements ICustomAttributeViewModel {
    readonly $controller: ICustomAttributeController<this>;
    value: unknown;
    view: ISyntheticView | undefined;
    constructor(
    /** @internal */ _factory: IViewFactory, 
    /** @internal */ _location: IRenderLocation);
    link(controller: IHydratableController, _childController: ICustomAttributeController, _target: INode, _instruction: IInstruction): void;
    activate(initiator: IHydratedController | null, scope: Scope, resolvedValue: unknown): void | Promise<void>;
    deactivate(_initiator: IHydratedController | null): void | Promise<void>;
    detaching(initiator: IHydratedController, _parent: IHydratedParentController): void | Promise<void>;
    dispose(): void;
}
export declare class RejectedTemplateController implements ICustomAttributeViewModel {
    private readonly _factory;
    private readonly _location;
    readonly $controller: ICustomAttributeController<this>;
    value: unknown;
    view: ISyntheticView | undefined;
    constructor(_factory: IViewFactory, _location: IRenderLocation);
    link(controller: IHydratableController, _childController: ICustomAttributeController, _target: INode, _instruction: IInstruction): void;
    activate(initiator: IHydratedController | null, scope: Scope, error: unknown): void | Promise<void>;
    deactivate(_initiator: IHydratedController | null): void | Promise<void>;
    detaching(initiator: IHydratedController, _parent: IHydratedParentController): void | Promise<void>;
    dispose(): void;
}
export declare class PromiseAttributePattern {
    'promise.resolve'(name: string, value: string, _parts: string[]): AttrSyntax;
}
export declare class FulfilledAttributePattern {
    'then'(name: string, value: string, _parts: string[]): AttrSyntax;
}
export declare class RejectedAttributePattern {
    'catch'(name: string, value: string, _parts: string[]): AttrSyntax;
}
//# sourceMappingURL=promise.d.ts.map