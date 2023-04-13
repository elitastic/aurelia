import { ICustomAttributeViewModel, ICustomAttributeController, INode, IWindow } from '@aurelia/runtime-html';
import { IRouter } from '../router';
import { IRouteContext } from '../route-context';
export declare class HrefCustomAttribute implements ICustomAttributeViewModel {
    private readonly el;
    private readonly router;
    private readonly ctx;
    value: unknown;
    private isInitialized;
    private isEnabled;
    private get isExternal();
    readonly $controller: ICustomAttributeController<this>;
    constructor(el: INode<HTMLElement>, router: IRouter, ctx: IRouteContext, w: IWindow);
    binding(): void;
    unbinding(): void;
    valueChanged(newValue: unknown): void;
    handleEvent(e: MouseEvent): void;
}
//# sourceMappingURL=href.d.ts.map