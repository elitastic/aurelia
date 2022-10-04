import { IExpressionParser, IObserverLocator, type IsBindingBehavior } from '@aurelia/runtime';
import { BindingMode, CommandType, IRenderer, IHydratableController, AttrSyntax, IPlatform, IAttrMapper, ICommandBuildInfo } from '@aurelia/runtime-html';
import type { BindingCommandInstance } from '@aurelia/runtime-html';
export declare const TranslationInstructionType = "tt";
export declare class TranslationAttributePattern {
    [key: string]: ((rawName: string, rawValue: string, parts: string[]) => AttrSyntax);
    static registerAlias(alias: string): void;
}
export declare class TranslationBindingInstruction {
    from: IsBindingBehavior;
    to: string;
    readonly type: string;
    mode: BindingMode.toView;
    constructor(from: IsBindingBehavior, to: string);
}
export declare class TranslationBindingCommand implements BindingCommandInstance {
    readonly type: CommandType.None;
    get name(): string;
    build(info: ICommandBuildInfo, parser: IExpressionParser, attrMapper: IAttrMapper): TranslationBindingInstruction;
}
export declare class TranslationBindingRenderer implements IRenderer {
    target: typeof TranslationInstructionType;
    constructor(exprParser: IExpressionParser, observerLocator: IObserverLocator, p: IPlatform);
    render(renderingCtrl: IHydratableController, target: HTMLElement, instruction: TranslationBindingInstruction): void;
}
export declare const TranslationBindInstructionType = "tbt";
export declare class TranslationBindAttributePattern {
    [key: string]: ((rawName: string, rawValue: string, parts: string[]) => AttrSyntax);
    static registerAlias(alias: string): void;
}
export declare class TranslationBindBindingInstruction {
    from: IsBindingBehavior;
    to: string;
    readonly type: string;
    mode: BindingMode.toView;
    constructor(from: IsBindingBehavior, to: string);
}
export declare class TranslationBindBindingCommand implements BindingCommandInstance {
    readonly type: CommandType.None;
    get name(): string;
    build(info: ICommandBuildInfo, exprParser: IExpressionParser, attrMapper: IAttrMapper): TranslationBindingInstruction;
}
export declare class TranslationBindBindingRenderer implements IRenderer {
    private readonly parser;
    private readonly oL;
    private readonly p;
    target: typeof TranslationBindInstructionType;
    constructor(parser: IExpressionParser, oL: IObserverLocator, p: IPlatform);
    render(renderingCtrl: IHydratableController, target: HTMLElement, instruction: TranslationBindBindingInstruction): void;
}
//# sourceMappingURL=translation-renderer.d.ts.map