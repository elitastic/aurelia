import { ViewportInstructionTree } from './instructions';
import { type NavigationOptions } from './options';
export declare const enum ExpressionKind {
    Route = 0,
    CompositeSegment = 1,
    ScopedSegment = 2,
    SegmentGroup = 3,
    Segment = 4,
    Component = 5,
    Action = 6,
    Viewport = 7,
    ParameterList = 8,
    Parameter = 9
}
export type RouteExpressionOrHigher = CompositeSegmentExpressionOrHigher | RouteExpression;
export declare class RouteExpression {
    readonly raw: string;
    readonly isAbsolute: boolean;
    readonly root: CompositeSegmentExpressionOrHigher;
    readonly queryParams: Readonly<URLSearchParams>;
    readonly fragment: string | null;
    readonly fragmentIsRoute: boolean;
    get kind(): ExpressionKind.Route;
    constructor(raw: string, isAbsolute: boolean, root: CompositeSegmentExpressionOrHigher, queryParams: Readonly<URLSearchParams>, fragment: string | null, fragmentIsRoute: boolean);
    static parse(path: string, fragmentIsRoute: boolean): RouteExpression;
    toInstructionTree(options: NavigationOptions): ViewportInstructionTree;
    toString(): string;
}
export type CompositeSegmentExpressionOrHigher = ScopedSegmentExpressionOrHigher | CompositeSegmentExpression;
export type CompositeSegmentExpressionOrLower = RouteExpression | CompositeSegmentExpression;
/**
 * A single 'traditional' (slash-separated) segment consisting of one or more sibling segments.
 *
 * ### Variations:
 *
 * 1: `a+b`
 * - siblings: [`a`, `b`]
 * - append: `false`
 *
 * 2: `+a`
 * - siblings: [`a`]
 * - append: `true`
 *
 * 3: `+a+a`
 * - siblings: [`a`, `b`]
 * - append: `true`
 *
 * Where
 * - a = `CompositeSegmentExpressionOrHigher` (`SegmentExpression | SegmentGroupExpression | ScopedSegmentExpression | CompositeSegmentExpression`)
 * - b = `CompositeSegmentExpressionOrHigher` (`SegmentExpression | SegmentGroupExpression | ScopedSegmentExpression | CompositeSegmentExpression`)
 */
export declare class CompositeSegmentExpression {
    readonly raw: string;
    readonly siblings: readonly ScopedSegmentExpressionOrHigher[];
    get kind(): ExpressionKind.CompositeSegment;
    constructor(raw: string, siblings: readonly ScopedSegmentExpressionOrHigher[]);
    toString(): string;
}
export type ScopedSegmentExpressionOrHigher = SegmentGroupExpressionOrHigher | ScopedSegmentExpression;
export type ScopedSegmentExpressionOrLower = CompositeSegmentExpressionOrLower | ScopedSegmentExpression;
/**
 * The (single) left-hand side and the (one or more) right-hand side of a slash-separated segment.
 *
 * Variations:
 *
 * 1: `a/b`
 * - left: `a`
 * - right: `b`
 *
 * Where
 * - a = `SegmentGroupExpressionOrHigher` (`SegmentExpression | SegmentGroupExpression`)
 * - b = `ScopedSegmentExpressionOrHigher` (`SegmentExpression | SegmentGroupExpression | ScopedSegmentExpression`)
 */
export declare class ScopedSegmentExpression {
    readonly raw: string;
    readonly left: SegmentGroupExpressionOrHigher;
    readonly right: ScopedSegmentExpressionOrHigher;
    get kind(): ExpressionKind.ScopedSegment;
    constructor(raw: string, left: SegmentGroupExpressionOrHigher, right: ScopedSegmentExpressionOrHigher);
    toString(): string;
}
export type SegmentGroupExpressionOrHigher = SegmentExpression | SegmentGroupExpression;
export type SegmentGroupExpressionOrLower = ScopedSegmentExpressionOrLower | SegmentGroupExpression;
/**
 * Any kind of segment wrapped in parentheses, increasing its precedence.
 * Specifically, the parentheses are needed to deeply specify scoped siblings.
 * The precedence is intentionally similar to the familiar mathematical `/` and `+` operators.
 *
 * For example, consider this viewport structure:
 * - viewport-a
 * - - viewport-a1
 * - - viewport-a2
 * - viewport-b
 * - - viewport-b1
 *
 * This can only be deeply specified by using the grouping operator: `a/(a1+a2)+b/b1`
 *
 * Because `a/a1+a2+b/b1` would be interpreted differently:
 * - viewport-a
 * - - viewport-a1
 * - viewport-a2
 * - viewport-b
 * - - viewport-b1
 *
 * ### Variations:
 *
 * 1: `(a)`
 * - expression: `a`
 *
 * Where
 * - a = `CompositeSegmentExpressionOrHigher` (`SegmentExpression | SegmentGroupExpression | ScopedSegmentExpression | CompositeSegmentExpression`)
 */
export declare class SegmentGroupExpression {
    readonly raw: string;
    readonly expression: CompositeSegmentExpressionOrHigher;
    get kind(): ExpressionKind.SegmentGroup;
    constructor(raw: string, expression: CompositeSegmentExpressionOrHigher);
    toString(): string;
}
/**
 * A (non-composite) segment specifying a single component and (optional) viewport / action.
 */
export declare class SegmentExpression {
    readonly raw: string;
    readonly component: ComponentExpression;
    readonly action: ActionExpression;
    readonly viewport: ViewportExpression;
    readonly scoped: boolean;
    get kind(): ExpressionKind.Segment;
    static get EMPTY(): SegmentExpression;
    constructor(raw: string, component: ComponentExpression, action: ActionExpression, viewport: ViewportExpression, scoped: boolean);
    toString(): string;
}
export declare class ComponentExpression {
    readonly raw: string;
    readonly name: string;
    readonly parameterList: ParameterListExpression;
    get kind(): ExpressionKind.Component;
    static get EMPTY(): ComponentExpression;
    /**
     * A single segment matching parameter, e.g. `:foo` (will match `a` but not `a/b`)
     */
    readonly isParameter: boolean;
    /**
     * A multi-segment matching parameter, e.g. `*foo` (will match `a` as well as `a/b`)
     */
    readonly isStar: boolean;
    /**
     * Whether this is either a parameter or a star segment.
     */
    readonly isDynamic: boolean;
    /**
     * If this is a dynamic segment (parameter or star), this is the name of the parameter (the name without the `:` or `*`)
     */
    readonly parameterName: string;
    constructor(raw: string, name: string, parameterList: ParameterListExpression);
    toString(): string;
}
export declare class ActionExpression {
    readonly raw: string;
    readonly name: string;
    readonly parameterList: ParameterListExpression;
    get kind(): ExpressionKind.Action;
    static get EMPTY(): ActionExpression;
    constructor(raw: string, name: string, parameterList: ParameterListExpression);
    toString(): string;
}
export declare class ViewportExpression {
    readonly raw: string;
    readonly name: string | null;
    get kind(): ExpressionKind.Viewport;
    static get EMPTY(): ViewportExpression;
    constructor(raw: string, name: string | null);
    toString(): string;
}
export declare class ParameterListExpression {
    readonly raw: string;
    readonly expressions: readonly ParameterExpression[];
    get kind(): ExpressionKind.ParameterList;
    static get EMPTY(): ParameterListExpression;
    constructor(raw: string, expressions: readonly ParameterExpression[]);
    toString(): string;
}
export declare class ParameterExpression {
    readonly raw: string;
    readonly key: string;
    readonly value: string;
    get kind(): ExpressionKind.Parameter;
    static get EMPTY(): ParameterExpression;
    constructor(raw: string, key: string, value: string);
    toString(): string;
}
export declare const AST: Readonly<{
    RouteExpression: typeof RouteExpression;
    CompositeSegmentExpression: typeof CompositeSegmentExpression;
    ScopedSegmentExpression: typeof ScopedSegmentExpression;
    SegmentGroupExpression: typeof SegmentGroupExpression;
    SegmentExpression: typeof SegmentExpression;
    ComponentExpression: typeof ComponentExpression;
    ActionExpression: typeof ActionExpression;
    ViewportExpression: typeof ViewportExpression;
    ParameterListExpression: typeof ParameterListExpression;
    ParameterExpression: typeof ParameterExpression;
}>;
//# sourceMappingURL=route-expression.d.ts.map