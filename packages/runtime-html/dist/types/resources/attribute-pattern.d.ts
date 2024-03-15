import { IContainer } from '@aurelia/kernel';
import type { Constructable } from '@aurelia/kernel';
export interface AttributePatternDefinition<T extends string = string> {
    pattern: T;
    symbols: string;
}
export interface ICharSpec {
    chars: string;
    repeat: boolean;
    isSymbol: boolean;
    isInverted: boolean;
    has(char: string): boolean;
    equals(other: ICharSpec): boolean;
}
export declare class CharSpec implements ICharSpec {
    chars: string;
    repeat: boolean;
    isSymbol: boolean;
    isInverted: boolean;
    has: (char: string) => boolean;
    constructor(chars: string, repeat: boolean, isSymbol: boolean, isInverted: boolean);
    equals(other: ICharSpec): boolean;
}
export declare class Interpretation {
    parts: readonly string[];
    get pattern(): string | null;
    set pattern(value: string | null);
    append(pattern: string, ch: string): void;
    next(pattern: string): void;
}
export interface ISegment {
    text: string;
    eachChar(callback: (spec: CharSpec) => void): void;
}
export declare class SegmentTypes {
    statics: number;
    dynamics: number;
    symbols: number;
}
export interface ISyntaxInterpreter {
    add(defs: AttributePatternDefinition[]): void;
    interpret(name: string): Interpretation;
}
export declare const ISyntaxInterpreter: import("@aurelia/kernel").InterfaceSymbol<ISyntaxInterpreter>;
export declare class SyntaxInterpreter implements ISyntaxInterpreter {
    add(defs: AttributePatternDefinition[]): void;
    interpret(name: string): Interpretation;
}
export declare class AttrSyntax {
    rawName: string;
    rawValue: string;
    target: string;
    command: string | null;
    parts: readonly string[] | null;
    constructor(rawName: string, rawValue: string, target: string, command: string | null, parts?: readonly string[] | null);
}
export type IAttributePattern<T extends string = string> = Record<T, (rawName: string, rawValue: string, parts: readonly string[]) => AttrSyntax>;
export declare const IAttributePattern: import("@aurelia/kernel").InterfaceSymbol<IAttributePattern<string>>;
export interface IAttributeParser {
    parse(name: string, value: string): AttrSyntax;
}
export declare const IAttributeParser: import("@aurelia/kernel").InterfaceSymbol<IAttributeParser>;
export declare class AttributeParser implements IAttributeParser {
    constructor();
    parse(name: string, value: string): AttrSyntax;
}
export interface AttributePatternKind {
    readonly name: string;
    define<const K extends AttributePatternDefinition, P extends Constructable<IAttributePattern<K['pattern']>> = Constructable<IAttributePattern<K['pattern']>>>(patternDefs: K[], Type: P): P;
    getPatternDefinitions(Type: Constructable): AttributePatternDefinition[];
    findAll(container: IContainer): readonly IAttributePattern[];
}
export declare function attributePattern<const K extends AttributePatternDefinition>(...patternDefs: K[]): <T extends Constructable<IAttributePattern<K['pattern']>>>(target: T) => T;
export declare const AttributePattern: Readonly<AttributePatternKind>;
export declare class DotSeparatedAttributePattern {
    'PART.PART'(rawName: string, rawValue: string, parts: readonly string[]): AttrSyntax;
    'PART.PART.PART'(rawName: string, rawValue: string, parts: readonly string[]): AttrSyntax;
}
export declare class RefAttributePattern {
    'ref'(rawName: string, rawValue: string, _parts: readonly string[]): AttrSyntax;
    'PART.ref'(rawName: string, rawValue: string, parts: readonly string[]): AttrSyntax;
}
export declare class EventAttributePattern {
    'PART.trigger:PART'(rawName: string, rawValue: string, parts: readonly string[]): AttrSyntax;
    'PART.capture:PART'(rawName: string, rawValue: string, parts: readonly string[]): AttrSyntax;
}
export declare class ColonPrefixedBindAttributePattern {
    ':PART'(rawName: string, rawValue: string, parts: readonly string[]): AttrSyntax;
}
export declare class AtPrefixedTriggerAttributePattern {
    '@PART'(rawName: string, rawValue: string, parts: readonly string[]): AttrSyntax;
    '@PART:PART'(rawName: string, rawValue: string, parts: readonly string[]): AttrSyntax;
}
export declare class SpreadAttributePattern {
    '...$attrs'(rawName: string, rawValue: string, _parts: readonly string[]): AttrSyntax;
}
//# sourceMappingURL=attribute-pattern.d.ts.map