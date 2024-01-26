import { preprocess } from '@aurelia/plugin-conventions';
import { TsJestTransformerOptions } from 'ts-jest';
import type { TransformOptions, TransformedSource } from '@jest/transform';
declare function _createTransformer(conventionsOptions?: {}, _preprocess?: typeof preprocess, _tsProcess?: (sourceText: string, sourcePath: string, transformOptions: import("ts-jest").TsJestTransformOptions) => TransformedSource): {
    canInstrument: boolean;
    getCacheKey: (fileData: string, filePath: string, options: TransformOptions<TsJestTransformerOptions>) => string;
    process: (sourceText: string, sourcePath: string, transformOptions: TransformOptions<TsJestTransformerOptions>) => TransformedSource;
};
declare function createTransformer(conventionsOptions?: {}): {
    canInstrument: boolean;
    getCacheKey: (fileData: string, filePath: string, options: TransformOptions<import("ts-jest").TsJestGlobalOptions>) => string;
    process: (sourceText: string, sourcePath: string, transformOptions: TransformOptions<import("ts-jest").TsJestGlobalOptions>) => TransformedSource;
};
declare const _default: {
    canInstrument: boolean;
    getCacheKey: (fileData: string, filePath: string, options: TransformOptions<import("ts-jest").TsJestGlobalOptions>) => string;
    process: (sourceText: string, sourcePath: string, transformOptions: TransformOptions<import("ts-jest").TsJestGlobalOptions>) => TransformedSource;
    createTransformer: typeof createTransformer;
    _createTransformer: typeof _createTransformer;
};
export default _default;
//# sourceMappingURL=index.d.ts.map