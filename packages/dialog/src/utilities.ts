/** @internal */ export const createLookup = <T = unknown>() => Object.create(null) as Record<string, T>;

/** @internal */ export const createError = (message: string) => new Error(message);

/** @internal */ export const isPromise = <T>(v: unknown): v is Promise<T> => v instanceof Promise;
/** @internal */ export const isArray = <T>(v: unknown): v is T[] => v instanceof Array;

// eslint-disable-next-line @typescript-eslint/ban-types
/** @internal */ export const isFunction = <K extends Function>(v: unknown): v is K => typeof v === 'function';

/** @internal */ export const isString = (v: unknown): v is string => typeof v === 'string';
