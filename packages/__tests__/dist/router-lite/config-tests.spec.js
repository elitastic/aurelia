var __esDecorate = (this && this.__esDecorate) || function (ctor, descriptorIn, decorators, contextIn, initializers, extraInitializers) {
    function accept(f) { if (f !== void 0 && typeof f !== "function") throw new TypeError("Function expected"); return f; }
    var kind = contextIn.kind, key = kind === "getter" ? "get" : kind === "setter" ? "set" : "value";
    var target = !descriptorIn && ctor ? contextIn["static"] ? ctor : ctor.prototype : null;
    var descriptor = descriptorIn || (target ? Object.getOwnPropertyDescriptor(target, contextIn.name) : {});
    var _, done = false;
    for (var i = decorators.length - 1; i >= 0; i--) {
        var context = {};
        for (var p in contextIn) context[p] = p === "access" ? {} : contextIn[p];
        for (var p in contextIn.access) context.access[p] = contextIn.access[p];
        context.addInitializer = function (f) { if (done) throw new TypeError("Cannot add initializers after decoration has completed"); extraInitializers.push(accept(f || null)); };
        var result = (0, decorators[i])(kind === "accessor" ? { get: descriptor.get, set: descriptor.set } : descriptor[key], context);
        if (kind === "accessor") {
            if (result === void 0) continue;
            if (result === null || typeof result !== "object") throw new TypeError("Object expected");
            if (_ = accept(result.get)) descriptor.get = _;
            if (_ = accept(result.set)) descriptor.set = _;
            if (_ = accept(result.init)) initializers.unshift(_);
        }
        else if (_ = accept(result)) {
            if (kind === "field") initializers.unshift(_);
            else descriptor[key] = _;
        }
    }
    if (target) Object.defineProperty(target, contextIn.name, descriptor);
    done = true;
};
var __runInitializers = (this && this.__runInitializers) || function (thisArg, initializers, value) {
    var useValue = arguments.length > 2;
    for (var i = 0; i < initializers.length; i++) {
        value = useValue ? initializers[i].call(thisArg, value) : initializers[i].call(thisArg);
    }
    return useValue ? value : void 0;
};
var __setFunctionName = (this && this.__setFunctionName) || function (f, name, prefix) {
    if (typeof name === "symbol") name = name.description ? "[".concat(name.description, "]") : "";
    return Object.defineProperty(f, "name", { configurable: true, value: prefix ? "".concat(prefix, " ", name) : name });
};
import { Aurelia, CustomElement, customElement } from '@aurelia/runtime-html';
import { IRouter, route, Route, RouterConfiguration } from '@aurelia/router-lite';
import { assert, TestContext } from '@aurelia/testing';
import { IHookInvocationAggregator } from './_shared/hook-invocation-tracker.js';
import { HookSpecs, TestRouteViewModelBase } from './_shared/view-models.js';
import { hookSpecsMap, verifyInvocationsEqual } from './_shared/hook-spec.js';
import { createFixture, IActivityTracker } from './_shared/create-fixture.js';
import { LogLevel, resolve } from '@aurelia/kernel';
import { TestRouterConfiguration } from './_shared/configuration.js';
function vp(count) {
    if (count === 1) {
        return `<au-viewport></au-viewport>`;
    }
    let template = '';
    for (let i = 0; i < count; ++i) {
        template = `${template}<au-viewport name="$${i}"></au-viewport>`;
    }
    return template;
}
function getDefaultHIAConfig() {
    return {
        resolveTimeoutMs: 100,
        resolveLabels: [],
    };
}
export function* prepend(prefix, component, ...calls) {
    for (const call of calls) {
        if (call === '') {
            yield '';
        }
        else {
            yield `${prefix}.${component}.${call}`;
        }
    }
}
export function* interleave(...generators) {
    while (generators.length > 0) {
        for (let i = 0, ii = generators.length; i < ii; ++i) {
            const gen = generators[i];
            const next = gen.next();
            if (next.done) {
                generators.splice(i, 1);
                --i;
                --ii;
            }
            else {
                const value = next.value;
                if (value) {
                    yield value;
                }
            }
        }
    }
}
export class SimpleActivityTrackingVMBase {
    constructor() {
        this.tracker = resolve(IActivityTracker);
    }
    attached() {
        this.tracker.setActive(this.$controller.definition.name);
    }
    setNonActive() {
        this.tracker.setActive(this.$controller.definition.name);
    }
}
describe('router-lite/config-tests.spec.ts', function () {
    describe('monomorphic timings', function () {
        const componentSpecs = [
            {
                kind: 'all-sync',
                hookSpecs: HookSpecs.create({
                    binding: hookSpecsMap.binding.sync,
                    bound: hookSpecsMap.bound.sync,
                    attaching: hookSpecsMap.attaching.sync,
                    attached: hookSpecsMap.attached.sync,
                    detaching: hookSpecsMap.detaching.sync,
                    unbinding: hookSpecsMap.unbinding.sync,
                    canLoad: hookSpecsMap.canLoad.sync,
                    loading: hookSpecsMap.loading.sync,
                    canUnload: hookSpecsMap.canUnload.sync,
                    unloading: hookSpecsMap.unloading.sync,
                }),
            },
            {
                kind: 'all-async',
                hookSpecs: getAllAsyncSpecs(1),
            },
        ];
        for (const componentSpec of componentSpecs) {
            const { kind, hookSpecs } = componentSpec;
            let A01 = (() => {
                let _classDecorators = [customElement({ name: 'a01', template: null })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var A01 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "A01");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A01 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A01 = _classThis;
            })();
            let A02 = (() => {
                let _classDecorators = [customElement({ name: 'a02', template: null })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var A02 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "A02");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A02 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A02 = _classThis;
            })();
            let A03 = (() => {
                let _classDecorators = [customElement({ name: 'a03', template: null })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var A03 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "A03");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A03 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A03 = _classThis;
            })();
            let A04 = (() => {
                let _classDecorators = [customElement({ name: 'a04', template: null })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var A04 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "A04");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A04 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A04 = _classThis;
            })();
            const A0 = [A01, A02, A03, A04];
            let Root1 = (() => {
                let _classDecorators = [customElement({ name: 'root1', template: vp(1) })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var Root1 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "Root1");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    Root1 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return Root1 = _classThis;
            })();
            let A11 = (() => {
                let _classDecorators = [customElement({ name: 'a11', template: vp(1) })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var A11 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "A11");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A11 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A11 = _classThis;
            })();
            let A12 = (() => {
                let _classDecorators = [customElement({ name: 'a12', template: vp(1) })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var A12 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "A12");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A12 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A12 = _classThis;
            })();
            let A13 = (() => {
                let _classDecorators = [customElement({ name: 'a13', template: vp(1) })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var A13 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "A13");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A13 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A13 = _classThis;
            })();
            let A14 = (() => {
                let _classDecorators = [customElement({ name: 'a14', template: vp(1) })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var A14 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "A14");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A14 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A14 = _classThis;
            })();
            const A1 = [A11, A12, A13, A14];
            let Root2 = (() => {
                let _classDecorators = [customElement({ name: 'root2', template: vp(2) })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var Root2 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "Root2");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    Root2 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return Root2 = _classThis;
            })();
            let A21 = (() => {
                let _classDecorators = [customElement({ name: 'a21', template: vp(2) })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var A21 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "A21");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A21 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A21 = _classThis;
            })();
            let A22 = (() => {
                let _classDecorators = [customElement({ name: 'a22', template: vp(2) })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = TestRouteViewModelBase;
                var A22 = _classThis = class extends _classSuper {
                    constructor() { super(resolve(IHookInvocationAggregator), hookSpecs); }
                };
                __setFunctionName(_classThis, "A22");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A22 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A22 = _classThis;
            })();
            const A2 = [A21, A22];
            const A = [...A0, ...A1, ...A2];
            describe(`componentSpec.kind:'${kind}'`, function () {
                describe('single', function () {
                    function runTest(spec) {
                        const { t1: [t1, t1c], t2: [t2, t2c], t3: [t3, t3c], t4: [t4, t4c] } = spec;
                        spec.configure();
                        it(`'${t1}' -> '${t2}' -> '${t3}' -> '${t4}'`, async function () {
                            const { router, hia, tearDown } = await createFixture(Root2, A, getDefaultHIAConfig);
                            const phase1 = `('' -> '${t1}')#1`;
                            const phase2 = `('${t1}' -> '${t2}')#2`;
                            const phase3 = `('${t2}' -> '${t3}')#3`;
                            const phase4 = `('${t3}' -> '${t4}')#4`;
                            hia.setPhase(phase1);
                            await router.load(t1);
                            hia.setPhase(phase2);
                            await router.load(t2);
                            hia.setPhase(phase3);
                            await router.load(t3);
                            hia.setPhase(phase4);
                            await router.load(t4);
                            await tearDown();
                            const expected = [...(function* () {
                                    yield `start.root2.binding`;
                                    yield `start.root2.bound`;
                                    yield `start.root2.attaching`;
                                    yield `start.root2.attached`;
                                    yield* prepend(phase1, t1c, 'canLoad', 'loading', 'binding', 'bound', 'attaching', 'attached');
                                    for (const [phase, { $t1c, $t2c }] of [
                                        [phase2, { $t1c: t1c, $t2c: t2c }],
                                        [phase3, { $t1c: t2c, $t2c: t3c }],
                                        [phase4, { $t1c: t3c, $t2c: t4c }],
                                    ]) {
                                        yield `${phase}.${$t1c}.canUnload`;
                                        yield `${phase}.${$t2c}.canLoad`;
                                        yield `${phase}.${$t1c}.unloading`;
                                        yield `${phase}.${$t2c}.loading`;
                                        yield* prepend(phase, $t1c, 'detaching', 'unbinding', 'dispose');
                                        yield* prepend(phase, $t2c, 'binding', 'bound', 'attaching', 'attached');
                                    }
                                    yield `stop.${t4c}.detaching`;
                                    yield `stop.root2.detaching`;
                                    yield `stop.${t4c}.unbinding`;
                                    yield `stop.root2.unbinding`;
                                    yield `stop.root2.dispose`;
                                    yield `stop.${t4c}.dispose`;
                                })()];
                            verifyInvocationsEqual(hia.notifyHistory, expected);
                            hia.dispose();
                        });
                    }
                    const specs = [
                        {
                            t1: ['1', 'a01'],
                            t2: ['2', 'a02'],
                            t3: ['1', 'a01'],
                            t4: ['2', 'a02'],
                            configure() {
                                Route.configure({
                                    routes: [
                                        {
                                            path: '1',
                                            component: A01,
                                        },
                                        {
                                            path: '2',
                                            component: A02,
                                        },
                                    ],
                                }, Root2);
                            },
                        },
                    ];
                    for (const spec of specs) {
                        runTest(spec);
                    }
                });
            });
        }
    });
    for (const inDependencies of [true, false]) {
        describe(`inDependencies: ${inDependencies}`, function () {
            it(`can load a configured child route with direct path and explicit component`, async function () {
                let A01 = (() => {
                    let _classDecorators = [customElement({ name: 'a01', template: null })];
                    let _classDescriptor;
                    let _classExtraInitializers = [];
                    let _classThis;
                    let _classSuper = SimpleActivityTrackingVMBase;
                    var A01 = _classThis = class extends _classSuper {
                    };
                    __setFunctionName(_classThis, "A01");
                    (() => {
                        const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                        __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                        A01 = _classThis = _classDescriptor.value;
                        if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                        __runInitializers(_classThis, _classExtraInitializers);
                    })();
                    return A01 = _classThis;
                })();
                let Root = (() => {
                    let _classDecorators = [route({ routes: [{ path: 'a', component: A01 }] }), customElement({ name: 'root', template: vp(1), dependencies: inDependencies ? [A01] : [] })];
                    let _classDescriptor;
                    let _classExtraInitializers = [];
                    let _classThis;
                    let _classSuper = SimpleActivityTrackingVMBase;
                    var Root = _classThis = class extends _classSuper {
                    };
                    __setFunctionName(_classThis, "Root");
                    (() => {
                        const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                        __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                        Root = _classThis = _classDescriptor.value;
                        if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                        __runInitializers(_classThis, _classExtraInitializers);
                    })();
                    return Root = _classThis;
                })();
                const { router, activityTracker } = await createFixture(Root, [], getDefaultHIAConfig);
                await router.load('a');
                verifyInvocationsEqual(activityTracker.activeVMs, ['root', 'a01']);
            });
            it(`can load a configured child route with indirect path and explicit component`, async function () {
                let A01 = (() => {
                    let _classDecorators = [route({ path: 'a' }), customElement({ name: 'a01', template: null })];
                    let _classDescriptor;
                    let _classExtraInitializers = [];
                    let _classThis;
                    let _classSuper = SimpleActivityTrackingVMBase;
                    var A01 = _classThis = class extends _classSuper {
                    };
                    __setFunctionName(_classThis, "A01");
                    (() => {
                        const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                        __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                        A01 = _classThis = _classDescriptor.value;
                        if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                        __runInitializers(_classThis, _classExtraInitializers);
                    })();
                    return A01 = _classThis;
                })();
                let Root = (() => {
                    let _classDecorators = [route({ routes: [A01] }), customElement({ name: 'root', template: vp(1), dependencies: inDependencies ? [A01] : [] })];
                    let _classDescriptor;
                    let _classExtraInitializers = [];
                    let _classThis;
                    let _classSuper = SimpleActivityTrackingVMBase;
                    var Root = _classThis = class extends _classSuper {
                    };
                    __setFunctionName(_classThis, "Root");
                    (() => {
                        const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                        __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                        Root = _classThis = _classDescriptor.value;
                        if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                        __runInitializers(_classThis, _classExtraInitializers);
                    })();
                    return Root = _classThis;
                })();
                const { router, activityTracker } = await createFixture(Root, [], getDefaultHIAConfig);
                await router.load('a');
                verifyInvocationsEqual(activityTracker.activeVMs, ['root', 'a01']);
            });
        });
    }
    it(`can load a configured child route by name`, async function () {
        let A01 = (() => {
            let _classDecorators = [customElement({ name: 'a01', template: null })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var A01 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "A01");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                A01 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return A01 = _classThis;
        })();
        let Root = (() => {
            let _classDecorators = [route({ routes: [A01] }), customElement({ name: 'root', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var Root = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "Root");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                Root = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return Root = _classThis;
        })();
        const { router, activityTracker } = await createFixture(Root, [], getDefaultHIAConfig);
        await router.load('a01');
        verifyInvocationsEqual(activityTracker.activeVMs, ['root', 'a01']);
    });
    it(`works with single multi segment static path`, async function () {
        let A01 = (() => {
            let _classDecorators = [customElement({ name: 'a01', template: null })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var A01 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "A01");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                A01 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return A01 = _classThis;
        })();
        let Root = (() => {
            let _classDecorators = [route({ routes: [{ path: 'a/x', component: A01 }] }), customElement({ name: 'root', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var Root = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "Root");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                Root = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return Root = _classThis;
        })();
        const { router, activityTracker } = await createFixture(Root, [], getDefaultHIAConfig, () => ({}));
        await router.load('a/x');
        verifyInvocationsEqual(activityTracker.activeVMs, ['root', 'a01']);
    });
    it(`works with single multi segment dynamic path`, async function () {
        let A01 = (() => {
            let _classDecorators = [customElement({ name: 'a01', template: null })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var A01 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "A01");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                A01 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return A01 = _classThis;
        })();
        let Root = (() => {
            let _classDecorators = [route({ routes: [{ path: 'a/:x', component: A01 }] }), customElement({ name: 'root', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var Root = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "Root");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                Root = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return Root = _classThis;
        })();
        const { router, activityTracker } = await createFixture(Root, [], getDefaultHIAConfig, () => ({}));
        await router.load('a/1');
        verifyInvocationsEqual(activityTracker.activeVMs, ['root', 'a01']);
    });
    it(`works with single multi segment static path with single child`, async function () {
        let B01 = (() => {
            let _classDecorators = [customElement({ name: 'b01', template: null })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var B01 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "B01");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                B01 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return B01 = _classThis;
        })();
        let A11 = (() => {
            let _classDecorators = [route({ routes: [{ path: 'b', component: B01 }] }), customElement({ name: 'a11', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var A11 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "A11");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                A11 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return A11 = _classThis;
        })();
        let Root = (() => {
            let _classDecorators = [route({ routes: [{ path: 'a/x', component: A11 }] }), customElement({ name: 'root', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var Root = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "Root");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                Root = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return Root = _classThis;
        })();
        const { router, activityTracker } = await createFixture(Root, [], getDefaultHIAConfig, () => ({}));
        await router.load('a/x/b');
        verifyInvocationsEqual(activityTracker.activeVMs, ['root', 'a11', 'b01']);
    });
    it(`works with single multi segment static path with single multi segment static child`, async function () {
        let B01 = (() => {
            let _classDecorators = [customElement({ name: 'b01', template: null })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var B01 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "B01");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                B01 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return B01 = _classThis;
        })();
        let A11 = (() => {
            let _classDecorators = [route({ routes: [{ path: 'b/x', component: B01 }] }), customElement({ name: 'a11', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var A11 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "A11");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                A11 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return A11 = _classThis;
        })();
        let Root = (() => {
            let _classDecorators = [route({ routes: [{ path: 'a/x', component: A11 }] }), customElement({ name: 'root', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var Root = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "Root");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                Root = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return Root = _classThis;
        })();
        const { router, activityTracker } = await createFixture(Root, [], getDefaultHIAConfig, () => ({}));
        await router.load('a/x/b/x');
        verifyInvocationsEqual(activityTracker.activeVMs, ['root', 'a11', 'b01']);
    });
    it(`works with single static path with single multi segment static child`, async function () {
        let B01 = (() => {
            let _classDecorators = [customElement({ name: 'b01', template: null })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var B01 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "B01");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                B01 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return B01 = _classThis;
        })();
        let A11 = (() => {
            let _classDecorators = [route({ routes: [{ path: 'b/x', component: B01 }] }), customElement({ name: 'a11', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var A11 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "A11");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                A11 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return A11 = _classThis;
        })();
        let Root = (() => {
            let _classDecorators = [route({ routes: [{ path: 'a', component: A11 }] }), customElement({ name: 'root', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var Root = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "Root");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                Root = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return Root = _classThis;
        })();
        const { router, activityTracker } = await createFixture(Root, [], getDefaultHIAConfig, () => ({}));
        await router.load('a/b/x');
        verifyInvocationsEqual(activityTracker.activeVMs, ['root', 'a11', 'b01']);
    });
    it(`works with single empty static path redirect`, async function () {
        let A01 = (() => {
            let _classDecorators = [customElement({ name: 'a01', template: null })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var A01 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "A01");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                A01 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return A01 = _classThis;
        })();
        let Root = (() => {
            let _classDecorators = [route({ routes: [{ path: '', redirectTo: 'a' }, { path: 'a', component: A01 }] }), customElement({ name: 'root', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var Root = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "Root");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                Root = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return Root = _classThis;
        })();
        const { router, activityTracker } = await createFixture(Root, [], getDefaultHIAConfig, () => ({}));
        await router.load('');
        verifyInvocationsEqual(activityTracker.activeVMs, ['root', 'a01']);
    });
    it(`works with single static path redirect`, async function () {
        let A01 = (() => {
            let _classDecorators = [customElement({ name: 'a01', template: null })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var A01 = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "A01");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                A01 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return A01 = _classThis;
        })();
        let Root = (() => {
            let _classDecorators = [route({ routes: [{ path: 'x', redirectTo: 'a' }, { path: 'a', component: A01 }] }), customElement({ name: 'root', template: vp(1) })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            let _classSuper = SimpleActivityTrackingVMBase;
            var Root = _classThis = class extends _classSuper {
            };
            __setFunctionName(_classThis, "Root");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                Root = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return Root = _classThis;
        })();
        const { router, activityTracker } = await createFixture(Root, [], getDefaultHIAConfig, () => ({}));
        await router.load('x');
        verifyInvocationsEqual(activityTracker.activeVMs, ['root', 'a01']);
    });
    describe(`throw error when`, function () {
        it(`load a configured child route with indirect path by name`, async function () {
            let A01 = (() => {
                let _classDecorators = [route({ path: 'a' }), customElement({ name: 'a01', template: null })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = SimpleActivityTrackingVMBase;
                var A01 = _classThis = class extends _classSuper {
                };
                __setFunctionName(_classThis, "A01");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    A01 = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return A01 = _classThis;
            })();
            let Root = (() => {
                let _classDecorators = [route({ routes: [A01] }), customElement({ name: 'root', template: vp(1) })];
                let _classDescriptor;
                let _classExtraInitializers = [];
                let _classThis;
                let _classSuper = SimpleActivityTrackingVMBase;
                var Root = _classThis = class extends _classSuper {
                };
                __setFunctionName(_classThis, "Root");
                (() => {
                    const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
                    __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                    Root = _classThis = _classDescriptor.value;
                    if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                    __runInitializers(_classThis, _classExtraInitializers);
                })();
                return Root = _classThis;
            })();
            const { router } = await createFixture(Root, [], getDefaultHIAConfig);
            let e = null;
            try {
                await router.load('a01');
            }
            catch (err) {
                e = err;
            }
            assert.notStrictEqual(e, null);
            assert.match(e.message, /AUR3401/);
        });
    });
    it('routes can be configured using the getRouteConfig hook', async function () {
        let C1 = (() => {
            let _classDecorators = [customElement({ name: 'ce-c1', template: 'c1' })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            var C1 = _classThis = class {
            };
            __setFunctionName(_classThis, "C1");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                C1 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return C1 = _classThis;
        })();
        let C2 = (() => {
            let _classDecorators = [customElement({ name: 'ce-c2', template: 'c2' })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            var C2 = _classThis = class {
            };
            __setFunctionName(_classThis, "C2");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                C2 = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return C2 = _classThis;
        })();
        let P = (() => {
            let _classDecorators = [customElement({ name: 'ce-p', template: 'p <au-viewport></au-viewport>' })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            var P = _classThis = class {
                constructor() {
                    this.getRouteConfigCalled = 0;
                }
                async getRouteConfig(parentDefinition, routeNode) {
                    assert.notEqual(parentDefinition, null, 'P parentDefinition');
                    assert.notEqual(routeNode, null, 'P routeNode');
                    assert.strictEqual(this.getRouteConfigCalled, 0);
                    this.getRouteConfigCalled++;
                    await new Promise((resolve) => setTimeout(resolve, 10));
                    return {
                        routes: [
                            { path: 'c1', component: C1 },
                            { path: 'c2', component: C2 },
                        ]
                    };
                }
            };
            __setFunctionName(_classThis, "P");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                P = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return P = _classThis;
        })();
        let Root = (() => {
            let _classDecorators = [customElement({ name: 'ro-ot', template: 'root <au-viewport></au-viewport>' })];
            let _classDescriptor;
            let _classExtraInitializers = [];
            let _classThis;
            var Root = _classThis = class {
                constructor() {
                    this.getRouteConfigCalled = 0;
                }
                getRouteConfig(parentDefinition, routeNode) {
                    assert.strictEqual(parentDefinition, null, 'root parentDefinition');
                    assert.strictEqual(routeNode, null, 'root routeNode');
                    assert.strictEqual(this.getRouteConfigCalled, 0);
                    this.getRouteConfigCalled++;
                    return {
                        routes: [
                            { path: 'p', component: P }
                        ]
                    };
                }
            };
            __setFunctionName(_classThis, "Root");
            (() => {
                const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(null) : void 0;
                __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
                Root = _classThis = _classDescriptor.value;
                if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
                __runInitializers(_classThis, _classExtraInitializers);
            })();
            return Root = _classThis;
        })();
        const ctx = TestContext.create();
        const { container } = ctx;
        container.register(TestRouterConfiguration.for(LogLevel.warn), RouterConfiguration, C1, C2, P, Root);
        const au = new Aurelia(container);
        const host = ctx.createElement('div');
        await au.app({ component: Root, host }).start();
        const router = container.get(IRouter);
        await router.load('p/c1');
        assert.html.textContent(host, 'root p c1');
        await router.load('p/c2');
        assert.html.textContent(host, 'root p c2');
        assert.strictEqual(au.root.controller.viewModel.getRouteConfigCalled, 1);
        assert.strictEqual(CustomElement.for(host.querySelector('ce-p')).viewModel.getRouteConfigCalled, 1);
        await au.stop(true);
    });
});
function getAllAsyncSpecs(count) {
    return HookSpecs.create({
        binding: hookSpecsMap.binding.async(count),
        bound: hookSpecsMap.bound.async(count),
        attaching: hookSpecsMap.attaching.async(count),
        attached: hookSpecsMap.attached.async(count),
        detaching: hookSpecsMap.detaching.async(count),
        unbinding: hookSpecsMap.unbinding.async(count),
        canLoad: hookSpecsMap.canLoad.async(count),
        loading: hookSpecsMap.loading.async(count),
        canUnload: hookSpecsMap.canUnload.async(count),
        unloading: hookSpecsMap.unloading.async(count),
    });
}
//# sourceMappingURL=config-tests.spec.js.map