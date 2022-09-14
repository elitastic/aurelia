import { I18N as t } from "@aurelia/i18n";

import { DI as e, IServiceLocator as i, IEventAggregator as o, ILogger as r, Registration as n, noop as a } from "@aurelia/kernel";

import { IExpressionParser as l } from "@aurelia/runtime";

import { IPlatform as s } from "@aurelia/runtime-html";

import { ValidationMessageProvider as c, IValidator as u } from "@aurelia/validation";

import { ValidationController as f, ValidationControllerFactory as d, getDefaultValidationHtmlConfiguration as m, ValidationHtmlConfiguration as h } from "@aurelia/validation-html";

function p(t, e, i, o) {
    var r = arguments.length, n = r < 3 ? e : null === o ? o = Object.getOwnPropertyDescriptor(e, i) : o, a;
    if ("object" === typeof Reflect && "function" === typeof Reflect.decorate) n = Reflect.decorate(t, e, i, o); else for (var l = t.length - 1; l >= 0; l--) if (a = t[l]) n = (r < 3 ? a(n) : r > 3 ? a(e, i, n) : a(e, i)) || n;
    return r > 3 && n && Object.defineProperty(e, i, n), n;
}

function v(t, e) {
    return function(i, o) {
        e(i, o, t);
    };
}

const y = "i18n:locale:changed:validation";

const g = e.createInterface("I18nKeyConfiguration");

let x = class LocalizedValidationController extends f {
    constructor(t, e, i, o, r) {
        super(i, o, r, t);
        this.localeChangeSubscription = e.subscribe(y, (() => {
            r.domReadQueue.queueTask((async () => {
                await this.revalidateErrors();
            }));
        }));
    }
};

x = p([ v(0, i), v(1, o), v(2, u), v(3, l), v(4, s) ], x);

class LocalizedValidationControllerFactory extends d {
    construct(t, e) {
        return t.invoke(x, e);
    }
}

let z = class LocalizedValidationMessageProvider extends c {
    constructor(t, e, i, o, r) {
        super(o, r, []);
        this.i18n = e;
        const n = t.DefaultNamespace;
        const a = t.DefaultKeyPrefix;
        if (void 0 !== n || void 0 !== a) {
            this.keyPrefix = void 0 !== n ? `${n}:` : "";
            this.keyPrefix = void 0 !== a ? `${this.keyPrefix}${a}.` : this.keyPrefix;
        }
        i.subscribe("i18n:locale:changed", (() => {
            this.registeredMessages = new WeakMap;
            i.publish(y);
        }));
    }
    getMessage(t) {
        const e = this.registeredMessages.get(t);
        if (void 0 !== e) return e;
        return this.setMessage(t, this.i18n.tr(this.getKey(t.messageKey)));
    }
    getDisplayName(t, e) {
        if (null !== e && void 0 !== e) return e instanceof Function ? e() : e;
        if (void 0 === t) return;
        return this.i18n.tr(this.getKey(t));
    }
    getKey(t) {
        const e = this.keyPrefix;
        return void 0 !== e ? `${e}${t}` : t;
    }
};

z = p([ v(0, g), v(1, t), v(2, o), v(3, l), v(4, r) ], z);

function C(t) {
    return {
        optionsProvider: t,
        register(e) {
            const i = {
                ...m(),
                MessageProviderType: z,
                ValidationControllerFactoryType: LocalizedValidationControllerFactory,
                DefaultNamespace: void 0,
                DefaultKeyPrefix: void 0
            };
            t(i);
            const o = {
                DefaultNamespace: i.DefaultNamespace,
                DefaultKeyPrefix: i.DefaultKeyPrefix
            };
            return e.register(h.customize((t => {
                for (const e of Object.keys(t)) if (e in i) t[e] = i[e];
            })), n.callback(g, (() => o)));
        },
        customize(e) {
            return C(e ?? t);
        }
    };
}

const V = C(a);

export { g as I18nKeyConfiguration, x as LocalizedValidationController, LocalizedValidationControllerFactory, z as LocalizedValidationMessageProvider, V as ValidationI18nConfiguration };
//# sourceMappingURL=index.mjs.map
