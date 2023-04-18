'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var i18n = require('@aurelia/i18n');
var kernel = require('@aurelia/kernel');
var runtime = require('@aurelia/runtime');
var runtimeHtml = require('@aurelia/runtime-html');
var validation = require('@aurelia/validation');
var validationHtml = require('@aurelia/validation-html');

/******************************************************************************
Copyright (c) Microsoft Corporation.

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
***************************************************************************** */

function __decorate(decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
}

function __param(paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
}

const I18N_VALIDATION_EA_CHANNEL = 'i18n:locale:changed:validation';
const I18nKeyConfiguration = /*@__PURE__*/ kernel.DI.createInterface('I18nKeyConfiguration');
exports.LocalizedValidationController = class LocalizedValidationController extends validationHtml.ValidationController {
    constructor(locator, ea, validator, parser, platform) {
        super(validator, parser, platform, locator);
        this.localeChangeSubscription = ea.subscribe(I18N_VALIDATION_EA_CHANNEL, () => { platform.domReadQueue.queueTask(async () => { await this.revalidateErrors(); }); });
    }
};
exports.LocalizedValidationController = __decorate([
    __param(0, kernel.IServiceLocator),
    __param(1, kernel.IEventAggregator),
    __param(2, validation.IValidator),
    __param(3, runtime.IExpressionParser),
    __param(4, runtimeHtml.IPlatform)
], exports.LocalizedValidationController);
class LocalizedValidationControllerFactory extends validationHtml.ValidationControllerFactory {
    construct(container, _dynamicDependencies) {
        return container.invoke(exports.LocalizedValidationController, _dynamicDependencies);
    }
}
exports.LocalizedValidationMessageProvider = class LocalizedValidationMessageProvider extends validation.ValidationMessageProvider {
    constructor(keyConfiguration, i18n, ea, parser, logger) {
        super(parser, logger, []);
        this.i18n = i18n;
        const namespace = keyConfiguration.DefaultNamespace;
        const prefix = keyConfiguration.DefaultKeyPrefix;
        if (namespace !== void 0 || prefix !== void 0) {
            this.keyPrefix = namespace !== void 0 ? `${namespace}:` : '';
            this.keyPrefix = prefix !== void 0 ? `${this.keyPrefix}${prefix}.` : this.keyPrefix;
        }
        // as this is registered singleton, disposing the subscription does not make much sense.
        ea.subscribe("i18n:locale:changed" /* Signals.I18N_EA_CHANNEL */, () => {
            this.registeredMessages = new WeakMap();
            ea.publish(I18N_VALIDATION_EA_CHANNEL);
        });
    }
    getMessage(rule) {
        const parsedMessage = this.registeredMessages.get(rule);
        if (parsedMessage !== void 0) {
            return parsedMessage;
        }
        return this.setMessage(rule, this.i18n.tr(this.getKey(rule.messageKey)));
    }
    getDisplayName(propertyName, displayName) {
        if (displayName !== null && displayName !== undefined) {
            return (displayName instanceof Function) ? displayName() : displayName;
        }
        if (propertyName === void 0) {
            return;
        }
        return this.i18n.tr(this.getKey(propertyName));
    }
    getKey(key) {
        const keyPrefix = this.keyPrefix;
        return keyPrefix !== void 0 ? `${keyPrefix}${key}` : key;
    }
};
exports.LocalizedValidationMessageProvider = __decorate([
    __param(0, I18nKeyConfiguration),
    __param(1, i18n.I18N),
    __param(2, kernel.IEventAggregator),
    __param(3, runtime.IExpressionParser),
    __param(4, kernel.ILogger)
], exports.LocalizedValidationMessageProvider);

function createConfiguration(optionsProvider) {
    return {
        optionsProvider,
        register(container) {
            const options = {
                ...validationHtml.getDefaultValidationHtmlConfiguration(),
                MessageProviderType: exports.LocalizedValidationMessageProvider,
                ValidationControllerFactoryType: LocalizedValidationControllerFactory,
                DefaultNamespace: void 0,
                DefaultKeyPrefix: void 0,
            };
            optionsProvider(options);
            const keyConfiguration = {
                DefaultNamespace: options.DefaultNamespace,
                DefaultKeyPrefix: options.DefaultKeyPrefix,
            };
            return container.register(validationHtml.ValidationHtmlConfiguration.customize((opt) => {
                // copy the customization iff the key exists in validation configuration
                for (const key of Object.keys(opt)) {
                    if (key in options) {
                        opt[key] = options[key]; // TS cannot infer that the value of the same key is being copied from A to B, and rejects the assignment due to type broadening
                    }
                }
            }), kernel.Registration.callback(I18nKeyConfiguration, () => keyConfiguration));
        },
        customize(cb) {
            return createConfiguration(cb ?? optionsProvider);
        },
    };
}
const ValidationI18nConfiguration = createConfiguration(kernel.noop);

exports.I18nKeyConfiguration = I18nKeyConfiguration;
exports.LocalizedValidationControllerFactory = LocalizedValidationControllerFactory;
exports.ValidationI18nConfiguration = ValidationI18nConfiguration;
//# sourceMappingURL=index.dev.cjs.map
