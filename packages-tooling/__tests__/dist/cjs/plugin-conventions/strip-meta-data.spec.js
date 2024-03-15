"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const runtime_html_1 = require("@aurelia/runtime-html");
const plugin_conventions_1 = require("@aurelia/plugin-conventions");
const assert = require("assert");
describe('stripMetaData', function () {
    it('returns empty html', function () {
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(' '), {
            aliases: [],
            html: ' ',
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips import tag', function () {
        const html = `<import from="./a"></import>
<template>
  <p></p>
</template>
`;
        const expected = `
<template>
  <p></p>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: ['./a'],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips import tags with wrong or missing close tag', function () {
        const html = `<import from="./a" />
<import from="b"></import>
<template>
  <import from="./c.css">
  <p></p>
</template>
`;
        const expected = `

<template>
${'  ' /* leading space is untouched */}
  <p></p>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: ['./a', 'b', './c.css'],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips require tag', function () {
        const html = `<require from="./a"></require>
<template>
  <p></p>
</template>
`;
        const expected = `
<template>
  <p></p>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: ['./a'],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips mixed import/require tags with wrong or missing close tag', function () {
        const html = `<import from="./a" /><import from="foo">
<require from="b"></require>
<template>
  <require from="./c.css">
  <p></p>
</template>
`;
        const expected = `

<template>
${'  ' /* leading space is untouched */}
  <p></p>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: ['./a', 'foo', 'b', './c.css'],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips import/require tags with alias', function () {
        const html = `<import from="./a" as="b"/>
<template><p></p></template>
`;
        const expected = `
<template><p></p></template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: ['./a'],
            depsAliases: { './a': { __MAIN__: 'b' } },
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips import/require tags with named alias', function () {
        const html = `<import from="./a" a.as="b"/>
<template><p></p></template>
`;
        const expected = `
<template><p></p></template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: ['./a'],
            depsAliases: { './a': { __MAIN__: null, a: 'b' } },
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips use-shadow-dom tag', function () {
        const html = `<use-shadow-dom></use-shadow-dom>
<template>
</template>
`;
        const expected = `
<template>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: 'open',
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips use-shadow-dom tag with mode attribute', function () {
        const html = `<use-shadow-dom mode="closed">
<template>
<require from="./a"></require>
<slot></slot>
</template>
`;
        const expected = `
<template>

<slot></slot>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: 'closed',
            deps: ['./a'],
            depsAliases: {},
            containerless: false,
            capture: false,
            hasSlot: true,
            bindables: {}
        });
    });
    it('strips use-shadow-dom attribute', function () {
        const html = `<template use-shadow-dom>
</template>
`;
        const expected = `<template >
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: 'open',
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips use-shadow-dom attribute with explicit mode', function () {
        const html = `<template use-shadow-dom="closed">
<require from="./a"></require>
</template>
`;
        const expected = `<template >

</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: 'closed',
            deps: ['./a'],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips containerless tag', function () {
        const html = `<containerless></containerless>
<template>
</template>
`;
        const expected = `
<template>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: true,
            capture: false,
            hasSlot: false,
            bindables: {}
        });
    });
    it('strips containerless tag without closing tag', function () {
        const html = `<containerless>
<template>
</template>
`;
        const expected = `
<template>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: true,
            capture: false,
            hasSlot: false,
            bindables: {}
        });
    });
    it('strips containerless attribute', function () {
        const html = `<template containerless>
</template>
`;
        const expected = `<template >
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: true,
            capture: false,
            hasSlot: false,
            bindables: {}
        });
    });
    it('strips capture with only open tag', function () {
        const input = `<capture>`;
        const expected = ``;
        const { html, capture } = (0, plugin_conventions_1.stripMetaData)(input);
        assert.deepStrictEqual({ html, capture }, { html: expected, capture: true });
    });
    it('strips capture with self closing tag', function () {
        const input = `<capture/>`;
        const expected = ``;
        const { html, capture } = (0, plugin_conventions_1.stripMetaData)(input);
        assert.deepStrictEqual({ html, capture }, { html: expected, capture: true });
    });
    it('strips capture with closing tag', function () {
        const input = `<capture></capture>`;
        const expected = ``;
        const { html, capture } = (0, plugin_conventions_1.stripMetaData)(input);
        assert.deepStrictEqual({ html, capture }, { html: expected, capture: true });
    });
    it('strips capture attribute on template', function () {
        const input = `<template capture></template>`;
        const expected = `<template ></template>`;
        const { html, capture } = (0, plugin_conventions_1.stripMetaData)(input);
        assert.deepStrictEqual({ html, capture }, { html: expected, capture: true });
    });
    it('strips capture attribute, ignores value on template', function () {
        const input = `<template capture=false></template>`;
        const expected = `<template ></template>`;
        const { html, capture } = (0, plugin_conventions_1.stripMetaData)(input);
        assert.deepStrictEqual({ html, capture }, { html: expected, capture: true });
    });
    it('strips bindable tag', function () {
        const html = `<bindable name="firstName"></bindable>
<template>
</template>
`;
        const expected = `
<template>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: { firstName: {} }
        });
    });
    it('strips bindable tag with more attrs', function () {
        const html = `<bindable name="firstName" mode="one-way">
<bindable name="lastName" mode="TwoWay" attribute="surname">
<bindable name="foo" mode="one_time"></bindable>
<bindable name="bar" mode="toView">
<bindable name="lo" mode="from-view"></bindable>
<template>
</template>
`;
        const expected = `




<template>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {
                firstName: { mode: runtime_html_1.BindingMode.toView },
                lastName: { mode: runtime_html_1.BindingMode.twoWay, attribute: 'surname' },
                foo: { mode: runtime_html_1.BindingMode.oneTime },
                bar: { mode: runtime_html_1.BindingMode.toView },
                lo: { mode: runtime_html_1.BindingMode.fromView }
            }
        });
    });
    it('strips bindable attribute', function () {
        const html = `<template bindable="firstName">
</template>
`;
        const expected = `<template >
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: { firstName: {} }
        });
    });
    it('strips bindable attribute with multiple names', function () {
        const html = `<template bindable="firstName,lastName">
</template>
`;
        const expected = `<template >
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: { firstName: {}, lastName: {} }
        });
    });
    it('strips bindable attribute with multiple names with spaces', function () {
        const html = `<template bindable="firstName,
                                      lastName,
                                      age">
</template>
`;
        const expected = `<template >
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: { firstName: {}, lastName: {}, age: {} }
        });
    });
    it('strips alias attribute with multiple names with spaces', function () {
        const html = `<template alias="firstName,
                                   alias">
</template>
`;
        const expected = `<template >
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: ['firstName', 'alias'],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips alias attribute with multiple names', function () {
        const html = `<template alias="firstName,                alias">
</template>
`;
        const expected = `<template >
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: ['firstName', 'alias'],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips alias attribute with single name', function () {
        const html = `<template alias="firstName">
</template>
`;
        const expected = `<template >
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: ['firstName'],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips alias node with single name', function () {
        const html = `<alias name="firstName"><template>
</template>
`;
        const expected = `<template>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: ['firstName'],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips alias node with multiple name', function () {
        const html = `<alias name="firstName,alias"><template>
</template>
`;
        const expected = `<template>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: ['firstName', 'alias'],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips alias node and attr combo', function () {
        const html = `<alias name="firstName,alias"><template alias="firstName2,            alias2">
</template>
`;
        const expected = `<template >
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: ['firstName', "alias", "firstName2", "alias2"],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('strips alias node with no name', function () {
        const html = `<alias><template>
</template>
`;
        const expected = `<template>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: {}
        });
    });
    it('checks slot tag', function () {
        const html = `<template>
  <slot></slot>
</template>
`;
        const expected = `<template>
  <slot></slot>
</template>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            capture: false,
            hasSlot: true,
            bindables: {}
        });
    });
    it('skip <template as-custom-element>', function () {
        const html = `<bindable name="firstName"></bindable>
<template as-custom-element="foo-bar">
  <bindable property="foo"></bindable>
  <p>\${foo}</p>
</template>
<foo-bar foo="Hello"></foo-bar>
`;
        const expected = `
<template as-custom-element="foo-bar">
  <bindable property="foo"></bindable>
  <p>\${foo}</p>
</template>
<foo-bar foo="Hello"></foo-bar>
`;
        assert.deepEqual((0, plugin_conventions_1.stripMetaData)(html), {
            aliases: [],
            html: expected,
            shadowMode: null,
            deps: [],
            depsAliases: {},
            containerless: false,
            hasSlot: false,
            capture: false,
            bindables: { firstName: {} }
        });
    });
});
//# sourceMappingURL=strip-meta-data.spec.js.map