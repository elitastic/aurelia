import { Aurelia, CustomElement } from "@aurelia/runtime-html";
import { TestContext, assert } from "@aurelia/testing";
describe("3-runtime-html/generated/template-compiler.static.spec.ts", function () {
    function createFixture() {
        const ctx = TestContext.create();
        const au = new Aurelia(ctx.container);
        const host = ctx.createElement("div");
        return { au, host };
    }
    function verify(au, host, expected) {
        au.start();
        const outerHtmlAfterStart1 = host.outerHTML;
        assert.visibleTextEqual(host, expected, "after start #1");
        au.stop();
        const outerHtmlAfterStop1 = host.outerHTML;
        assert.visibleTextEqual(host, "", "after stop #1");
        au.start();
        const outerHtmlAfterStart2 = host.outerHTML;
        assert.visibleTextEqual(host, expected, "after start #2");
        au.stop();
        const outerHtmlAfterStop2 = host.outerHTML;
        assert.visibleTextEqual(host, "", "after stop #2");
        assert.strictEqual(outerHtmlAfterStart1, outerHtmlAfterStart2, "outerHTML after start #1 / #2");
        assert.strictEqual(outerHtmlAfterStop1, outerHtmlAfterStop2, "outerHTML after stop #1 / #2");
        au.dispose();
    }
    it("tag$01 text$01 _", function () {
        const { au, host } = createFixture();
        const App = CustomElement.define({ name: "app", template: "<template><div>a</div></template>" }, class {
        });
        const component = new App();
        au.app({ host, component });
        verify(au, host, "a");
    });
    it("tag$01 text$03 _", function () {
        const { au, host } = createFixture();
        const App = CustomElement.define({ name: "app", template: "<template><div>${msg}</div></template>" }, class {
            constructor() {
                this.msg = "a";
            }
        });
        const component = new App();
        au.app({ host, component });
        verify(au, host, "a");
    });
    it("tag$02 text$01 _", function () {
        const { au, host } = createFixture();
        const App = CustomElement.define({ name: "app", template: "<template>a</template>" }, class {
        });
        const component = new App();
        au.app({ host, component });
        verify(au, host, "a");
    });
    it("tag$02 text$03 _", function () {
        const { au, host } = createFixture();
        const App = CustomElement.define({ name: "app", template: "<template>${msg}</template>" }, class {
            constructor() {
                this.msg = "a";
            }
        });
        const component = new App();
        au.app({ host, component });
        verify(au, host, "a");
    });
    it("tag$03 text$03 _", function () {
        var _a;
        const { au, host } = createFixture();
        const MyFoo = CustomElement.define({ name: "my-foo", template: "<template>${msg}</template>" }, (_a = class {
                constructor() {
                    this.msg = "";
                    this.not = "";
                    this.item = "";
                }
            },
            _a.bindables = ["msg", "not", "item"],
            _a));
        au.register(MyFoo);
        const App = CustomElement.define({ name: "app", template: "<template><my-foo msg.bind=\"msg\"></my-foo></template>" }, class {
            constructor() {
                this.msg = "a";
            }
        });
        const component = new App();
        au.app({ host, component });
        verify(au, host, "a");
    });
    it("tag$04 text$03 _", function () {
        var _a;
        const { au, host } = createFixture();
        const MyFoo = CustomElement.define({ name: "my-foo", template: "<template>${msg}</template>" }, (_a = class {
                constructor() {
                    this.msg = "";
                    this.not = "";
                    this.item = "";
                }
            },
            _a.bindables = ["msg", "not", "item"],
            _a.containerless = true,
            _a));
        au.register(MyFoo);
        const App = CustomElement.define({ name: "app", template: "<template><my-foo msg.bind=\"msg\"></my-foo></template>" }, class {
            constructor() {
                this.msg = "a";
            }
        });
        const component = new App();
        au.app({ host, component });
        verify(au, host, "a");
    });
    it("tag$05 text$03 _", function () {
        var _a;
        const { au, host } = createFixture();
        const MyFoo = CustomElement.define({ name: "my-foo", template: "<template>${msg}</template>" }, (_a = class {
                constructor() {
                    this.msg = "";
                    this.not = "";
                    this.item = "";
                }
            },
            _a.bindables = ["msg", "not", "item"],
            _a.shadowOptions = { mode: "open" },
            _a));
        au.register(MyFoo);
        const App = CustomElement.define({ name: "app", template: "<template><my-foo msg.bind=\"msg\"></my-foo></template>" }, class {
            constructor() {
                this.msg = "a";
            }
        });
        const component = new App();
        au.app({ host, component });
        verify(au, host, "a");
    });
    it("tag$06 text$03 _", function () {
        var _a;
        const { au, host } = createFixture();
        const MyFoo = CustomElement.define({ name: "my-foo", template: "<template>${msg}</template>" }, (_a = class {
                constructor() {
                    this.msg = "";
                    this.not = "";
                    this.item = "";
                }
            },
            _a.bindables = ["msg", "not", "item"],
            _a.shadowOptions = { mode: "closed" },
            _a));
        au.register(MyFoo);
        const App = CustomElement.define({ name: "app", template: "<template><my-foo msg.bind=\"msg\"></my-foo></template>" }, class {
            constructor() {
                this.msg = "a";
            }
        });
        const component = new App();
        au.app({ host, component });
        verify(au, host, "a");
    });
});
//# sourceMappingURL=template-compiler.static.spec.js.map