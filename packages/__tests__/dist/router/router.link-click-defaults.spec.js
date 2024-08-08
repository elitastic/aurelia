import { IRouter, RouterConfiguration } from '@aurelia/router';
import { Aurelia, CustomElement } from '@aurelia/runtime-html';
import { MockBrowserHistoryLocation, TestContext, assert } from '@aurelia/testing';
describe('router/router.link-click-defaults.spec.ts', function () {
    var _a, _b, _c;
    function getModifiedRouter(container) {
        const router = container.get(IRouter);
        const mockBrowserHistoryLocation = new MockBrowserHistoryLocation();
        mockBrowserHistoryLocation.changeCallback = async (ev) => { router.viewer.handlePopStateEvent(ev); };
        router.viewer.history = mockBrowserHistoryLocation;
        router.viewer.location = mockBrowserHistoryLocation;
        return router;
    }
    function spyNavigationStates(router, spy) {
        let _pushState;
        let _replaceState;
        if (spy) {
            _pushState = router.viewer.location.pushState;
            router.viewer.location.pushState = function (data, title, path) {
                spy('push', data, title, path);
                _pushState.call(router.viewer.location, data, title, path);
            };
            _replaceState = router.viewer.location.replaceState;
            router.viewer.location.replaceState = function (data, title, path) {
                spy('replace', data, title, path);
                _replaceState.call(router.viewer.location, data, title, path);
            };
        }
        return { _pushState, _replaceState };
    }
    function unspyNavigationStates(router, _push, _replace) {
        if (_push) {
            router.viewer.location.pushState = _push;
            router.viewer.location.replaceState = _replace;
        }
    }
    async function $setup(config, dependencies = [], routes = [], stateSpy = void 0) {
        var _d;
        const ctx = TestContext.create();
        const { container, platform } = ctx;
        const App = CustomElement.define({
            name: 'app',
            template: '<au-viewport name="app"></au-viewport>',
            dependencies
        }, (_d = class {
            },
            _d.routes = routes,
            _d));
        const host = ctx.doc.createElement('div');
        ctx.doc.body.appendChild(host);
        const au = new Aurelia(container)
            .register(RouterConfiguration.customize(config ?? {}), App)
            .app({ host: host, component: App });
        const router = getModifiedRouter(container);
        const { _pushState, _replaceState } = spyNavigationStates(router, stateSpy);
        await au.start();
        async function $teardown() {
            unspyNavigationStates(router, _pushState, _replaceState);
            RouterConfiguration.customize();
            await au.stop(true);
            ctx.doc.body.removeChild(host);
            au.dispose();
        }
        return { ctx, container, platform, host, au, router, $teardown, App };
    }
    this.timeout(30000);
    const GrandChild = CustomElement.define({
        name: 'grandchild',
        template: '!grandchild!',
    }, class GrandChild {
        loading() {
            return new Promise((resolve) => {
                setTimeout(() => resolve(0), 100);
            });
        }
    });
    const Child = CustomElement.define({
        name: 'child',
        template: '!child!<au-viewport></au-viewport>',
    }, (_a = class Child {
        },
        _a.routes = [
            {
                path: '',
                component: Promise.resolve({ GrandChild }),
            },
            {
                path: 'grandchild',
                component: Promise.resolve({ GrandChild }),
            },
        ],
        _a));
    const Parent = CustomElement.define({
        name: 'parent',
        template: '!parent!<au-viewport></au-viewport>',
    }, (_b = class Parent {
        },
        _b.routes = [
            {
                path: 'child',
                component: Promise.resolve({ Child }),
            },
        ],
        _b));
    const tests = [
        { load: '/parent/child', result: '!parent!!child!!grandchild!', },
    ];
    const Nav = CustomElement.define({
        name: 'nav', template: `
    <style>.active { background-color: gold; }</style>

    ${tests.map(test => `<a load="${test.load}">${test.load}).</a>`).join('\n')}

    <au-viewport name="nav-vp"></au-viewport>
  `,
    }, (_c = class Nav {
        },
        _c.routes = [
            {
                path: 'parent',
                component: Promise.resolve({ Parent }),
            },
        ],
        _c));
    for (let i = 0; i < tests.length; i++) {
        const test = tests[i];
        it(`can load all components, including defaults, for link "${test.load}"`, async function () {
            const { platform, host, router, $teardown } = await $setup({}, [Nav, Parent, Child, GrandChild]);
            await $load('/nav', router, platform);
            await platform.domQueue.yield();
            const links = host.getElementsByTagName('A');
            const link = links[i];
            link.click();
            await platform.domQueue.yield();
            await new Promise((resolve) => { setTimeout(() => resolve(0), 200); });
            assert.includes(host.textContent, test.result, test.load);
            for (const l of links) {
                assert.strictEqual(l.classList.contains('active'), l === link, `${l.innerText}: ${l.classList.contains('active')}`);
            }
            await $teardown();
        });
    }
});
const $load = async (path, router, platform) => {
    await router.load(path);
    platform.domQueue.flush();
};
//# sourceMappingURL=router.link-click-defaults.spec.js.map