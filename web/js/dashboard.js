// DOM wiring: one client + store per channel, one render per animation frame.

import { createClient } from "./ws-client.js";
import { createStore } from "./store.js";
import { defaultRenderers, statusAnnouncement } from "./renderers.js";

/**
 * Coalesces any number of `request()` calls into at most one `render()` per frame, and
 * renders nothing while the page is hidden (rAF is throttled there anyway).
 */
export function createRenderScheduler(render, { raf, isHidden }) {
  let pending = false;
  return {
    request() {
      if (pending) return;
      pending = true;
      raf(() => {
        pending = false;
        if (!isHidden()) render();
      });
    },
  };
}

/**
 * @param {HTMLElement} el
 * @param {object} options
 * @param {string} options.url        WebSocket origin, e.g. `ws://127.0.0.1:8000`
 * @param {string[]} [options.channels]
 * @param {Object<string, Function[]>} [options.renderers]  channel -> renderer functions
 *   (see renderers.js); merged over the defaults, `"*"` is the fallback for other channels
 * @param {object} [options.clientOptions]  passed through to createClient
 */
export function mount(el, { url, channels = ["orders"], renderers = {}, clientOptions = {} }) {
  const doc = el.ownerDocument;
  const registry = { ...defaultRenderers, ...renderers };
  const announcer = Object.assign(doc.createElement("div"), { className: "visually-hidden" });
  announcer.setAttribute("role", "status");
  announcer.setAttribute("aria-live", "polite");
  el.replaceChildren(announcer);

  const scheduler = createRenderScheduler(renderAll, {
    raf: (fn) => requestAnimationFrame(fn),
    isHidden: () => doc.hidden,
  });

  const views = channels.map((channel) => {
    const section = doc.createElement("section");
    section.className = "channel";
    section.setAttribute("aria-labelledby", `heading-${channel}`);
    const heading = doc.createElement("h2");
    heading.id = `heading-${channel}`;
    heading.textContent = `Channel: ${channel}`;
    const grid = doc.createElement("div");
    grid.className = "panels";
    section.append(heading, grid);
    el.append(section);

    const store = createStore();
    const client = createClient({ url, channel, ...clientOptions });
    let announced = null;
    client.on("event", (env) => {
      store.add(env);
      scheduler.request();
    });
    client.on("snapshot", ({ events }) => {
      for (const env of events) store.add(env, { replay: true });
      scheduler.request();
    });
    client.on("gap", () => scheduler.request());
    client.on("status", ({ state }) => {
      if (state !== announced) {
        announced = state; // retries stay in "reconnecting"; announce the change, not each attempt
        announcer.textContent = statusAnnouncement(channel, state);
      }
      scheduler.request();
    });

    return { channel, grid, store, client, panels: [], fns: registry[channel] ?? registry["*"] };
  });

  function panelFor(view, index, out) {
    if (view.panels[index]) return view.panels[index];
    const article = doc.createElement("article");
    article.className = "panel" + (out.wide ? " panel--wide" : "");
    const h3 = doc.createElement("h3");
    h3.textContent = out.title;
    const body = doc.createElement("div");
    body.className = "panel-body";
    article.append(h3, body);
    view.grid.append(article);
    return (view.panels[index] = { body, html: null });
  }

  function renderAll() {
    const now = Date.now();
    for (const view of views) {
      const data = { channel: view.channel, now, store: view.store.snapshot(), conn: view.client.getState() };
      view.fns.forEach((fn, i) => {
        let out;
        try {
          out = fn(data);
        } catch (err) {
          console.error(`renderer ${fn.name || i} failed`, err);
          out = { title: fn.name || "Panel", html: '<p class="empty">This panel failed to render.</p>' };
        }
        const panel = panelFor(view, i, out);
        if (out.html !== panel.html) {
          panel.body.innerHTML = out.html; // renderers escape wire data
          panel.html = out.html;
        }
      });
    }
  }

  // Ages and the sparkline advance with the clock even when no event arrives.
  const tick = setInterval(scheduler.request, 1000);
  const onVisible = () => {
    if (!doc.hidden) scheduler.request();
  };
  doc.addEventListener("visibilitychange", onVisible);

  for (const view of views) view.client.start();
  scheduler.request();

  return {
    render: renderAll,
    stop() {
      clearInterval(tick);
      doc.removeEventListener("visibilitychange", onVisible);
      for (const view of views) view.client.stop();
    },
  };
}
