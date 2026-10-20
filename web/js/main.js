import { mount } from "./dashboard.js";
import { originFromLocation } from "./ws-client.js";

const root = document.getElementById("app");
const channel = new URLSearchParams(location.search).get("channel") ?? "orders";

if (/^[a-z0-9_-]{1,32}$/.test(channel)) {
  mount(root, {
    url: originFromLocation(location),
    channels: [channel],
    // A later project can add a channel with no server change, e.g. windowed aggregates:
    //   renderers: { windows: [windowsTable, connectionPanel] },
    // where windowsTable({store}) reads the `type: "window"` events
    // ({window_start, window_end, zone, orders, revenue, late_dropped}) and returns
    // {id: "windows", title: "Windows", html}. Then open /?channel=windows.
  });
} else {
  root.textContent = "Invalid channel name: use 1-32 characters from a-z, 0-9, _ and -.";
}
