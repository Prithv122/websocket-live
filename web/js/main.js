import { mount } from "./dashboard.js";
import { originFromLocation } from "./ws-client.js";

const root = document.getElementById("app");
const channel = new URLSearchParams(location.search).get("channel") ?? "orders";

if (/^[a-z0-9_-]{1,32}$/.test(channel)) {
  mount(root, {
    url: originFromLocation(location),
    channels: [channel],
    // Channels need no server change. /?channel=windows shows the streaming pipeline's window
    // records (see renderers.js); another channel can pass `renderers: { name: [...] }` here.
  });
} else {
  root.textContent = "Invalid channel name: use 1-32 characters from a-z, 0-9, _ and -.";
}
