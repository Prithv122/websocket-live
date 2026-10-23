# Resume Bullets — WebSocket Live

Form: **action → technical specifics → measured outcome.** Numbers or it doesn't go on the resume.

---

## Bullets

- Built a WebSocket push server (FastAPI/Starlette, own fan-out hub, sequence numbers with resume from a 1,000-event ring buffer) and a vanilla-JS live dashboard; in a pre-registered 60-run single-machine benchmark with 10% of 200 clients never reading, a naive broadcast delivered 0% of 4 KB events to healthy clients while a per-client drop-oldest queue held p95 latency at 23.6 ms with 100% delivered to healthy clients.
- Designed the benchmark to avoid coordinated omission (latency from the scheduled publish time, missing messages counted as infinite) with raw-socket stalled clients, a loop-lag validity check and an escalation ladder fixed before any run; the verdict is computed by a script, and naive's pass at small message sizes was reported, not tuned away.

## Which roles this supports

- [ ] Data Scientist / ML
- [ ] AI Engineer (LLM/NLP/CV)
- [x] Data Engineer
- [x] Data Analyst / Python Developer

## Keywords this project earns

_Only list what you actually used and could be questioned on._

WebSockets · FastAPI · Starlette · asyncio · backpressure · bounded queues · load testing · p95/p99 latency · coordinated omission · pre-registration · vanilla JavaScript · SVG · reconnect with exponential backoff and jitter

---

### Bad vs good

❌ "Built a machine learning model to predict customer churn using Python."
✅ "Built a churn classifier on 240k accounts (LightGBM, 1:40 class imbalance) with isotonic calibration and cost-sensitive thresholding, lifting precision@10% from 0.31 to 0.58 over the business's existing rules baseline."

The second one is answerable in an interview. The first invites the question you can't answer.
