# Kafka Component Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `kafka-component`, a PyPI package providing async Kafka producer/consumer components that plug into the `python-components` `Component`/`System` framework, matching the conventions of its sibling packages (`fastapi-component`, `prometheus-component`).

**Architecture:** Two independent `Component` subclasses — `KafkaProducerComponent` and `KafkaConsumerComponent` — built on `aiokafka`. A small `errors.py` module defines a pluggable `ErrorPolicy` protocol (`SkipAndLogPolicy` default, `DeadLetterPolicy` optional) decoupled from the producer via structural typing (a `Sender` protocol), so error handling never hard-imports the producer module. A shared `_routes.py` helper builds the `/health` `APIRouter` both components expose via `routes()`.

**Tech Stack:** Python ≥3.11, `aiokafka` 0.14.x, `python-components` 0.4.x, `fastapi` (for `APIRouter`), `uv` + `hatchling` + `ruff` + `pytest` + `pytest-asyncio` + `testcontainers[kafka]` 4.15.x.

**Spec:** This plan's spec is the design captured earlier in this conversation and persisted at `/home/fab/codes/kafka-component/CONTEXT.md` (glossary) and `/home/fab/codes/kafka-component/docs/adr/0001-aiokafka-client-and-delivery-semantics.md` (client + delivery-semantics decision). Executors should read both alongside this plan.

## Global Constraints

- Python `>=3.11`, CI matrix tests 3.11–3.14.
- Package manager: `uv` exclusively. No committed lockfile.
- Build backend: `hatchling`. Package lives flat at repo root (`kafka_component/`, not `src/kafka_component/`).
- Tests live **inside** the package: `kafka_component/tests/`, not a top-level `tests/`.
- Runtime dependency: `python-components>=0.4.0,<0.5` (tight upper bound, matches siblings).
- Runtime dependency: `aiokafka>=0.14,<1`.
- Runtime dependency: `fastapi>=0.115` (only for `APIRouter`/health route).
- Lint/format: `ruff format --check .` and `ruff check .` must both pass — no exceptions.
- No global state: every component instance owns its own `AIOKafkaProducer`/`AIOKafkaConsumer`, nothing module-level.
- JSON value-only serialization: message keys/headers pass through untouched; only `.value` is `json.dumps`/`json.loads`.
- Delivery semantics (ADR-0001): producer defaults `acks="all"`; consumer `enable_auto_commit=False`, commits only after the handler succeeds; `auto_offset_reset="earliest"`.
- Consumer processes messages strictly sequentially — never `asyncio.gather` over a batch.
- Config is plain constructor kwargs, never a `config` component injected via `.using()`.
- License: MIT, author "Fabricio Lima".

---

## File Structure

```
kafka_component/
  __init__.py            # public API re-exports (Task 9)
  _routes.py               # build_health_router() shared by both components (Task 3)
  errors.py                 # Sender, ErrorPolicy, SkipAndLogPolicy, DeadLetterPolicy (Task 2)
  producer.py                # KafkaProducerComponent (Task 5)
  consumer.py                 # KafkaConsumerComponent (Task 6, 7, 8)
  py.typed
  tests/
    __init__.py
    conftest.py                 # session-scoped testcontainers Kafka fixture (Task 1)
    helpers.py                   # raw aiokafka producer/consumer test utilities (Task 4)
    test_errors.py
    test_routes.py
    test_producer.py
    test_consumer.py
pyproject.toml
pytest.ini
.python-version
CHANGELOG.md
LICENSE
README.md
docker-compose.yml
.github/workflows/ci.yml
.github/workflows/publish-pypi.yml
```

---

### Task 1: Repo scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `pytest.ini`
- Create: `.python-version`
- Create: `LICENSE`
- Create: `docker-compose.yml`
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/publish-pypi.yml`
- Create: `kafka_component/__init__.py` (empty placeholder — filled in Task 9)
- Create: `kafka_component/py.typed` (empty file)
- Create: `kafka_component/tests/__init__.py` (empty)
- Create: `kafka_component/tests/conftest.py`

**Interfaces:**
- Produces: `bootstrap_servers` pytest fixture (session-scoped, `str`) that every later integration test consumes.

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "kafka-component"
version = "0.1.0"
description = "Async Kafka producer/consumer components for python-components"
readme = "README.md"
license = "MIT"
requires-python = ">=3.11"
authors = [{ name = "Fabricio Lima" }]
keywords = ["kafka", "python-components", "asyncio", "aiokafka"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: 3.14",
    "Typing :: Typed",
]
dependencies = [
    "python-components>=0.4.0,<0.5",
    "aiokafka>=0.14,<1",
    "fastapi>=0.115",
]

[project.urls]
Homepage = "https://github.com/fabriciooml/kafka-component"
Repository = "https://github.com/fabriciooml/kafka-component"
Issues = "https://github.com/fabriciooml/kafka-component/issues"
Changelog = "https://github.com/fabriciooml/kafka-component/blob/main/CHANGELOG.md"

[dependency-groups]
dev = [
    "pytest>=8",
    "pytest-asyncio>=0.24",
    "ruff<0.9",  # no committed lockfile; CI resolves fresh, pin upper bound to avoid surprise breakage
    "httpx>=0.27",
    "testcontainers[kafka]>=4.15,<5",
]

[tool.hatch.build.targets.wheel]
packages = ["kafka_component"]
exclude = ["kafka_component/tests"]
```

- [ ] **Step 2: Write `pytest.ini`**

```ini
[pytest]
testpaths = kafka_component/tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --tb=short
asyncio_mode = auto
```

- [ ] **Step 3: Write `.python-version`**

```
3.13
```

- [ ] **Step 4: Write `LICENSE`**

```
MIT License

Copyright (c) 2026 Fabricio Lima

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

- [ ] **Step 5: Write `docker-compose.yml`** (local dev test suite, apache/kafka official KRaft image)

```yaml
services:
  kafka:
    image: apache/kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

- [ ] **Step 6: Write `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
  workflow_call:

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.11", "3.12", "3.13", "3.14"]
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
        with:
          python-version: ${{ matrix.python-version }}
      - run: uv sync --all-groups
      - run: uv run pytest
      - run: uv run ruff format --check .
      - run: uv run ruff check .
```

- [ ] **Step 7: Write `.github/workflows/publish-pypi.yml`**

```yaml
name: Publish to PyPI

on:
  release:
    types: [published]
  workflow_dispatch:
    inputs:
      target:
        description: "PyPI target"
        required: true
        default: "pypi"
        type: choice
        options: ["pypi", "testpypi"]

jobs:
  ci:
    uses: ./.github/workflows/ci.yml

  publish:
    needs: ci
    runs-on: ubuntu-latest
    environment: ${{ github.event.inputs.target || 'pypi' }}
    permissions:
      id-token: write
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv build
      - uses: pypa/gh-action-pypi-publish@release/v1
        with:
          repository-url: ${{ (github.event.inputs.target == 'testpypi') && 'https://test.pypi.org/legacy/' || '' }}
```

- [ ] **Step 8: Create empty placeholder files**

```bash
mkdir -p kafka_component/tests
touch kafka_component/__init__.py
touch kafka_component/py.typed
touch kafka_component/tests/__init__.py
```

- [ ] **Step 9: Write `kafka_component/tests/conftest.py`**

```python
from __future__ import annotations

import pytest
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="session")
def kafka_container():
    with KafkaContainer("apache/kafka:latest") as container:
        yield container


@pytest.fixture(scope="session")
def bootstrap_servers(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()
```

- [ ] **Step 10: Verify scaffolding resolves and lints clean**

Run: `uv sync --all-groups && uv run ruff check . && uv run ruff format --check .`
Expected: exits 0, no errors (empty package, nothing to lint yet, but confirms `pyproject.toml`/`pytest.ini` are valid and dependencies resolve).

- [ ] **Step 11: Commit**

```bash
git init
git add pyproject.toml pytest.ini .python-version LICENSE docker-compose.yml .github kafka_component
git commit -m "chore: scaffold kafka-component repo"
```

---

### Task 2: `ErrorPolicy` protocol + `SkipAndLogPolicy`

**Files:**
- Create: `kafka_component/errors.py`
- Test: `kafka_component/tests/test_errors.py`

**Interfaces:**
- Produces: `ErrorPolicy` (Protocol, `async def handle(self, message, exc: Exception) -> None`), `SkipAndLogPolicy` (class implementing it). Later tasks (`consumer.py`) import `ErrorPolicy` and instantiate `SkipAndLogPolicy()` as the default.

- [ ] **Step 1: Write the failing test**

```python
# kafka_component/tests/test_errors.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from kafka_component.errors import SkipAndLogPolicy


@dataclass
class FakeMessage:
    topic: str
    partition: int
    offset: int
    value: Any


async def test_skip_and_log_policy_logs_and_returns(caplog):
    policy = SkipAndLogPolicy()
    message = FakeMessage(topic="orders", partition=0, offset=7, value={"id": 1})
    exc = ValueError("boom")

    with caplog.at_level(logging.ERROR):
        await policy.handle(message, exc)

    assert len(caplog.records) == 1
    record_text = caplog.records[0].getMessage()
    assert "orders" in record_text
    assert "7" in record_text
    assert "boom" in record_text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest kafka_component/tests/test_errors.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'kafka_component.errors'`

- [ ] **Step 3: Write minimal implementation**

```python
# kafka_component/errors.py
from __future__ import annotations

import logging
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class Sender(Protocol):
    async def send(self, topic: str, value: Any, *, key: bytes | str | None = None) -> None: ...


class ErrorPolicy(Protocol):
    async def handle(self, message: Any, exc: Exception) -> None: ...


class SkipAndLogPolicy:
    async def handle(self, message: Any, exc: Exception) -> None:
        logger.error(
            "kafka_component: handler failed for topic=%s partition=%s offset=%s: %s",
            getattr(message, "topic", None),
            getattr(message, "partition", None),
            getattr(message, "offset", None),
            exc,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest kafka_component/tests/test_errors.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add kafka_component/errors.py kafka_component/tests/test_errors.py
git commit -m "feat: add ErrorPolicy protocol and SkipAndLogPolicy"
```

---

### Task 3: `DeadLetterPolicy`

**Files:**
- Modify: `kafka_component/errors.py`
- Modify: `kafka_component/tests/test_errors.py`

**Interfaces:**
- Consumes: `Sender` protocol from Task 2 (`async def send(self, topic, value, *, key=None) -> None`).
- Produces: `DeadLetterPolicy(producer: Sender, dlq_topic: str | None = None)`. Later Task 8 wires this to a real `KafkaProducerComponent`.

- [ ] **Step 1: Write the failing test**

```python
# append to kafka_component/tests/test_errors.py
from kafka_component.errors import DeadLetterPolicy


class FakeSender:
    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []

    async def send(self, topic: str, value: Any, *, key=None) -> None:
        self.sent.append((topic, value))


async def test_dead_letter_policy_uses_explicit_topic():
    sender = FakeSender()
    policy = DeadLetterPolicy(sender, dlq_topic="custom.dlq")
    message = FakeMessage(topic="orders", partition=0, offset=7, value={"id": 1})
    exc = ValueError("boom")

    await policy.handle(message, exc)

    assert len(sender.sent) == 1
    topic, payload = sender.sent[0]
    assert topic == "custom.dlq"
    assert payload["original_topic"] == "orders"
    assert payload["original_partition"] == 0
    assert payload["original_offset"] == 7
    assert payload["original_value"] == {"id": 1}
    assert payload["error_type"] == "ValueError"
    assert payload["error_message"] == "boom"


async def test_dead_letter_policy_defaults_topic_from_original():
    sender = FakeSender()
    policy = DeadLetterPolicy(sender)
    message = FakeMessage(topic="orders", partition=0, offset=7, value={"id": 1})

    await policy.handle(message, ValueError("boom"))

    topic, _ = sender.sent[0]
    assert topic == "orders.DLQ"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest kafka_component/tests/test_errors.py -v`
Expected: FAIL with `ImportError: cannot import name 'DeadLetterPolicy'`

- [ ] **Step 3: Write minimal implementation**

```python
# append to kafka_component/errors.py
class DeadLetterPolicy:
    def __init__(self, producer: Sender, dlq_topic: str | None = None) -> None:
        self._producer = producer
        self._dlq_topic = dlq_topic

    async def handle(self, message: Any, exc: Exception) -> None:
        topic = self._dlq_topic or f"{message.topic}.DLQ"
        payload = {
            "original_topic": message.topic,
            "original_partition": message.partition,
            "original_offset": message.offset,
            "original_value": message.value,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
        await self._producer.send(topic, payload)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest kafka_component/tests/test_errors.py -v`
Expected: PASS, 3 passed

- [ ] **Step 5: Commit**

```bash
git add kafka_component/errors.py kafka_component/tests/test_errors.py
git commit -m "feat: add DeadLetterPolicy"
```

---

### Task 4: Shared health router (`_routes.py`)

**Files:**
- Create: `kafka_component/_routes.py`
- Create: `kafka_component/tests/test_routes.py`

**Interfaces:**
- Produces: `build_health_router(get_status: Callable[[], dict[str, Any]]) -> APIRouter`. Task 5 and Task 6 both call this from their `routes()` method.

- [ ] **Step 1: Write the failing test**

```python
# kafka_component/tests/test_routes.py
from __future__ import annotations

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from kafka_component._routes import build_health_router


async def test_health_router_returns_status_from_callback():
    def get_status() -> dict:
        return {"connected": True, "last_error": None}

    app = FastAPI()
    app.include_router(build_health_router(get_status))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"connected": True, "last_error": None}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest kafka_component/tests/test_routes.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'kafka_component._routes'`

- [ ] **Step 3: Write minimal implementation**

```python
# kafka_component/_routes.py
from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter


def build_health_router(get_status: Callable[[], dict[str, Any]]) -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    async def health() -> dict[str, Any]:
        return get_status()

    return router
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest kafka_component/tests/test_routes.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add kafka_component/_routes.py kafka_component/tests/test_routes.py
git commit -m "feat: add shared health router builder"
```

---

### Task 5: Raw Kafka test helpers

**Files:**
- Create: `kafka_component/tests/helpers.py`

**Interfaces:**
- Produces: `raw_produce(bootstrap_servers: str, topic: str, value: Any) -> None` and `raw_consume_one(bootstrap_servers: str, topic: str, *, group_id: str, timeout: float = 10.0) -> ConsumerRecord`. Tasks 6, 7, 8, 9 (integration tests) import both to independently verify producer/consumer behavior without testing the component against itself.

- [ ] **Step 1: Write `kafka_component/tests/helpers.py`** (no separate test — this is test infrastructure, exercised indirectly by every integration test that imports it)

```python
from __future__ import annotations

import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


async def raw_produce(bootstrap_servers: str, topic: str, value: Any) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    try:
        await producer.send_and_wait(topic, value=value)
    finally:
        await producer.stop()


async def raw_consume_one(
    bootstrap_servers: str,
    topic: str,
    *,
    group_id: str,
    timeout: float = 10.0,
):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    try:
        return await asyncio.wait_for(consumer.getone(), timeout=timeout)
    finally:
        await consumer.stop()
```

- [ ] **Step 2: Sanity-check helpers against the live testcontainer**

Run: `uv run python -c "
import asyncio
from kafka_component.tests.conftest import KafkaContainer
from kafka_component.tests.helpers import raw_produce, raw_consume_one

async def main():
    with KafkaContainer('apache/kafka:latest') as c:
        servers = c.get_bootstrap_server()
        await raw_produce(servers, 'sanity-topic', {'ok': True})
        msg = await raw_consume_one(servers, 'sanity-topic', group_id='sanity-group')
        assert msg.value == {'ok': True}
        print('helpers OK')

asyncio.run(main())
"`
Expected: prints `helpers OK`, exits 0.

- [ ] **Step 3: Commit**

```bash
git add kafka_component/tests/helpers.py
git commit -m "test: add raw aiokafka helpers for integration tests"
```

---

### Task 6: `KafkaProducerComponent`

**Files:**
- Create: `kafka_component/producer.py`
- Create: `kafka_component/tests/test_producer.py`

**Interfaces:**
- Consumes: `build_health_router` from `kafka_component._routes` (Task 4); `raw_consume_one` from `kafka_component.tests.helpers` (Task 5); `python_components.Component`.
- Produces: `KafkaProducerComponent(bootstrap_servers: str | list[str], client_id: str | None = None)` with `async def start()`, `async def shutdown()`, `async def send(topic: str, value: Any, *, key: bytes | str | None = None) -> None`, `def routes() -> APIRouter`, `def get_status() -> dict[str, Any]`. Task 8's `DeadLetterPolicy` wiring and any consumer test that produces fixture data may use this class.

- [ ] **Step 1: Write the failing test**

```python
# kafka_component/tests/test_producer.py
from __future__ import annotations

from kafka_component.producer import KafkaProducerComponent
from kafka_component.tests.helpers import raw_consume_one


async def test_send_publishes_json_encoded_value(bootstrap_servers):
    component = KafkaProducerComponent(bootstrap_servers=bootstrap_servers)
    await component.start()
    try:
        await component.send("producer-test-topic", {"order_id": 42})
    finally:
        await component.shutdown()

    msg = await raw_consume_one(
        bootstrap_servers, "producer-test-topic", group_id="producer-test-group"
    )
    assert msg.value == {"order_id": 42}


async def test_get_status_reflects_lifecycle(bootstrap_servers):
    component = KafkaProducerComponent(bootstrap_servers=bootstrap_servers)
    assert component.get_status() == {"connected": False, "last_error": None}

    await component.start()
    try:
        assert component.get_status() == {"connected": True, "last_error": None}
    finally:
        await component.shutdown()

    assert component.get_status()["connected"] is False


async def test_send_before_start_raises(bootstrap_servers):
    component = KafkaProducerComponent(bootstrap_servers=bootstrap_servers)
    try:
        await component.send("nope", {"x": 1})
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest kafka_component/tests/test_producer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'kafka_component.producer'`

- [ ] **Step 3: Write minimal implementation**

```python
# kafka_component/producer.py
from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter
from python_components import Component

from kafka_component._routes import build_health_router


class KafkaProducerComponent(Component):
    def __init__(
        self,
        *,
        bootstrap_servers: str | list[str],
        client_id: str | None = None,
    ) -> None:
        self.using([])
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._producer: AIOKafkaProducer | None = None
        self._started = False
        self._last_error: str | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            acks="all",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self._producer.start()
        self._started = True

    async def shutdown(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
        self._started = False

    async def send(
        self, topic: str, value: Any, *, key: bytes | str | None = None
    ) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaProducerComponent.send() called before start()")
        try:
            await self._producer.send_and_wait(topic, value=value, key=key)
        except Exception as exc:
            self._last_error = str(exc)
            raise

    def routes(self) -> APIRouter:
        return build_health_router(self.get_status)

    def get_status(self) -> dict[str, Any]:
        return {"connected": self._started, "last_error": self._last_error}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest kafka_component/tests/test_producer.py -v`
Expected: PASS, 3 passed

- [ ] **Step 5: Commit**

```bash
git add kafka_component/producer.py kafka_component/tests/test_producer.py
git commit -m "feat: add KafkaProducerComponent"
```

---

### Task 7: `KafkaConsumerComponent` core loop

**Files:**
- Create: `kafka_component/consumer.py`
- Create: `kafka_component/tests/test_consumer.py`

**Interfaces:**
- Consumes: `ErrorPolicy`, `SkipAndLogPolicy` from `kafka_component.errors` (Task 2); `build_health_router` from `kafka_component._routes` (Task 4); `raw_produce` from `kafka_component.tests.helpers` (Task 5); `python_components.Component`.
- Produces: `KafkaConsumerComponent(bootstrap_servers, group_id, topics: list[str], handler: Callable[[Any], Awaitable[None]], error_policy: ErrorPolicy | None = None, client_id: str | None = None)` with `async def start()`, `async def shutdown()`, `def routes()`, `def get_status()`. Task 8 extends `_consume_loop` on this same class (do not re-declare the class — Task 8 modifies this file).

- [ ] **Step 1: Write the failing test**

```python
# kafka_component/tests/test_consumer.py
from __future__ import annotations

import asyncio
from typing import Any

from kafka_component.consumer import KafkaConsumerComponent
from kafka_component.tests.helpers import raw_produce


async def test_consumer_receives_preexisting_messages_in_order(bootstrap_servers):
    topic = "consumer-order-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})
    await raw_produce(bootstrap_servers, topic, {"seq": 2})
    await raw_produce(bootstrap_servers, topic, {"seq": 3})

    received: list[Any] = []

    async def handler(value: Any) -> None:
        received.append(value)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="order-test-group",
        topics=[topic],
        handler=handler,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 3:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert received == [{"seq": 1}, {"seq": 2}, {"seq": 3}]


async def test_consumer_subscribes_to_multiple_topics(bootstrap_servers):
    topic_a = "multi-topic-a"
    topic_b = "multi-topic-b"
    await raw_produce(bootstrap_servers, topic_a, {"from": "a"})
    await raw_produce(bootstrap_servers, topic_b, {"from": "b"})

    received: list[Any] = []

    async def handler(value: Any) -> None:
        received.append(value)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="multi-topic-group",
        topics=[topic_a, topic_b],
        handler=handler,
    )
    await component.start()
    try:
        for _ in range(50):
            if len(received) == 2:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert {"from": "a"} in received
    assert {"from": "b"} in received
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest kafka_component/tests/test_consumer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'kafka_component.consumer'`

- [ ] **Step 3: Write minimal implementation**

```python
# kafka_component/consumer.py
from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable

from aiokafka import AIOKafkaConsumer
from fastapi import APIRouter
from python_components import Component

from kafka_component._routes import build_health_router
from kafka_component.errors import ErrorPolicy, SkipAndLogPolicy


class KafkaConsumerComponent(Component):
    def __init__(
        self,
        *,
        bootstrap_servers: str | list[str],
        group_id: str,
        topics: list[str],
        handler: Callable[[Any], Awaitable[None]],
        error_policy: ErrorPolicy | None = None,
        client_id: str | None = None,
    ) -> None:
        self.using([])
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._topics = topics
        self._handler = handler
        self._error_policy: ErrorPolicy = error_policy or SkipAndLogPolicy()
        self._client_id = client_id
        self._consumer: AIOKafkaConsumer | None = None
        self._started = False
        self._last_error: str | None = None
        self._consume_task: asyncio.Task[None] | None = None
        self._stopping: asyncio.Event = asyncio.Event()

    async def start(self) -> None:
        self._stopping = asyncio.Event()
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            client_id=self._client_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await self._consumer.start()
        self._started = True
        self._consume_task = asyncio.create_task(self._consume_loop())

    async def shutdown(self) -> None:
        self._stopping.set()
        if self._consume_task is not None:
            await self._consume_task
        if self._consumer is not None:
            await self._consumer.stop()
        self._started = False

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        while not self._stopping.is_set():
            try:
                msg = await asyncio.wait_for(self._consumer.getone(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            try:
                await self._handler(msg.value)
            except Exception as exc:
                self._last_error = str(exc)
                try:
                    await self._error_policy.handle(msg, exc)
                except Exception as policy_exc:
                    self._last_error = str(policy_exc)
                    continue

            await self._consumer.commit()

    def routes(self) -> APIRouter:
        return build_health_router(self.get_status)

    def get_status(self) -> dict[str, Any]:
        return {"connected": self._started, "last_error": self._last_error}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest kafka_component/tests/test_consumer.py -v`
Expected: PASS, 2 passed

- [ ] **Step 5: Commit**

```bash
git add kafka_component/consumer.py kafka_component/tests/test_consumer.py
git commit -m "feat: add KafkaConsumerComponent core consume loop"
```

---

### Task 8: `KafkaConsumerComponent` error policy + graceful shutdown

**Files:**
- Modify: `kafka_component/tests/test_consumer.py` (this task only adds tests — `consumer.py`'s `_consume_loop` from Task 7 already implements this behavior; the goal is to prove it)

**Interfaces:**
- Consumes: `KafkaConsumerComponent` (Task 7), `raw_produce`/`raw_consume_one` (Task 5), `SkipAndLogPolicy`, `DeadLetterPolicy` (Tasks 2, 3), `KafkaProducerComponent` (Task 6).
- Produces: nothing new for later tasks — this is a verification task.

- [ ] **Step 1: Write the failing test**

```python
# append to kafka_component/tests/test_consumer.py
from kafka_component.errors import DeadLetterPolicy
from kafka_component.producer import KafkaProducerComponent
from kafka_component.tests.helpers import raw_consume_one


async def test_handler_failure_invokes_error_policy_and_continues(bootstrap_servers):
    topic = "error-policy-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1, "fail": True})
    await raw_produce(bootstrap_servers, topic, {"seq": 2, "fail": False})

    handled: list[tuple[Any, Exception]] = []

    class SpyPolicy:
        async def handle(self, message: Any, exc: Exception) -> None:
            handled.append((message.value, exc))

    processed: list[Any] = []

    async def handler(value: Any) -> None:
        if value["fail"]:
            raise ValueError("handler exploded")
        processed.append(value)

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="error-policy-group",
        topics=[topic],
        handler=handler,
        error_policy=SpyPolicy(),
    )
    await component.start()
    try:
        for _ in range(50):
            if len(processed) == 1 and len(handled) == 1:
                break
            await asyncio.sleep(0.2)
    finally:
        await component.shutdown()

    assert processed == [{"seq": 2, "fail": False}]
    assert len(handled) == 1
    assert handled[0][0] == {"seq": 1, "fail": True}
    assert isinstance(handled[0][1], ValueError)


async def test_dead_letter_policy_republishes_failed_message(bootstrap_servers):
    topic = "dlq-source-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1, "fail": True})

    producer = KafkaProducerComponent(bootstrap_servers=bootstrap_servers)
    await producer.start()

    async def handler(value: Any) -> None:
        raise ValueError("boom")

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="dlq-test-group",
        topics=[topic],
        handler=handler,
        error_policy=DeadLetterPolicy(producer),
    )
    await component.start()
    try:
        dlq_msg = await raw_consume_one(
            bootstrap_servers, f"{topic}.DLQ", group_id="dlq-verify-group", timeout=15.0
        )
    finally:
        await component.shutdown()
        await producer.shutdown()

    assert dlq_msg.value["original_topic"] == topic
    assert dlq_msg.value["original_value"] == {"seq": 1, "fail": True}
    assert dlq_msg.value["error_type"] == "ValueError"


async def test_shutdown_drains_in_flight_message_before_stopping(bootstrap_servers):
    topic = "drain-topic"
    await raw_produce(bootstrap_servers, topic, {"seq": 1})

    started_processing = asyncio.Event()
    finished_processing = asyncio.Event()

    async def handler(value: Any) -> None:
        started_processing.set()
        await asyncio.sleep(1.0)
        finished_processing.set()

    component = KafkaConsumerComponent(
        bootstrap_servers=bootstrap_servers,
        group_id="drain-group",
        topics=[topic],
        handler=handler,
    )
    await component.start()
    await asyncio.wait_for(started_processing.wait(), timeout=10.0)

    await component.shutdown()

    assert finished_processing.is_set()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest kafka_component/tests/test_consumer.py -v -k "error_policy or dead_letter or drains"`
Expected: FAIL — `ImportError: cannot import name 'DeadLetterPolicy'` is already resolved from Task 3, so this should actually run; if it fails, it means Task 7's `_consume_loop` doesn't yet call `self._error_policy.handle` or doesn't drain correctly. Confirm the failure message points at behavior, not imports, before proceeding.

- [ ] **Step 3: Fix implementation if needed**

The `_consume_loop` written in Task 7 already implements the try/except-around-handler, `error_policy.handle`, and the "await the in-flight `getone()`/handler pair to completion before re-checking `self._stopping`" drain behavior. If Step 2 fails, re-read `kafka_component/consumer.py`'s `_consume_loop` against this task's three tests and adjust so that:
1. A handler exception calls `self._error_policy.handle(msg, exc)` and the loop proceeds to the next message (not raise).
2. `DeadLetterPolicy.handle` (called from `_consume_loop`) successfully publishes to `{topic}.DLQ` via the injected `Sender`.
3. `shutdown()` only returns once the currently-awaited `handler(msg.value)` call has completed — never mid-handler.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest kafka_component/tests/test_consumer.py -v`
Expected: PASS, 5 passed (2 from Task 7 + 3 from this task)

- [ ] **Step 5: Commit**

```bash
git add kafka_component/tests/test_consumer.py
git commit -m "test: verify consumer error policy wiring and graceful shutdown drain"
```

---

### Task 9: Public API + README + CHANGELOG

**Files:**
- Modify: `kafka_component/__init__.py`
- Create: `README.md`
- Create: `CHANGELOG.md`

**Interfaces:**
- Consumes: `KafkaProducerComponent` (Task 6), `KafkaConsumerComponent` (Task 7), `ErrorPolicy`, `SkipAndLogPolicy`, `DeadLetterPolicy`, `Sender` (Tasks 2, 3).
- Produces: the package's importable public surface — nothing downstream in this plan, but this is what real consumers of the library will `import`.

- [ ] **Step 1: Write the failing test**

```python
# run inline, not a persisted test file (Steps 2 below is the check)
```

- [ ] **Step 2: Write `kafka_component/__init__.py`**

```python
from __future__ import annotations

from kafka_component.consumer import KafkaConsumerComponent
from kafka_component.errors import DeadLetterPolicy, ErrorPolicy, Sender, SkipAndLogPolicy
from kafka_component.producer import KafkaProducerComponent

__all__ = [
    "DeadLetterPolicy",
    "ErrorPolicy",
    "KafkaConsumerComponent",
    "KafkaProducerComponent",
    "Sender",
    "SkipAndLogPolicy",
]
```

- [ ] **Step 3: Verify the public surface imports cleanly**

Run: `uv run python -c "import kafka_component as k; assert sorted(k.__all__) == ['DeadLetterPolicy', 'ErrorPolicy', 'KafkaConsumerComponent', 'KafkaProducerComponent', 'Sender', 'SkipAndLogPolicy']; print('exports OK')"`
Expected: prints `exports OK`, exits 0

- [ ] **Step 4: Write `README.md`**

```markdown
# kafka-component

Async Kafka producer/consumer components for [python-components](https://github.com/lucassant95/python-components).

## Install

```bash
uv add kafka-component
```

## Requirements

- Python >= 3.11
- A running Kafka broker (KRaft or ZooKeeper mode)

## Usage

```python
from python_components import System
from kafka_component import KafkaProducerComponent, KafkaConsumerComponent, DeadLetterPolicy

producer = KafkaProducerComponent(bootstrap_servers="localhost:9092")

async def handle_order(value):
    print("received order", value)

consumer = KafkaConsumerComponent(
    bootstrap_servers="localhost:9092",
    group_id="orders-service",
    topics=["orders"],
    handler=handle_order,
    error_policy=DeadLetterPolicy(producer, dlq_topic="orders.DLQ"),
)

system = System({"producer": producer, "consumer": consumer})

async with system:
    await producer.send("orders", {"order_id": 1})
```

## Semantics and caveats

- **At-least-once delivery.** The consumer commits offsets only after the handler succeeds. A crash between a successful handler call and the commit can redeliver a message — handlers should be idempotent if that matters.
- **Sequential processing.** One message is handled at a time, in partition order. There is no built-in concurrency; scale via more consumer instances/partitions, not intra-instance parallelism.
- **Not a supervisor.** On handler failure, the configured `ErrorPolicy` runs (default `SkipAndLogPolicy`, or `DeadLetterPolicy`) and the loop continues — the component itself does not retry, crash-loop, or restart. Health is exposed via `routes()`; process supervision is the caller's job.
- **JSON value-only.** Message values are `json.dumps`/`json.loads`. Keys and headers pass through untouched.
- **Graceful shutdown drains the in-flight message.** `shutdown()` waits for the currently-processing message's handler (and its `ErrorPolicy`, if it fails) to finish before stopping — it does not abandon work mid-message.

## Development

```bash
uv sync --all-groups
uv run pytest
uv run ruff format --check .
uv run ruff check .
```

Integration tests spin up a real Kafka broker via [testcontainers](https://testcontainers.com/modules/kafka/). For local development against a persistent broker instead, run `docker compose up -d`.

## License

MIT
```

- [ ] **Step 5: Write `CHANGELOG.md`**

```markdown
# Changelog

## 0.1.0

- Initial release: `KafkaProducerComponent`, `KafkaConsumerComponent`, `ErrorPolicy` (`SkipAndLogPolicy`, `DeadLetterPolicy`).
```

- [ ] **Step 6: Run full test suite + lint one final time**

Run: `uv run pytest && uv run ruff format --check . && uv run ruff check .`
Expected: all tests pass, both ruff checks exit 0

- [ ] **Step 7: Commit**

```bash
git add kafka_component/__init__.py README.md CHANGELOG.md
git commit -m "docs: add public API exports, README, and CHANGELOG"
```
