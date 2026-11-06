# Distributed order-processing simulation

A Python simulation of out-of-order messages, duplicate delivery, missing events,
and stale workers. Uses only the Python standard library; no services or packages
need to be installed.

## Run

Requirements: Python 3.10 or newer.

From the repository directory:

```sh
python3 order_simulation.py
python3 -m unittest -v
```

The first command runs all three lab scenarios and prints their results. The
second runs nine automated tests, including concurrent duplicate deliveries and
all permutations of the required messages.

## Ordering and fencing rules

Each order starts at **version 0, CREATED**. Events contain `order_id`, `version`,
and `status`, for example `Event("ordering", 1, "PAID")`. Version 1 means PAID and
version 2 means SHIPPED, giving the lifecycle:

```text
v0 CREATED → v1 PAID → v2 SHIPPED
```

An event can advance an order only when its version equals the current version
plus one. Future versions wait in a per-order pending buffer. Once a gap closes,
the store drains all consecutive pending versions in order. Versions already
applied or already buffered are ignored, so retries do not apply a transition
twice. An invalid version/status combination raises `ValueError`.

Workers acquire increasing fencing tokens from the store. A token grant replaces
the current owner for that order immediately. Every delivery checks that token
against the current owner **inside the same lock** used to mutate the order and
pending buffer. Old tokens and unissued tokens are rejected before they can
modify either. Tokens are scoped to the order whose ownership was acquired.

## Recorded evidence

The following is actual output from `python3 order_simulation.py`, also saved in
[evidence/simulation.txt](evidence/simulation.txt).

### 1. Correct ordering and duplicate handling

The delivery order is **version 2, version 1, version 2 again**. The order stays
CREATED while version 1 is missing. The arrival of version 1 applies PAID first,
then the buffered SHIPPED event. The final delivery is ignored.

```text
=== 1. Out-of-order delivery: v2, v1, v2 again ===
STATE order=ordering: v0 CREATED; pending=[]
DELIVER order=ordering v2 SHIPPED; token=1
WAIT order=ordering: missing v1; pending=[2]; state=v0 CREATED
DELIVER order=ordering v1 PAID; token=1
APPLY order=ordering v1 PAID
APPLY order=ordering v2 SHIPPED
DELIVER order=ordering v2 SHIPPED; token=1
IGNORE duplicate: order=ordering v2 SHIPPED
STATE order=ordering: v2 SHIPPED; pending=[]
```

### 2. Missing-event case

Only version 2 is delivered to a separate order. Version 1 is explicitly reported
as missing and version 2 remains pending. The state never advances from CREATED.

```text
=== 2. Missing event: only v2 arrives ===
DELIVER order=missing v2 SHIPPED; token=2
WAIT order=missing: missing v1; pending=[2]; state=v0 CREATED
STATE order=missing: v0 CREATED; pending=[2]
```

### 3. Stale-worker rejection

Worker A pauses after acquiring token 3. Worker B takes over with token 4 and
successfully pays the order. A then attempts version 2, which is the correct next
version, but its old fencing token prevents the write. The state remains PAID
until B submits that event with the current token.

```text
=== 3. Stale worker: ownership changes while A is paused ===
Worker A acquires token=3, then pauses
Worker B acquires newer token=4
DELIVER order=workers v1 PAID; token=4
APPLY order=workers v1 PAID
Worker A resumes and attempts the next valid version
DELIVER order=workers v2 SHIPPED; token=3
REJECT stale/invalid worker: order=workers v2 SHIPPED; token=3, current=4
STATE order=workers: v1 PAID; pending=[]
Worker B completes the order
DELIVER order=workers v2 SHIPPED; token=4
APPLY order=workers v2 SHIPPED
STATE order=workers: v2 SHIPPED; pending=[]
```

### Test results

Full test names and results are saved in [evidence/tests.txt](evidence/tests.txt).

```text
Ran 9 tests in 0.001s

OK
```

To regenerate the evidence after making changes:

```sh
python3 order_simulation.py > evidence/simulation.txt
python3 -m unittest -v > evidence/tests.txt 2>&1
```

## Simulation scope

The demonstration uses deterministic calls to model delayed delivery and a paused
worker; sleeps and a message broker are unnecessary to reproduce those schedules.
The concurrency test also exercises simultaneous deliveries using real threads.
The store and pending buffers are in memory, so restarting the process loses
their contents. Waiting means buffering while other deliveries can continue;
there is no automatic timeout, retry, or retrieval of missing versions. A pending
event accepted from an authorized worker remains valid after ownership changes;
fencing governs new write requests. A production implementation would need durable
storage and atomic token checks and state updates at its shared storage boundary.

## Reflection: why timestamps alone are insufficient

Clocks on different machines can disagree because of clock skew, drift, or clock
adjustments. Network delays can also cause an earlier event to arrive after a
later one, so arrival time does not establish the intended state-transition
order. Equal timestamps and retried messages do not reliably identify duplicates
or reveal a missing predecessor. Per-order versions provide an explicit sequence
and expose gaps, while fencing tokens prevent a worker whose ownership has
expired from making new writes even if its timestamps appear recent.
