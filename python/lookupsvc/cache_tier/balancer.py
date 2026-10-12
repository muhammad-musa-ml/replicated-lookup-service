import threading
import time
from dataclasses import dataclass, field

import grpc

from ..gen import lookup_pb2_grpc as pb_grpc

CHANNEL_OPTIONS = [
    ("grpc.max_receive_message_length", 32 * 1024 * 1024),
    ("grpc.max_send_message_length", 32 * 1024 * 1024),
    ("grpc.initial_reconnect_backoff_ms", 200),
    ("grpc.max_reconnect_backoff_ms", 2000),
    ("grpc.keepalive_time_ms", 10000),
    ("grpc.keepalive_timeout_ms", 3000),
]


class AllReplicasDown(Exception):
    def __init__(self, attempts):
        super().__init__("all replicas unavailable")
        self.attempts = attempts


class UpstreamError(Exception):
    """A gRPC failure that is not 'replica unreachable'. These are not retried on
    another replica: the request reached a server and it answered with an error."""

    def __init__(self, replica, code, details):
        super().__init__(f"{replica}: {code.name} {details}")
        self.replica = replica
        self.code = code
        self.details = details


@dataclass
class Replica:
    name: str
    target: str
    stub: object
    channel: object = field(default=None, repr=False)
    down_until: float = 0.0
    suspect: bool = False  # failed last time; try it with a short deadline
    state: object = grpc.ChannelConnectivity.READY

    def on_state(self, state):
        self.state = state

    def close(self):
        if self.channel is not None:
            self.channel.unsubscribe(self.on_state)
            self.channel.close()


def connect(name: str, target: str) -> Replica:
    channel = grpc.insecure_channel(target, options=CHANNEL_OPTIONS)
    replica = Replica(name, target, pb_grpc.LookupStub(channel), channel=channel)
    replica.state = grpc.ChannelConnectivity.IDLE
    # try_to_connect keeps the channel dialing in the background, so a healthy
    # replica is READY before the first request and a returning one is picked up
    channel.subscribe(replica.on_state, try_to_connect=True)
    return replica


@dataclass
class Answer:
    reply: object
    replica: str
    attempts: list  # replicas that were unreachable before this one answered


class Balancer:
    """Round-robin over the replicas with failover.

    A call starts at the next replica in the rotation. If that replica is
    unreachable it is skipped for `cooldown` seconds (still tried last, in case
    everything is marked down) and the call moves to the next one. Unreachable
    means gRPC UNAVAILABLE, or DEADLINE_EXCEEDED on a channel that never got
    connected: some networks (a Windows loopback, a container whose address just
    vanished) swallow the refusal and the call only ever sees its deadline. Any
    other gRPC error is raised as UpstreamError without trying another replica.

    A replica that failed last time, or whose channel is not connected, is tried
    with `probe_timeout` instead of the full deadline, so a dead one costs a
    request at most that much."""

    def __init__(self, replicas, cooldown: float = 5.0, probe_timeout: float = 0.5,
                 clock=time.monotonic, on_error=None):
        if not replicas:
            raise ValueError("need at least one replica")
        self.replicas = list(replicas)
        self.cooldown = cooldown
        self.probe_timeout = probe_timeout
        self._clock = clock
        self._on_error = on_error or (lambda replica, code: None)
        self._next = 0
        self._lock = threading.Lock()

    def _order(self):
        with self._lock:
            start = self._next % len(self.replicas)
            self._next += 1
            now = self._clock()
            rotated = self.replicas[start:] + self.replicas[:start]
            up = [r for r in rotated if r.down_until <= now]
            down = [r for r in rotated if r.down_until > now]
        return up + down

    def _unreachable(self, replica, code) -> bool:
        if code == grpc.StatusCode.UNAVAILABLE:
            return True
        return code == grpc.StatusCode.DEADLINE_EXCEEDED and replica.state != grpc.ChannelConnectivity.READY

    def call(self, method: str, request, timeout: float) -> Answer:
        attempts = []
        for replica in self._order():
            # a channel that is not connected right now (the replica's connection
            # just dropped, or it never came up) gets the short deadline too
            shaky = replica.suspect or replica.state != grpc.ChannelConnectivity.READY
            wait = min(timeout, self.probe_timeout) if shaky else timeout
            try:
                reply = getattr(replica.stub, method)(request, timeout=wait)
            except grpc.RpcError as e:
                code = e.code()
                self._on_error(replica.name, code)
                if self._unreachable(replica, code):
                    with self._lock:
                        replica.down_until = self._clock() + self.cooldown
                        replica.suspect = True
                    attempts.append(replica.name)
                    continue
                raise UpstreamError(replica.name, code, e.details()) from e
            with self._lock:
                replica.down_until = 0.0
                replica.suspect = False
            return Answer(reply, replica.name, attempts)
        raise AllReplicasDown(attempts)

    def states(self):
        now = self._clock()
        with self._lock:
            return {r.name: ("down" if r.down_until > now else "up") for r in self.replicas}

    def close(self):
        for r in self.replicas:
            r.close()
