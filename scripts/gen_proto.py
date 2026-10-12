"""Generate the Python gRPC stubs from proto/ into python/lookupsvc/gen.

Also builds the frozen v1 contract into python/tests/legacy_gen for the
compatibility tests. Run from anywhere: python scripts/gen_proto.py
"""
import re
import sys
from pathlib import Path

from grpc_tools import protoc

ROOT = Path(__file__).resolve().parent.parent
TARGETS = [
    (ROOT / "proto", ["lookup.proto", "table.proto"], ROOT / "python" / "lookupsvc" / "gen"),
    (ROOT / "proto-v1", ["lookup_v1.proto"], ROOT / "python" / "tests" / "legacy_gen"),
]


def generate(proto_dir: Path, files: list[str], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "__init__.py").write_text("")
    args = ["protoc", f"-I{proto_dir}", f"--python_out={out}", f"--grpc_python_out={out}"]
    args += [str(proto_dir / f) for f in files]
    if protoc.main(args) != 0:
        sys.exit(f"protoc failed for {files}")
    # grpc_tools emits absolute sibling imports; make them package-relative.
    for stub in out.glob("*_pb2_grpc.py"):
        text = stub.read_text()
        text = re.sub(r"^import (\w+_pb2) as", r"from . import \1 as", text, flags=re.M)
        stub.write_text(text)


if __name__ == "__main__":
    for proto_dir, files, out in TARGETS:
        generate(proto_dir, files, out)
        print("generated", ", ".join(files), "->", out.relative_to(ROOT))
