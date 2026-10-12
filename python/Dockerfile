# build context is the repository root; one image runs the Python dataset
# replica, the cache tier, the table service and the data fetch
FROM python:3.13-slim AS build
RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
WORKDIR /src
COPY proto proto
COPY scripts/gen_proto.py scripts/gen_proto.py
COPY python python
RUN pip install --no-cache-dir grpcio-tools \
    && python scripts/gen_proto.py --skip-legacy \
    && pip install --no-cache-dir "./python[cache]"

FROM python:3.13-slim
COPY --from=build /opt/venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1
