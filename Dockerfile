FROM amazonlinux:2023 AS base
COPY --from=ghcr.io/astral-sh/uv:0.12.3 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1

RUN dnf -y install python3.11 python3.11-devel cmake gcc gcc-c++ make geos geos-devel \
    && dnf clean all \
    && rm -rf /var/cache/dnf
RUN ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && ln -sf /usr/bin/python3.11 /usr/bin/python

FROM base AS build

RUN mkdir /app
WORKDIR /app/
RUN uv venv -p 3.11
ENV PATH="/app/.venv/bin:$PATH"
# appease exact-extract with the special scikit-build-core version
# RUN echo "scikit-build-core<0.10" > /tmp/build-constraints.txt && \
#     uv pip install -r pyproject.toml --build-constraint /tmp/build-constraints.txt
COPY . .
RUN uv build \
    && echo "scikit-build-core<0.10" > /tmp/build-constraints.txt \
    && uv pip install dist/ngiab_data_preprocess-*.whl --build-constraint /tmp/build-constraints.txt

FROM amazonlinux:2023 AS runtime

RUN dnf -y install python3.11 \
    && dnf clean all \
    && rm -rf /var/cache/dnf

COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
