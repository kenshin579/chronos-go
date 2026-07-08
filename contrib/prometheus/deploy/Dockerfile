# Build the loadgen demo from the repo root context (nested module uses a
# replace directive pointing at the parent, so the whole repo must be present).
FROM golang:1.26 AS build
WORKDIR /src
COPY . .
WORKDIR /src/contrib/prometheus
RUN CGO_ENABLED=0 go build -o /out/loadgen ./cmd/loadgen

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/loadgen /loadgen
EXPOSE 2112
ENTRYPOINT ["/loadgen"]
