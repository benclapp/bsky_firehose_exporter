# Bsky Firehose Exporter

This is a Prometheus Exporter for the Bluesky firehose. This is probably a bad idea to collect
these metrics this particular way. But it's possible, and that's cool in itself!

This mostly exists to experiment with the Firehose as a first dable with it.

## Docker

Images are available from this repo's container registry.

```
docker run --rm -p 10025:10025 ghcr.io/benclapp/bsky_firehose_exporter:latest
```
