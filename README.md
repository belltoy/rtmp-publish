RTMP Publish Bench Tool
=======================

This tool read flv packages from a specified file and push it to destinations from list or generated path, concurrently.

## Build

```
cargo build
```

## Run

```
cargo run -- -h
```

### Examples

```
cargo run -- -i ~/Videos/BigBuckBunny_320x180.flv -c 100 -p rtmp://localhost:1935/test/stream-
```

Or you can read target RTMP urls list from generated file:

```
> cat target.list
rtmp://example.com/app/stream_a
rtmp://example.com/app/stream_b
rtmp://example.com/app/stream_c
rtmp://example.com/app/stream_d
rtmp://example.com/app/stream_e
rtmp://example.com/app/stream_f

> cargo run -- -i ~/Videos/BigBuckBunny_320x180.flv target.list
```

## License

MIT
