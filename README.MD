# gumloop

A small single-threaded async runtime in Rust, written from scratch to learn how Tokio works underneath. It has `spawn`, `block_on`, join handles with cancellation, timers, and socket I/O through mio, which is enough to run a TCP echo server on one thread.

It's a learning project, not something to use in production.

```bash
cargo run
```

Then connect from one or more other terminals and type something:

```bash
nc 127.0.0.1 8080
```