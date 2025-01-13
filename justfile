# run the tests
test:
    cargo test --tests -- --nocapture

# run the tests with cargo-watch
test-watch :
    cargo watch -x 'test --tests -- --nocapture'

# run the tests with coverage
test-coverage:
    cargo tarpaulin --tests

# update the dependencies to the latest version
deps-update:
    cargo upgrade -i allow && cargo update

# run clippy
format:
    cargo fmt

# build the program
build:
    cargo build

# fix all clippy warnings
fix: 
    cargo fix --allow-dirty --allow-staged

example-basic:
    cargo run --example basic

example-deduping:
    cargo run --example deduping