# Sprinter 👟

[![Crates.io](https://img.shields.io/crates/v/sprinter.svg)](https://crates.io/crates/sprinter)
[![Documentation](https://docs.rs/sprinter/badge.svg)](https://docs.rs/sprinter)

A Rust library for running parallel queued tasks

## Features

- 🔄 Queue tasks and run them in parallel according to concurrency limits
- ⚡ Extremely low execution overhead
- 🔍 Deduplication of tasks

## Installation

```bash
cargo add sprinter
```

Or add to your `Cargo.toml`:

```toml
[dependencies]
sprinter = "0.3.0"
```

## Usage

### Basic Example

[Here's a basic example](example/basic.rs) of using Sprinter to run parallel tasks:

```rust
use sprinter::Queue;
use std::fmt::Error;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("sprint start ...");
    let start = tokio::time::Instant::now();
    // Create a queue with concurrency of 2
    let queue: Queue<i32, Error, ()> = Queue::new(2);

    // Define some async tasks
    let task1 = || async {
        println!("task1 start ...");
        sleep(tokio::time::Duration::from_millis(250)).await;
        println!("task1 done!");
        Ok(1)
    };
    let task2 = || async {
        println!("task2 start ...");
        sleep(tokio::time::Duration::from_millis(50)).await;
        println!("task2 done!");
        Ok(2)
    };
    let task3 = || async {
        println!("task3 start ...");
        sleep(tokio::time::Duration::from_millis(50)).await;
        println!("task3 done!");
        Ok(3)
    };

    // Push tasks to the queue
    queue.push(&"task1".to_string(), task1).await.unwrap();
    queue.push(&"task2".to_string(), task2).await.unwrap();
    queue.push(&"task3".to_string(), task3).await.unwrap();

    // Signal that all tasks have been pushed
    queue.set_push_done().await;

    // Wait for all tasks to complete and get results
    let results = queue.wait_for_tasks_done().await;

    let end = tokio::time::Instant::now();
    println!("sprint done in {:?}", end - start);
    println!("results: {:?}", results);
}
```

It will print:

```
sprint start ...
task1 start ...
task2 start ...
task2 done!
task3 start ...
task3 done!
task1 done!
sprint done in 251.949651ms
results: {"task1": Ok(Ok(1)), "task2": Ok(Ok(2)), "task3": Ok(Ok(3))}
```

This will execute tasks in parallel with a maximum concurrency of 2, meaning two tasks can run simultaneously. The output will show tasks running and completing based on their duration and the concurrency limit.

### Deduping Example

[Here's an example](example/deduping.rs) of using Sprinter to run parallel tasks with deduplication:

```rust
...

for _ in 0..3 {
    // Push tasks to the queue
    queue
        .push_deduping(&"task1".to_string(), task1, || async {
            println!("task1 deduped!");
            ()
        })
        .await
        .unwrap();
    queue
        .push_deduping(&"task2".to_string(), task2, || async {
            println!("task2 deduped!");
            ()
        })
        .await
        .unwrap();
    queue
        .push_deduping(&"task3".to_string(), task3, || async {
            println!("task3 deduped!");
            ()
        })
        .await
        .unwrap();
}

```

It will print:

```
sprint start ...
task2 start ...
task1 start ...
task2 done!
task2 deduped!
task2 deduped!
task3 start ...
task3 done!
task3 deduped!
task3 deduped!
task1 done!
task1 deduped!
task1 deduped!
sprint done in 251.677382ms
```

This code executes the tasks in parallel; `task1` will be executed only once, while the `on_deduped` function will be called after its resolution.

## API

### Creating a Queue

```rust
let queue: Queue<T, E, G> = Queue::new(concurrency: usize);
```

The Queue type takes three type parameters:
- `T`: The type of successful task result
- `E`: The type of error that tasks may return
- `G`: The type of deduplication callback result

Parameters:
- `concurrency`: Maximum number of tasks that can run in parallel

### Methods

### `push`

```rust
async fn push<F, R>(&self, task_id: &String, task: F) -> Result<(), QueueError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Future<Output = Result<T, E>> + Send + 'static
```
Pushes a new task to the queue. The task will be executed as soon as it is pushed or when the queue has available capacity.

Parameters:
- `task_id`: Unique identifier for the task. Must not be empty.
- `task`: The task to execute. Must be a future that returns `Result<T, E>`.

Returns:
- `Ok(())` if the task was successfully pushed
- `Err(QueueError)` if the task_id is empty or already exists

### `push_deduping`

```rust
async fn push_deduping<F, R, D, DR>(&self, task_id: &String, task: F, on_deduped: D) -> Result<(), QueueError>
where
    F: FnOnce() -> R + Send + 'static,
    D: FnOnce() -> DR + Send + 'static,
    R: Future<Output = Result<T, E>> + Send + 'static,
    DR: Future<Output = G> + Send + 'static
```
Pushes a task with deduplication support. If a task with the same ID exists, the `on_deduped` callback will be executed after the original task completes.

Parameters:
- `task_id`: Unique identifier for the task. Must not be empty.
- `task`: The task to execute. Must be a future that returns `Result<T, E>`.
- `on_deduped`: Callback function to execute if the task is deduplicated.

### `set_push_done`
```rust
async fn set_push_done(&self)
```
Signals that all tasks have been pushed to the queue. This must be called even if all tasks have completed, as the queue will not finish processing until this is called.

### `wait_for_task_done`
```rust
async fn wait_for_task_done(&self, task_id: &String) -> Result<Result<T, E>, QueueError>
```
Waits for a specific task to complete and returns its result.

Parameters:
- `task_id`: The unique identifier of the task to wait for

Returns:
- `Ok(Ok(T))` if the task completed successfully
- `Ok(Err(E))` if the task failed
- `Err(QueueError::TaskNotFound)` if the task doesn't exist

### `wait_for_tasks_done`
```rust
async fn wait_for_tasks_done(&self) -> Result<(), QueueError>
```
Waits for all tasks to complete. This is the most efficient way to wait for completion when you don't need the results.

Note: `set_push_done()` must be called before this method.

### `wait_for_results`
```rust
async fn wait_for_results(&self) -> HashMap<String, Result<Result<T, E>, QueueError>>
```
Waits for all tasks to complete and returns a map of task IDs to their results.

Note: `set_push_done()` must be called before this method.

Returns:
- A HashMap where:
  - Keys are task IDs
  - Values are the results of each task, wrapped in Result types for both task and queue errors

### `state`
```rust
async fn state(&self) -> QueueState
```
Returns the current state of the queue. Possible states are:
- `QueueState::Idle`: Queue is not processing tasks
- `QueueState::Running`: Queue is actively processing tasks
- `QueueState::Done`: All tasks have completed

### `reset`
```rust
async fn reset(&self)
```
Resets the queue to its initial state

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
