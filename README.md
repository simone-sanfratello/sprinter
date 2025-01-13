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
sprinter = "0.2.1"
```

## Usage

### Basic Example

Here's a basic example of using Sprinter to run parallel tasks:

```rust
use sprinter::Queue;
use std::fmt::Error;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("sprint start ...");
    let start = tokio::time::Instant::now();
    // Create a queue with concurrency of 2
    let queue: Queue<i32, Error> = Queue::new(2);

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
    queue.push(&"task1".to_string(), task1, None).await.unwrap();
    queue.push(&"task2".to_string(), task2, None).await.unwrap();
    queue.push(&"task3".to_string(), task3, None).await.unwrap();

    // Signal that all tasks have been pushed
    queue.set_push_done().await;

    // Wait for all tasks to complete and get results
    let results = queue.wait_for_results().await;

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

Here's an example of using Sprinter to run parallel tasks with deduplication:

```rust
TODO
```

It will print:

```
TODO
```

This code executes the tasks in parallel; `task1` will be executed only once, while the `on_deduped` function will be called after its resolution.

## API

### Creating a Queue

```rust
let queue: Queue<T, E> = Queue::new(concurrency: usize);
```

The Queue type takes two type parameters:
- `T`: The type of successful task result
- `E`: The type of error that tasks may return

Parameters:
- `concurrency`: Maximum number of tasks that can run in parallel

### Methods

### `push`
```rust
async fn push<F, R>(&self, task_id: &String, task: F, task_options: Option<TaskOptions>) -> Result<bool, QueueError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Future<Output = Result<T, E>> + Send + 'static
```
Pushes a new task to the queue. The task will be executed as soon as it is pushed or when the queue has available capacity.

Parameters:
- `task_id`: Unique identifier for the task. Must not be empty.
- `task`: The task to execute. Must be a future that returns `Result<T, E>`.
- `task_options`: Optional task options
    - `on_deduped`: Optional function to be called when the task is deduped

Returns:
- `Ok(true)` if the task was successfully pushed
- `Ok(false)` if a task with the same ID already exists (deduplication)
- `Err(QueueError)` if the task_id is empty or other errors occur

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
