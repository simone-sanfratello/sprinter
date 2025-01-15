use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
};
use tokio::sync::{watch, Mutex, RwLock, Semaphore};

// TODO generic for TaskId type
pub type TaskId = String;
pub const MARKER_TASK_ID_PUSH_DONE: &str = "#";

#[derive(Clone, Debug, PartialEq)]
pub enum TaskState {
    Add,
    Pending,
    Running,
    Succeed,
    Failed,
    AllPushed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueueState {
    Running,
    Done,
    Idle,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueueError {
    TaskNotFound(TaskId),
    Other(String),
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct CompletionLog {
    task_id: TaskId,
}

pub struct Task<GenericTaskResult, GenericTaskResultError> {
    pub id: TaskId,
    pub task: Pin<Box<dyn Future<Output = Result<GenericTaskResult, GenericTaskResultError>> + Send + Sync>>,
}

pub struct TaskInfo<GenericTaskResult, GenericTaskResultError, GenericDedupedResult> {
    pub status: TaskState,
    pub result: Option<Result<GenericTaskResult, GenericTaskResultError>>,
    pub on_deduped: Option<Vec<Pin<Box<dyn Future<Output = GenericDedupedResult> + Send + Sync>>>>,
}

pub struct Queue<GenericTaskResult, GenericTaskResultError, GenericDedupedResult> {
    semaphore: Arc<Semaphore>,
    queue: Arc<Mutex<VecDeque<Task<GenericTaskResult, GenericTaskResultError>>>>,
    index: Arc<
        RwLock<
            HashMap<
                TaskId,
                TaskInfo<GenericTaskResult, GenericTaskResultError, GenericDedupedResult>,
            >,
        >,
    >,
    queue_state_tx: watch::Sender<QueueState>,
    queue_state_rx: watch::Receiver<QueueState>,
    tasks_state_tx: watch::Sender<(TaskId, TaskState)>,
    tasks_state_rx: watch::Receiver<(TaskId, TaskState)>,
    push_done: Arc<RwLock<bool>>,
    #[cfg(test)]
    completion_log: Arc<RwLock<Vec<CompletionLog>>>,
}

impl<GenericTaskResult, GenericTaskResultError, GenericDedupedResult>
    Queue<GenericTaskResult, GenericTaskResultError, GenericDedupedResult>
where
    GenericTaskResult: Send + Sync + 'static,
    GenericTaskResultError: Send + Sync + 'static,
    GenericDedupedResult: Send + Sync + 'static,
{
    /// Creates a new queue with the given concurrency.
    ///
    /// # Arguments
    ///
    /// * `concurrency`: Maximum number of tasks that can run in parallel
    ///
    /// # Returns
    ///
    /// A new queue instance
    ///
    /// # Examples
    ///
    /// ```rust
    /// let queue: sprinter::Queue<i32, std::fmt::Error, ()> = sprinter::Queue::new(2);
    /// ```
    pub fn new(concurrency: usize) -> Self {
        let (queue_state_tx, queue_state_rx) = watch::channel(QueueState::Idle);
        let (tasks_state_tx, tasks_state_rx) = watch::channel((String::new(), TaskState::Pending));

        Self {
            semaphore: Arc::new(Semaphore::const_new(concurrency)),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            index: Arc::new(RwLock::new(HashMap::new())),
            queue_state_tx,
            queue_state_rx,
            tasks_state_tx,
            tasks_state_rx,
            push_done: Arc::new(RwLock::new(false)),
            #[cfg(test)]
            completion_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Pushes a new task to the queue, it will be executed as soon as it is pushed or when the queue is ready.
    /// Each task must have a unique id, and execution is deduped by `task_id`.
    /// Note that the queue will not complete until `set_push_done` is called, even if all tasks have been completed.
    ///
    /// # Arguments
    ///
    /// * `task_id`: Unique identifier for the task
    /// * `task`: Task to be executed
    ///
    /// # Returns
    ///
    /// `true` if the task was pushed, `false` if the task is deduped
    ///
    /// # Examples
    ///
    /// ```rust
    /// # tokio_test::block_on(async {
    /// let queue: sprinter::Queue<i32, std::fmt::Error, ()> = sprinter::Queue::new(1);
    /// queue.push(&"task1".to_string(), || async { Ok(1) }).await.unwrap();
    /// # })
    /// ```
    pub async fn push<GenericTaskClosure, GenericFutureTaskResult>(
        &self,
        task_id: &String,
        task: GenericTaskClosure,
    ) -> Result<bool, QueueError>
    where
        GenericTaskClosure: FnOnce() -> GenericFutureTaskResult + Send + Sync + 'static,
        GenericFutureTaskResult:
            Future<Output = Result<GenericTaskResult, GenericTaskResultError>> + Send + Sync + 'static,
    {
        if task_id.is_empty() {
            return Err(QueueError::Other("task_id cannot be empty".to_string()));
        }

        // First check with read lock
        if self.index.read().await.contains_key(task_id) {
            return Ok(false);
        }

        // Then acquire write lock for insertion
        let mut index = self.index.write().await;
        // Double check in case of race condition
        if index.contains_key(task_id) {
            return Ok(false);
        }

        index.insert(
            task_id.clone(),
            TaskInfo {
                status: TaskState::Pending,
                result: None,
                on_deduped: None,
            },
        );
        drop(index);

        self._push(task_id, task).await;

        Ok(true)
    }

    // TODO doc
    pub async fn push_deduping<
        GenericTaskClosure,
        GenericFutureTaskResult,
        GenericDedupedClosure,
        GenericFutureDedupedResult,
    >(
        &self,
        task_id: &String,
        task: GenericTaskClosure,
        on_deduped: GenericDedupedClosure,
    ) -> Result<bool, QueueError>
    where
        GenericTaskClosure: FnOnce() -> GenericFutureTaskResult + Send + Sync + 'static,
        GenericDedupedClosure: FnOnce() -> GenericFutureDedupedResult + Send + Sync + 'static,
        GenericFutureTaskResult:
            Future<Output = Result<GenericTaskResult, GenericTaskResultError>> + Send + Sync + 'static,
        GenericFutureDedupedResult: Future<Output = GenericDedupedResult> + Send + Sync + 'static,
    {
        if task_id.is_empty() {
            return Err(QueueError::Other("task_id cannot be empty".to_string()));
        }

        let mut index = self.index.write().await;
        let task_info = index.get_mut(task_id);
        if let Some(t) = task_info {
            if t.status == TaskState::Succeed || t.status == TaskState::Failed {
                let on_deduped = Box::pin(on_deduped());
                on_deduped.await;
            } else {
                let on_deduped = Box::pin(on_deduped());
                // deduped is always Some here
                t.on_deduped.as_mut().unwrap().push(on_deduped);
            }
            drop(index);
            return Ok(false);
        }

        index.insert(
            task_id.clone(),
            TaskInfo {
                status: TaskState::Pending,
                result: None,
                on_deduped: Some(Vec::new()),
            },
        );
        drop(index);

        self._push(task_id, task).await;

        Ok(true)
    }

    async fn _push<GenericTaskClosure, GenericFutureTaskResult>(
        &self,
        task_id: &String,
        task: GenericTaskClosure,
    ) where
        GenericTaskClosure: FnOnce() -> GenericFutureTaskResult + Send + Sync + 'static,
        GenericFutureTaskResult:
            Future<Output = Result<GenericTaskResult, GenericTaskResultError>> + Send + Sync + 'static,
    {
        // wrap the task for future execution
        let boxed_future = Box::pin(task());

        // add task to queue
        let mut queue = self.queue.lock().await;
        queue.push_back(Task {
            id: task_id.clone(),
            task: boxed_future,
        });
        drop(queue);

        self.tasks_state_tx
            .send((task_id.clone(), TaskState::Add))
            .unwrap();

        // ping the ticker to start processing
        self.tick().await;
    }

    /// Starts processing tasks in the queue.
    async fn tick(&self) {
        // if already running, do nothing
        if *self.queue_state_rx.borrow() == QueueState::Running {
            return;
        }

        // start processing
        self.queue_state_tx.send(QueueState::Running).unwrap();

        let queue = self.queue.clone();
        let index = self.index.clone();
        let push_done = self.push_done.clone();
        let semaphore = self.semaphore.clone();
        let queue_state_tx = self.queue_state_tx.clone();
        let tasks_state_tx = self.tasks_state_tx.clone();
        let mut tasks_state_rx = self.tasks_state_rx.clone();

        #[cfg(test)]
        let completion_log = self.completion_log.clone();

        tokio::spawn(async move {
            let mut running_tasks = Vec::new();

            loop {
                // TODO debug feature flag
                // {
                //     // Print currently running tasks
                //     let index_lock = index.lock().await;
                //     let running_tasks: Vec<_> = index_lock
                //         .iter()
                //         .filter(|(_, info)| info.status == TaskState::Running)
                //         .map(|(id, _)| id)
                //         .collect();
                //     if !running_tasks.is_empty() {
                //         println!("Currently running tasks: {:?}", running_tasks);
                //     }
                //     drop(index_lock);
                // }

                // clean up completed tasks
                running_tasks.retain(|handle: &tokio::task::JoinHandle<_>| !handle.is_finished());

                // check if there are any new tasks to process
                let mut queue_lock = queue.lock().await;
                if let Some(task) = queue_lock.pop_front() {
                    drop(queue_lock);

                    // acquire semaphore permit before spawning the task
                    let permit = semaphore.clone().acquire_owned().await.unwrap();

                    let task_id = task.id.clone();
                    let index = index.clone();
                    let tasks_state_tx = tasks_state_tx.clone();

                    #[cfg(test)]
                    let completion_log = completion_log.clone();

                    let handle = tokio::spawn(async move {
                        // the semaphore permit will be dropped when this task completes
                        let _permit = permit;

                        {
                            let mut index = index.write().await;
                            if let Some(task_info) = index.get_mut(&task_id) {
                                task_info.status = TaskState::Running;
                                tasks_state_tx
                                    .send((task_id.clone(), TaskState::Running))
                                    .unwrap();
                            }
                        }

                        // execute the task
                        let result = task.task.await;

                        {
                            let mut index = index.write().await;
                            if let Some(task_info) = index.get_mut(&task_id) {
                                task_info.status = if result.is_ok() {
                                    // println!(" >>> Task {} is Succeed", task_id);
                                    tasks_state_tx
                                        .send((task_id.clone(), TaskState::Succeed))
                                        .unwrap();
                                    TaskState::Succeed
                                } else {
                                    // println!(" >>> Task {} is Failed", task_id);
                                    tasks_state_tx
                                        .send((task_id.clone(), TaskState::Failed))
                                        .unwrap();
                                    TaskState::Failed
                                };
                                task_info.result = Some(result);

                                // run deduped callbacks
                                if let Some(on_deduped) = task_info.on_deduped.as_mut() {
                                    for on_deduped in on_deduped {
                                        on_deduped.await;
                                    }
                                }
                            }
                        }

                        #[cfg(test)]
                        completion_log.write().await.push(CompletionLog {
                            task_id: task_id.clone(),
                        });
                    });

                    running_tasks.push(handle);
                } else {
                    drop(queue_lock);
                }

                // check if all tasks are done - use read locks for better concurrency
                let queue_empty = queue.lock().await.is_empty();
                let tasks_all_pushed = *push_done.read().await;
                if queue_empty && running_tasks.is_empty() && tasks_all_pushed {
                    queue_state_tx.send(QueueState::Done).unwrap();
                    break;
                }

                // wait for a task update
                // - when running tasks count is less than concurrency, for task to be added
                // - when running tasks count is equal to concurrency, for task to be done
                tasks_state_rx.changed().await.unwrap();
            }
        });
    }

    /// Signals that all tasks have been pushed to the queue.
    /// This is needed since the queue starts processing as soon as the first task is pushed.
    /// Note the queue will not complete until `set_push_done` is called, even if all tasks have been completed.
    pub async fn set_push_done(&self) {
        let mut push_done = self.push_done.write().await;
        *push_done = true;
        drop(push_done); // Release write lock before sending message
        
        // Send message after releasing lock to reduce contention
        if let Err(_) = self.tasks_state_tx.send((
            MARKER_TASK_ID_PUSH_DONE.to_string(),
            TaskState::AllPushed,
        )) {
            // Channel closed - queue likely shutting down
            return;
        }
    }

    /// Waits for a task to complete and returns the result.
    ///
    /// # Arguments
    ///
    /// * `task_id`: Unique identifier for the task
    ///
    /// # Returns
    ///
    /// The result of the task
    pub async fn wait_for_task_done(
        &self,
        task_id: &String,
    ) -> Result<Result<GenericTaskResult, GenericTaskResultError>, QueueError>
    where
        GenericTaskResult: Clone,
        GenericTaskResultError: Clone,
    {
        // First try with read lock
        {
            let index = self.index.read().await;
            if let Some(info) = index.get(task_id) {
                if info.status == TaskState::Succeed || info.status == TaskState::Failed {
                    return Ok(info.result.clone().unwrap());
                }
            } else {
                return Err(QueueError::TaskNotFound(task_id.clone()));
            }
        }

        // If task is not complete, wait for completion
        let mut rx = self.tasks_state_rx.clone();
        rx.wait_for(|(id, state)| {
            id == task_id && (*state == TaskState::Succeed || *state == TaskState::Failed)
        })
        .await
        .unwrap();

        // Get final result
        let index = self.index.read().await;
        match index.get(task_id) {
            Some(info) => Ok(info.result.clone().unwrap()),
            None => Err(QueueError::TaskNotFound(task_id.clone())),
        }
    }

    /// Waits for all tasks to complete; `set_push_done` must be called before this method to complete.
    /// This is the leanest way to wait for all tasks to complete, without interest in the results.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # tokio_test::block_on(async {
    /// let queue: sprinter::Queue<i32, std::fmt::Error, ()> = sprinter::Queue::new(2);
    /// queue.push(&"task1".to_string(), || async { Ok(1) }).await.unwrap();
    /// queue.push(&"task2".to_string(), || async { Ok(2) }).await.unwrap();
    /// queue.set_push_done().await;
    /// queue.wait_for_tasks_done().await.unwrap();
    /// # })
    /// ```

    pub async fn wait_for_tasks_done(&self) -> Result<(), QueueError> {
        let mut rx = self.queue_state_rx.clone();
        let result = rx.wait_for(|state| *state == QueueState::Done).await;
        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(QueueError::Other(err.to_string())),
        }
    }

    /// Waits for all tasks to complete; `set_push_done` must be called before this method to complete.
    /// Use `wait_for_tasks_done` if you are not interested in the results.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # tokio_test::block_on(async {
    /// let queue: sprinter::Queue<i32, std::fmt::Error, ()> = sprinter::Queue::new(2);
    /// queue.push(&"task1".to_string(), || async { Ok(1) }).await.unwrap();
    /// queue.push(&"task2".to_string(), || async { Ok(2) }).await.unwrap();
    /// queue.set_push_done().await;
    /// queue.wait_for_tasks_done().await.unwrap();
    /// # })
    /// ```
    pub async fn wait_for_results(
        &self,
    ) -> HashMap<String, Result<Result<GenericTaskResult, GenericTaskResultError>, QueueError>>
    where
        GenericTaskResult: Clone,
        GenericTaskResultError: Clone,
    {
        let mut rx = self.queue_state_rx.clone();
        rx.wait_for(|state| *state == QueueState::Done)
            .await
            .unwrap();

        let index = self.index.read().await;
        let results = index
            .iter()
            .map(|(name, info)| (name.clone(), Ok(info.result.clone().unwrap())))
            .collect();

        results
    }

    /// Returns the current state of the queue.
    pub async fn state(&self) -> QueueState {
        self.queue_state_rx.borrow().clone()
    }

    #[cfg(test)]
    pub async fn _test_get_completion_order(&self) -> Vec<String> {
        self.completion_log
            .read()
            .await
            .iter()
            .map(|log| log.task_id.clone())
            .collect()
    }

    pub async fn reset(&self) {
        // Acquire all locks in a consistent order to prevent deadlocks
        let mut index = self.index.write().await;
        let mut queue = self.queue.lock().await;
        let mut all_tasks_pushed = self.push_done.write().await;
        
        // Clear all data structures
        index.clear();
        queue.clear();
        *all_tasks_pushed = false;

        #[cfg(test)]
        {
            let mut completion_log = self.completion_log.write().await;
            completion_log.clear();
        }

        // Release locks before sending message
        drop(index);
        drop(queue);
        drop(all_tasks_pushed);

        self.queue_state_tx.send(QueueState::Idle).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fmt::Error, sync::atomic::AtomicBool};
    use tokio::time::sleep;

    #[tokio::test]
    async fn queue_should_run_tasks_in_parallel() {
        let queue: Queue<i32, Error, ()> = Queue::new(3);

        // Create 6 tasks with different durations
        let task1 = || async {
            sleep(tokio::time::Duration::from_millis(1500)).await;
            Ok(1)
        };
        let task2 = || async {
            sleep(tokio::time::Duration::from_millis(500)).await;
            Ok(2)
        };
        let task3 = || async {
            sleep(tokio::time::Duration::from_millis(150)).await;
            Ok(3)
        };
        let task4 = || async {
            sleep(tokio::time::Duration::from_millis(1000)).await;
            Ok(4)
        };
        let task5 = || async {
            sleep(tokio::time::Duration::from_millis(500)).await;
            Ok(5)
        };
        let task6 = || async {
            sleep(tokio::time::Duration::from_millis(100)).await;
            Ok(6)
        };

        let start = tokio::time::Instant::now();
        let _ = queue.push(&"t1".to_string(), task1).await.unwrap();
        let _ = queue.push(&"t2".to_string(), task2).await.unwrap();
        let _ = queue.push(&"t3".to_string(), task3).await.unwrap();
        let _ = queue.push(&"t4".to_string(), task4).await.unwrap();
        let _ = queue.push(&"t5".to_string(), task5).await.unwrap();
        let _ = queue.push(&"t6".to_string(), task6).await.unwrap();
        queue.set_push_done().await;

        let results = queue.wait_for_results().await;

        let expected_results = HashMap::from([
            ("t1".to_string(), Ok(Ok(1))),
            ("t2".to_string(), Ok(Ok(2))),
            ("t3".to_string(), Ok(Ok(3))),
            ("t4".to_string(), Ok(Ok(4))),
            ("t5".to_string(), Ok(Ok(5))),
            ("t6".to_string(), Ok(Ok(6))),
        ]);
        assert_eq!(results, expected_results);

        // Expected completion order based on duration:
        // First wave (t1, t2, t3 start immediately due to concurrency 3):
        // - t3 finishes first (250ms)
        // - t2 finishes second (500ms)
        // Second wave (t4, t5, t6 start as permits become available):
        // - t5 finishes third (750ms)
        // - t4 finishes fourth (1000ms)
        // - t6 finishes fifth (100ms after starting) because of concurrency 3
        // - t1 finishes last (1500ms)

        let completion_order = queue._test_get_completion_order().await;
        assert_eq!(completion_order, vec!["t3", "t2", "t5", "t6", "t4", "t1"]);

        let end = tokio::time::Instant::now();
        // TODO exclude from coverage
        assert!(
            end - start < tokio::time::Duration::from_millis(1503),
            "sprint exceeded 1503ms (1500ms longest task + 3ms overhead) - {:?}",
            end - start
        );
    }

    #[tokio::test]
    async fn queue_should_run_tasks_with_delay_between_pushing_tasks() {
        let queue: Queue<i32, Error, ()> = Queue::new(2);

        // Create 6 tasks with different durations
        let task1 = || async {
            sleep(tokio::time::Duration::from_millis(100)).await;
            Ok(1)
        };
        let task2 = || async {
            sleep(tokio::time::Duration::from_millis(1500)).await;
            Ok(2)
        };
        let task3 = || async {
            sleep(tokio::time::Duration::from_millis(750)).await;
            Ok(3)
        };
        let task4 = || async {
            sleep(tokio::time::Duration::from_millis(500)).await;
            Ok(4)
        };
        let task5 = || async {
            sleep(tokio::time::Duration::from_millis(100)).await;
            Ok(5)
        };
        let task6 = || async {
            sleep(tokio::time::Duration::from_millis(50)).await;
            Ok(6)
        };

        let start = tokio::time::Instant::now();
        let _ = queue.push(&"t1".to_string(), task1).await.unwrap();
        sleep(tokio::time::Duration::from_millis(100)).await;
        let _ = queue.push(&"t2".to_string(), task2).await.unwrap();
        sleep(tokio::time::Duration::from_millis(100)).await;
        let _ = queue.push(&"t3".to_string(), task3).await.unwrap();
        sleep(tokio::time::Duration::from_millis(100)).await;
        let _ = queue.push(&"t4".to_string(), task4).await.unwrap();
        sleep(tokio::time::Duration::from_millis(100)).await;
        let _ = queue.push(&"t5".to_string(), task5).await.unwrap();
        sleep(tokio::time::Duration::from_millis(100)).await;
        let _ = queue.push(&"t6".to_string(), task6).await.unwrap();
        queue.set_push_done().await;

        let results = queue.wait_for_results().await;

        let expected_results = HashMap::from([
            ("t1".to_string(), Ok(Ok(1))),
            ("t2".to_string(), Ok(Ok(2))),
            ("t3".to_string(), Ok(Ok(3))),
            ("t4".to_string(), Ok(Ok(4))),
            ("t5".to_string(), Ok(Ok(5))),
            ("t6".to_string(), Ok(Ok(6))),
        ]);
        assert_eq!(results, expected_results);

        let completion_order = queue._test_get_completion_order().await;
        assert_eq!(completion_order, vec!["t1", "t3", "t4", "t5", "t2", "t6"]);

        let end = tokio::time::Instant::now();
        // TODO exclude from coverage
        assert!(
            end - start < tokio::time::Duration::from_millis(1610),
            "sprint exceeded 1610ms (1500ms longest task + pauses between pushes + 10ms overhead) - {:?}",
            end - start
        );
    }

    #[tokio::test]
    async fn task_done_should_return_result_when_ready() {
        let queue: Queue<i32, Error, ()> = Queue::new(2);

        let task = || async {
            sleep(tokio::time::Duration::from_millis(100)).await;
            Ok(42)
        };

        let task_id = "test_task".to_string();
        queue.push(&task_id, task).await.unwrap();
        queue.set_push_done().await;

        let result = queue.wait_for_task_done(&task_id).await.unwrap();
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn task_done_should_return_error_for_nonexistent_task() {
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        let result = queue.wait_for_task_done(&"nonexistent".to_string()).await;
        assert!(matches!(result, Err(QueueError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn task_push_should_fail_for_empty_task_id() {
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        let result = queue.push(&"".to_string(), || async { Ok(1) }).await;
        assert_eq!(
            result,
            Err(QueueError::Other("task_id cannot be empty".to_string()))
        );
    }

    #[tokio::test]
    async fn queue_should_run_tasks_deduping() {
        let queue: Queue<&str, Error, ()> = Queue::new(3);

        let task1 = || async {
            sleep(tokio::time::Duration::from_millis(50)).await;
            Ok::<&str, Error>("result1")
        };
        let task2 = || async {
            sleep(tokio::time::Duration::from_millis(50)).await;
            Ok::<&str, Error>("result2")
        };
        let task3 = || async {
            sleep(tokio::time::Duration::from_millis(50)).await;
            Ok::<&str, Error>("result3")
        };

        assert_eq!(queue.push(&"t1".to_string(), task1).await, Ok(true));
        assert_eq!(queue.push(&"t1".to_string(), task1).await, Ok(false));
        assert_eq!(queue.push(&"t2".to_string(), task2).await, Ok(true));
        assert_eq!(queue.push(&"t1".to_string(), task1).await, Ok(false));
        assert_eq!(queue.push(&"t3".to_string(), task3).await, Ok(true));
        assert_eq!(queue.push(&"t1".to_string(), task1).await, Ok(false));
        queue.set_push_done().await;

        let results = queue.wait_for_results().await;

        let expected_results = HashMap::from([
            ("t1".to_string(), Ok(Ok("result1"))),
            ("t2".to_string(), Ok(Ok("result2"))),
            ("t3".to_string(), Ok(Ok("result3"))),
        ]);
        assert_eq!(results, expected_results);

        let completion_order = queue._test_get_completion_order().await;
        assert_eq!(completion_order, vec!["t1", "t2", "t3"]);
    }

    #[tokio::test]
    async fn readme_example() {
        println!("sprint start ...");
        let start = tokio::time::Instant::now();
        let queue: Queue<i32, Error, ()> = Queue::new(2);

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

        queue.push(&"task1".to_string(), task1).await.unwrap();
        queue.push(&"task1".to_string(), task1).await.unwrap();
        queue.push(&"task2".to_string(), task2).await.unwrap();
        queue.push(&"task3".to_string(), task3).await.unwrap();
        queue.set_push_done().await;

        let results = queue.wait_for_results().await;

        let end = tokio::time::Instant::now();
        println!("sprint done in {:?}", end - start);

        assert_eq!(
            results,
            HashMap::from([
                ("task1".to_string(), Ok(Ok(1))),
                ("task2".to_string(), Ok(Ok(2))),
                ("task3".to_string(), Ok(Ok(3))),
            ])
        );
        let completion_order = queue._test_get_completion_order().await;
        assert_eq!(completion_order, vec!["task2", "task3", "task1"]);
    }

    #[tokio::test]
    async fn on_deduped_should_be_called() {
        let queue: Queue<i32, Error, ()> = Queue::new(2);
        let task = || async { Ok(1) };
        let deduped = Arc::new(AtomicBool::new(false));

        let _ = queue
            .push_deduping(&"task1".to_string(), task, || async {
                assert!(false, "on_deduped on first task should not be called");
            })
            .await
            .unwrap();
        let d = Arc::clone(&deduped);
        let _ = queue
            .push_deduping(&"task1".to_string(), task, move || async move {
                // println!("on_deduped on second task");
                d.store(true, std::sync::atomic::Ordering::Release);
            })
            .await
            .unwrap();

        queue.set_push_done().await;
        queue.wait_for_tasks_done().await.unwrap();

        let d = Arc::try_unwrap(deduped).unwrap();
        assert!(d.into_inner(), "on_deduped on second task should be called");
    }

    // TODO mix push and push_deduping

    // TODO test with hundreds of tasks
}
