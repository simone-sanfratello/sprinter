use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{watch, Mutex, Notify, RwLock, Semaphore};

/// Injected failures for tests (`HookTestSession`) and OR-combined with real `watch` errors so coverage runs hit the same branches as production.
mod failure_injection {
    use std::sync::atomic::{AtomicBool, Ordering};
    #[cfg(test)]
    use std::sync::{Mutex, MutexGuard};

    use tokio::sync::watch;

    #[cfg(test)]
    static TEST_SERIAL: Mutex<()> = Mutex::new(());

    /// Serializes unit tests and clears failure-injection flags (parallel `cargo test` would otherwise leak `FAIL_*` across tests).
    #[cfg(test)]
    pub(crate) fn test_serial() -> MutexGuard<'static, ()> {
        let g = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        reset_all();
        g
    }

    #[cfg(test)]
    pub(crate) fn reset_all() {
        FAIL_TASKS_SEND_ADD.store(false, Ordering::SeqCst);
        FAIL_QUEUE_STATE_ON_TICK.store(false, Ordering::SeqCst);
        FAIL_WORKER_RUNNING_SEND.store(false, Ordering::SeqCst);
        FAIL_WORKER_FINAL_SEND.store(false, Ordering::SeqCst);
        FAIL_COORD_TASKS_CHANGED.store(false, Ordering::SeqCst);
        FAIL_WAIT_TASK_CHANGED.store(false, Ordering::SeqCst);
        FAIL_QUEUE_STATE_WAIT.store(false, Ordering::SeqCst);
        FAIL_RESET_QUEUE_STATE.store(false, Ordering::SeqCst);
        FAIL_SET_PUSH_DONE_SEND.store(false, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub struct HookTestSession {
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    #[cfg(test)]
    impl HookTestSession {
        pub fn new() -> Self {
            let _lock = test_serial();
            Self { _lock }
        }
    }

    #[cfg(test)]
    impl Drop for HookTestSession {
        fn drop(&mut self) {
            reset_all();
        }
    }

    pub static FAIL_TASKS_SEND_ADD: AtomicBool = AtomicBool::new(false);
    pub static FAIL_QUEUE_STATE_ON_TICK: AtomicBool = AtomicBool::new(false);
    pub static FAIL_WORKER_RUNNING_SEND: AtomicBool = AtomicBool::new(false);
    pub static FAIL_WORKER_FINAL_SEND: AtomicBool = AtomicBool::new(false);
    pub static FAIL_COORD_TASKS_CHANGED: AtomicBool = AtomicBool::new(false);
    pub static FAIL_WAIT_TASK_CHANGED: AtomicBool = AtomicBool::new(false);
    pub static FAIL_QUEUE_STATE_WAIT: AtomicBool = AtomicBool::new(false);
    pub static FAIL_RESET_QUEUE_STATE: AtomicBool = AtomicBool::new(false);
    pub static FAIL_SET_PUSH_DONE_SEND: AtomicBool = AtomicBool::new(false);

    #[inline]
    pub fn tasks_add_send_failed(res: Result<(), watch::error::SendError<(super::TaskId, super::TaskState)>>) -> bool {
        FAIL_TASKS_SEND_ADD.swap(false, Ordering::SeqCst) | res.is_err()
    }

    #[inline]
    pub fn queue_state_running_send_failed(res: Result<(), watch::error::SendError<super::QueueState>>) -> bool {
        FAIL_QUEUE_STATE_ON_TICK.swap(false, Ordering::SeqCst) | res.is_err()
    }

    #[inline]
    pub fn worker_running_send_failed(res: Result<(), watch::error::SendError<(super::TaskId, super::TaskState)>>) -> bool {
        FAIL_WORKER_RUNNING_SEND.swap(false, Ordering::SeqCst) | res.is_err()
    }

    #[inline]
    pub fn worker_final_send_failed(res: Result<(), watch::error::SendError<(super::TaskId, super::TaskState)>>) -> bool {
        FAIL_WORKER_FINAL_SEND.swap(false, Ordering::SeqCst) | res.is_err()
    }

    #[inline]
    pub fn tasks_changed_failed(res: Result<(), watch::error::RecvError>) -> bool {
        FAIL_COORD_TASKS_CHANGED.swap(false, Ordering::SeqCst) | res.is_err()
    }

    #[inline]
    pub fn wait_task_changed_failed(res: Result<(), watch::error::RecvError>) -> bool {
        FAIL_WAIT_TASK_CHANGED.swap(false, Ordering::SeqCst) | res.is_err()
    }

    #[inline]
    pub fn queue_wait_failed<T>(
        res: Result<watch::Ref<'_, T>, watch::error::RecvError>,
    ) -> bool {
        FAIL_QUEUE_STATE_WAIT.swap(false, Ordering::SeqCst) | res.is_err()
    }

    #[inline]
    pub fn reset_queue_state_send_failed(res: Result<(), watch::error::SendError<super::QueueState>>) -> bool {
        FAIL_RESET_QUEUE_STATE.swap(false, Ordering::SeqCst) | res.is_err()
    }

    #[inline]
    pub fn set_push_done_send_failed(res: Result<(), watch::error::SendError<(super::TaskId, super::TaskState)>>) -> bool {
        FAIL_SET_PUSH_DONE_SEND.swap(false, Ordering::SeqCst) | res.is_err()
    }
}

#[cfg(test)]
pub(crate) use failure_injection::test_serial;
#[cfg(test)]
pub use failure_injection::HookTestSession;

// TODO generic for TaskId type
pub type TaskId = String;

type BoxedTaskFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;
type DedupeFuture<D> = Pin<Box<dyn Future<Output = D> + Send>>;
type TaskIndex<R, E, D> = HashMap<TaskId, TaskInfo<R, E, D>>;
pub const MARKER_TASK_ID_PUSH_DONE: &str = "#";

#[derive(Clone, Copy, Debug, PartialEq)]
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
    pub task: Arc<Mutex<BoxedTaskFuture<GenericTaskResult, GenericTaskResultError>>>,
}

pub struct TaskInfo<GenericTaskResult, GenericTaskResultError, GenericDedupedResult> {
    pub status: TaskState,
    pub result: Option<Result<GenericTaskResult, GenericTaskResultError>>,
    pub on_deduped: Option<Vec<DedupeFuture<GenericDedupedResult>>>,
}

pub struct Queue<GenericTaskResult, GenericTaskResultError, GenericDedupedResult> {
    semaphore: Arc<Semaphore>,
    queue: Arc<RwLock<VecDeque<Task<GenericTaskResult, GenericTaskResultError>>>>,
    index: Arc<Mutex<TaskIndex<GenericTaskResult, GenericTaskResultError, GenericDedupedResult>>>,
    queue_state_tx: watch::Sender<QueueState>,
    queue_state_rx: watch::Receiver<QueueState>,
    tasks_state_tx: watch::Sender<(TaskId, TaskState)>,
    tasks_state_rx: watch::Receiver<(TaskId, TaskState)>,
    /// Ensures at most one coordinator loop is running; avoids concurrent `tick()` spawning duplicate workers.
    coordinator_active: Arc<AtomicBool>,
    /// Wakes the coordinator when a worker `JoinHandle` finishes (including after dedupe callbacks).
    /// `tasks_state` alone is insufficient: task status is sent before follow-up async work.
    coordinator_wake: Arc<Notify>,
    push_done: Arc<RwLock<bool>>,
    #[cfg(test)]
    completion_log: Arc<RwLock<Vec<CompletionLog>>>,
}

impl<GenericTaskResult, GenericTaskResultError, GenericDedupedResult> Queue<GenericTaskResult, GenericTaskResultError, GenericDedupedResult>
where
    GenericTaskResult: Send + 'static,
    GenericTaskResultError: Send + 'static,
    GenericDedupedResult: Send + 'static,
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
            queue: Arc::new(RwLock::new(VecDeque::new())),
            index: Arc::new(Mutex::new(HashMap::new())),
            queue_state_tx,
            queue_state_rx,
            tasks_state_tx,
            tasks_state_rx,
            coordinator_active: Arc::new(AtomicBool::new(false)),
            coordinator_wake: Arc::new(Notify::new()),
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
    /// Error if the task_id is empty or already exists
    ///
    /// # Examples
    ///
    /// ```rust
    /// # tokio_test::block_on(async {
    /// let queue: sprinter::Queue<i32, std::fmt::Error, ()> = sprinter::Queue::new(1);
    /// queue.push(&"task1".to_string(), || async { Ok(1) }).await.unwrap();
    /// # })
    /// ```
    #[rustfmt::skip]
    pub async fn push<GenericTaskClosure, GenericFutureTaskResult>(&self, task_id: &String, task: GenericTaskClosure) -> Result<(), QueueError>
    where
        GenericTaskClosure: FnOnce() -> GenericFutureTaskResult + Send + 'static,
        GenericFutureTaskResult: Future<Output = Result<GenericTaskResult, GenericTaskResultError>> + Send + 'static,
    {
        if task_id.is_empty() {
            return Err(QueueError::Other("task_id cannot be empty".to_string()));
        }

        let mut index = self.index.lock().await;
        if index.contains_key(task_id) {
            drop(index);
            return Err(QueueError::Other("task_id already exists".to_string()));
        }

        index.insert(task_id.clone(), TaskInfo { status: TaskState::Pending, result: None, on_deduped: None });
        drop(index);

        if let Err(e) = self._push(task_id.as_str(), task).await {
            let mut index = self.index.lock().await;
            index.remove(task_id);
            return Err(e);
        }

        Ok(())
    }

    /// Pushes a new task to the queue, it will be executed as soon as it is pushed or when the queue is ready.
    /// Each task must have a unique id, and execution is deduped by `task_id`.
    /// Note that the queue will not complete until `set_push_done` is called, even if all tasks have been completed.
    ///
    /// # Arguments
    ///
    /// * `task_id`: Unique identifier for the task
    /// * `task`: Task to be executed
    /// * `on_deduped`: Callback to be executed if the task is deduplicated
    ///
    /// # Returns
    ///
    /// Error if the task_id is empty
    ///
    /// # Examples
    ///
    /// ```rust
    /// # tokio_test::block_on(async {
    /// let queue: sprinter::Queue<i32, std::fmt::Error, ()> = sprinter::Queue::new(1);
    /// queue.push_deduping(&"task1".to_string(), || async { Ok(1) }, || async {
    ///     println!("task1 deduped!");
    /// }).await.unwrap();
    /// # })
    /// ```
    #[rustfmt::skip]
    pub async fn push_deduping<GenericTaskClosure, GenericFutureTaskResult, GenericDedupedClosure, GenericFutureDedupedResult>(
        &self,
        task_id: &String,
        task: GenericTaskClosure,
        on_deduped: GenericDedupedClosure,
    ) -> Result<(), QueueError>
    where
        GenericTaskClosure: FnOnce() -> GenericFutureTaskResult + Send + 'static,
        GenericDedupedClosure: FnOnce() -> GenericFutureDedupedResult + Send + 'static,
        GenericFutureTaskResult: Future<Output = Result<GenericTaskResult, GenericTaskResultError>> + Send + 'static,
        GenericFutureDedupedResult: Future<Output = GenericDedupedResult> + Send + 'static,
    {
        if task_id.is_empty() {
            return Err(QueueError::Other("task_id cannot be empty".to_string()));
        }

        let mut index = self.index.lock().await;

        let done_already = index.get(task_id).is_some_and(|t| {
            matches!(t.status, TaskState::Succeed | TaskState::Failed)
        });
        if done_already {
            drop(index);
            Box::pin(on_deduped()).await;
            return Ok(());
        }

        let task_info = index.get_mut(task_id);
        if let Some(t) = task_info {
            t.on_deduped.get_or_insert_with(Vec::new).push(Box::pin(on_deduped()));
            drop(index);
            return Ok(());
        }

        index.insert(task_id.clone(), TaskInfo { status: TaskState::Pending, result: None, on_deduped: Some(Vec::new()) });
        drop(index);

        if let Err(e) = self._push(task_id.as_str(), task).await {
            let mut index = self.index.lock().await;
            index.remove(task_id);
            return Err(e);
        }

        Ok(())
    }

    async fn _push<GenericTaskClosure, GenericFutureTaskResult>(
        &self,
        task_id: &str,
        task: GenericTaskClosure,
    ) -> Result<(), QueueError>
    where
        GenericTaskClosure: FnOnce() -> GenericFutureTaskResult + Send + 'static,
        GenericFutureTaskResult: Future<Output = Result<GenericTaskResult, GenericTaskResultError>> + Send + 'static,
    {
        let future = Arc::new(Mutex::new(Box::pin(task()) as BoxedTaskFuture<GenericTaskResult, GenericTaskResultError>));

        let mut queue = self.queue.write().await;
        queue.push_back(Task { id: task_id.to_owned(), task: future });
        drop(queue);

        let add_send = self.tasks_state_tx.send((task_id.to_owned(), TaskState::Add));
        let add_send_failed = failure_injection::tasks_add_send_failed(add_send);
        if add_send_failed {
            let mut queue = self.queue.write().await;
            queue.retain(|t| t.id != task_id);
            return Err(QueueError::Other("tasks state channel closed".to_string()));
        }

        if let Err(e) = self.tick().await {
            let mut queue = self.queue.write().await;
            queue.retain(|t| t.id != task_id);
            return Err(e);
        }
        Ok(())
    }

    /// Starts processing tasks in the queue.
    #[rustfmt::skip]
    async fn tick(&self) -> Result<(), QueueError> {
        if self.coordinator_active.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok() {
            let run_send = self.queue_state_tx.send(QueueState::Running);
            let run_send_failed = failure_injection::queue_state_running_send_failed(run_send);
            if run_send_failed {
                self.coordinator_active.store(false, Ordering::Release);
                return Err(QueueError::Other("queue state channel closed".to_string()));
            }

            let queue = self.queue.clone();
            let index = self.index.clone();
            let push_done = self.push_done.clone();
            let semaphore = self.semaphore.clone();
            let queue_state_tx = self.queue_state_tx.clone();
            let tasks_state_tx = self.tasks_state_tx.clone();
            let mut tasks_state_rx = self.tasks_state_rx.clone();
            let coordinator_active = self.coordinator_active.clone();
            let coordinator_wake = self.coordinator_wake.clone();

            #[cfg(test)] let completion_log = self.completion_log.clone();

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
                    running_tasks
                        .retain(|handle: &tokio::task::JoinHandle<_>| !handle.is_finished());

                    // check if there are any new tasks to process
                    let mut queue_lock = queue.write().await;
                    if let Some(task) = queue_lock.pop_front() {
                        drop(queue_lock);

                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => {
                                coordinator_active.store(false, Ordering::Release);
                                break;
                            }
                        };

                        let task_id = task.id.clone();
                        let index = index.clone();
                        let tasks_state_tx = tasks_state_tx.clone();

                        #[cfg(test)] let completion_log = completion_log.clone();
                        let coordinator_wake_worker = coordinator_wake.clone();
                        let coordinator_active_worker = coordinator_active.clone();

                        let handle = tokio::spawn(async move {
                            let _permit = permit;

                            {
                                let mut index = index.lock().await;
                                if let Some(task_info) = index.get_mut(&task_id) {
                                    task_info.status = TaskState::Running;
                                    let r = tasks_state_tx.send((task_id.clone(), TaskState::Running));
                                    let bad = failure_injection::worker_running_send_failed(r);
                                    if bad {
                                        drop(index);
                                        coordinator_wake_worker.notify_one();
                                        coordinator_active_worker.store(false, Ordering::Release);
                                        return;
                                    }
                                }
                            }

                            let mut future = task.task.lock().await;
                            let result = future.as_mut().await;

                            let dedupe_futures = {
                                let mut index = index.lock().await;
                                if let Some(task_info) = index.get_mut(&task_id) {
                                    let state_update = if result.is_ok() {
                                        TaskState::Succeed
                                    } else {
                                        TaskState::Failed
                                    };
                                    let r = tasks_state_tx.send((task_id.clone(), state_update));
                                    let bad = failure_injection::worker_final_send_failed(r);
                                    if bad {
                                        drop(index);
                                        coordinator_wake_worker.notify_one();
                                        coordinator_active_worker.store(false, Ordering::Release);
                                        return;
                                    }
                                    task_info.status = state_update;
                                    task_info.result = Some(result);
                                    task_info.on_deduped.take()
                                } else {
                                    None
                                }
                            };
                            if let Some(mut callbacks) = dedupe_futures {
                                for cb in callbacks.drain(..) {
                                    cb.await;
                                }
                            }

                            #[cfg(test)] completion_log.write().await.push(CompletionLog { task_id: task_id.clone() });
                            coordinator_wake_worker.notify_one();
                        });

                        running_tasks.push(handle);
                    } else {
                        drop(queue_lock);
                    }

                    // check if all tasks are done
                    let queue_empty = queue.read().await.is_empty();
                    let tasks_all_pushed = *push_done.read().await;

                    // println!("DEBUG  end loop, queue_empty: {:?}, running_tasks: {:?}, tasks_all_pushed: {:?}", queue_empty, running_tasks.len(), tasks_all_pushed);

                    if queue_empty && running_tasks.is_empty() && tasks_all_pushed {
                        let _ = queue_state_tx.send(QueueState::Done);
                        coordinator_active.store(false, Ordering::Release);
                        break;
                    }
                    tokio::select! {
                        biased;
                        _ = coordinator_wake.notified() => { std::hint::black_box(()) }
                        r = tasks_state_rx.changed() => {
                            let bad = failure_injection::tasks_changed_failed(r);
                            if bad {
                                coordinator_active.store(false, Ordering::Release);
                                break;
                            }
                        }
                    }
                }
            });
        }
        Ok(())
    }

    /// Signals that all tasks have been pushed to the queue.
    /// This is needed since the queue starts processing as soon as the first task is pushed.
    /// Note the queue will not complete until `set_push_done` is called, even if all tasks have been completed.
    pub async fn set_push_done(&self) -> Result<(), QueueError> {
        let mut push_done = self.push_done.write().await;
        *push_done = true;
        let r = self
            .tasks_state_tx
            .send((MARKER_TASK_ID_PUSH_DONE.to_string(), TaskState::AllPushed));
        let bad = failure_injection::set_push_done_send_failed(r);
        if bad {
            return Err(QueueError::Other("tasks state channel closed".to_string()));
        }
        Ok(())
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
    #[rustfmt::skip]
    pub async fn wait_for_task_done(
        &self,
        task_id: &String,
    ) -> Result<Result<GenericTaskResult, GenericTaskResultError>, QueueError>
    where
        GenericTaskResult: Clone,
        GenericTaskResultError: Clone,
    {
        let mut rx = self.tasks_state_rx.clone();
        loop {
            {
                let index = self.index.lock().await;
                match index.get(task_id) {
                    None => return Err(QueueError::TaskNotFound(task_id.clone())),
                    Some(info) => match &info.status {
                        TaskState::Succeed | TaskState::Failed => {
                            let inner = info.result.clone().ok_or_else(|| {
                                QueueError::Other("internal error: task finished without stored result".to_string())
                            })?;
                            return Ok(inner);
                        }
                        _ => {
                            std::hint::black_box(());
                        }
                    },
                }
            }
            let ch = rx.changed().await;
            let bad = failure_injection::wait_task_changed_failed(ch);
            if bad {
                return Err(QueueError::Other("tasks state channel closed".to_string()));
            }
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
    /// queue.set_push_done().await.unwrap();
    /// queue.wait_for_tasks_done().await.unwrap();
    /// # })
    /// ```
    pub async fn wait_for_tasks_done(&self) -> Result<(), QueueError> {
        let mut rx = self.queue_state_rx.clone();
        let w = rx.wait_for(|state| *state == QueueState::Done).await;
        let bad = failure_injection::queue_wait_failed(w);
        if bad {
            return Err(QueueError::Other("queue state channel closed".to_string()));
        }
        Ok(())
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
    /// queue.set_push_done().await.unwrap();
    /// let _results = queue.wait_for_results().await.unwrap();
    /// # })
    /// ```
    pub async fn wait_for_results(
        &self,
    ) -> Result<HashMap<String, Result<Result<GenericTaskResult, GenericTaskResultError>, QueueError>>, QueueError>
    where
        GenericTaskResult: Clone,
        GenericTaskResultError: Clone,
    {
        let mut rx = self.queue_state_rx.clone();
        let w = rx.wait_for(|state| *state == QueueState::Done).await;
        let bad = failure_injection::queue_wait_failed(w);
        if bad {
            return Err(QueueError::Other("queue state channel closed".to_string()));
        }

        let index = self.index.lock().await;
        let mut out = HashMap::with_capacity(index.len());
        for (name, info) in index.iter() {
            let entry = match &info.result {
                Some(r) => Ok(r.clone()),
                None => Err(QueueError::Other(format!("task {} finished without stored result", name))),
            };
            out.insert(name.clone(), entry);
        }
        Ok(out)
    }

    /// Returns the current state of the queue.
    pub async fn state(&self) -> QueueState {
        self.queue_state_rx.borrow().clone()
    }

    #[cfg(test)]
    pub async fn _test_get_completion_order(&self) -> Vec<String> {
        self.completion_log.read().await.iter().map(|log| log.task_id.clone()).collect()
    }

    pub async fn reset(&self) -> Result<(), QueueError> {
        let mut index = self.index.lock().await;
        index.clear();
        let mut queue = self.queue.write().await;
        queue.clear();
        let mut all_tasks_pushed = self.push_done.write().await;
        *all_tasks_pushed = false;

        #[cfg(test)]
        {
            let mut completion_log = self.completion_log.write().await;
            completion_log.clear();
        }
        self.coordinator_active.store(false, Ordering::Release);
        let r = self.queue_state_tx.send(QueueState::Idle);
        let bad = failure_injection::reset_queue_state_send_failed(r);
        if bad {
            return Err(QueueError::Other("queue state channel closed".to_string()));
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn test_close_semaphore(&self) {
        self.semaphore.close();
    }

    #[cfg(test)]
    pub async fn test_corrupt_terminal_without_result(&self, task_id: &str) {
        let mut index = self.index.lock().await;
        if let Some(t) = index.get_mut(task_id) {
            t.status = TaskState::Succeed;
            t.result = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fmt::Error,
        sync::atomic::{AtomicBool, AtomicU32, Ordering as AtomicOrdering},
        time::Duration,
    };
    use tokio::time::sleep;

    #[tokio::test]
    async fn queue_should_run_tasks_in_parallel() {
        let _s = crate::test_serial();
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
        queue.push(&"t1".to_string(), task1).await.unwrap();
        queue.push(&"t2".to_string(), task2).await.unwrap();
        queue.push(&"t3".to_string(), task3).await.unwrap();
        queue.push(&"t4".to_string(), task4).await.unwrap();
        queue.push(&"t5".to_string(), task5).await.unwrap();
        queue.push(&"t6".to_string(), task6).await.unwrap();
        queue.set_push_done().await.unwrap();

        let results = queue.wait_for_results().await.unwrap();

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
        // Wall-clock cap: longest task is 1500ms; allow generous slack for CI and coverage tools
        // (e.g. tarpaulin) where scheduling is slower. Correctness is enforced by completion order.
        assert!(end - start < tokio::time::Duration::from_millis(3500), "sprint exceeded parallel wall-clock budget - {:?}", end - start);
    }

    #[tokio::test]
    async fn queue_should_run_tasks_with_delay_between_pushing_tasks() {
        let _s = crate::test_serial();
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
        queue.push(&"t1".to_string(), task1).await.unwrap();
        sleep(tokio::time::Duration::from_millis(100)).await;
        queue.push(&"t2".to_string(), task2).await.unwrap();
        sleep(tokio::time::Duration::from_millis(100)).await;
        queue.push(&"t3".to_string(), task3).await.unwrap();
        sleep(tokio::time::Duration::from_millis(100)).await;
        queue.push(&"t4".to_string(), task4).await.unwrap();
        sleep(tokio::time::Duration::from_millis(100)).await;
        queue.push(&"t5".to_string(), task5).await.unwrap();
        sleep(tokio::time::Duration::from_millis(100)).await;
        queue.push(&"t6".to_string(), task6).await.unwrap();
        queue.set_push_done().await.unwrap();

        let results = queue.wait_for_results().await.unwrap();

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
        assert!(end - start < tokio::time::Duration::from_millis(4000), "sprint exceeded parallel wall-clock budget - {:?}", end - start);
    }

    #[tokio::test]
    async fn task_done_should_return_result_when_ready() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(2);

        let task = || async {
            sleep(tokio::time::Duration::from_millis(100)).await;
            Ok(42)
        };

        let task_id = "test_task".to_string();
        queue.push(&task_id, task).await.unwrap();
        queue.set_push_done().await.unwrap();

        let result = queue.wait_for_task_done(&task_id).await.unwrap();
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn task_done_should_return_error_for_nonexistent_task() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        let result = queue.wait_for_task_done(&"nonexistent".to_string()).await;
        assert!(matches!(result, Err(QueueError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn task_push_should_fail_for_empty_task_id() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        let result = queue.push(&"".to_string(), || async { Ok(1) }).await;
        assert_eq!(result, Err(QueueError::Other("task_id cannot be empty".to_string())));
    }

    #[tokio::test]
    async fn queue_should_run_tasks_deduping() {
        let _s = crate::test_serial();
        let queue: Queue<&str, Error, ()> = Queue::new(3);
        let tasks_counter: Arc<Mutex<HashMap<String, u32>>> = Arc::new(Mutex::new(HashMap::new()));
        let dedupe_counter: Arc<Mutex<HashMap<String, u32>>> = Arc::new(Mutex::new(HashMap::new()));

        let task_counter_ref = Arc::clone(&tasks_counter);
        let dedupe_counter_ref = Arc::clone(&dedupe_counter);
        assert_eq!(
            queue
                .push_deduping(
                    &"t1".to_string(),
                    move || async move {
                        let mut c = task_counter_ref.lock().await;
                        let count = c.entry("t1".to_string()).or_insert(0);
                        *count += 1;
                        sleep(tokio::time::Duration::from_millis(50)).await;
                        Ok::<&str, Error>("result1")
                    },
                    move || async move {
                        let mut c = dedupe_counter_ref.lock().await;
                        let count = c.entry("t1".to_string()).or_insert(0);
                        *count += 1;
                    }
                )
                .await,
            Ok(())
        );
        let task_counter_ref = Arc::clone(&tasks_counter);
        let dedupe_counter_ref = Arc::clone(&dedupe_counter);
        assert_eq!(
            queue
                .push_deduping(
                    &"t1".to_string(),
                    move || async move {
                        let mut c = task_counter_ref.lock().await;
                        let count = c.entry("t1".to_string()).or_insert(0);
                        *count += 1;
                        sleep(tokio::time::Duration::from_millis(50)).await;
                        Ok::<&str, Error>("result1")
                    },
                    move || async move {
                        let mut c = dedupe_counter_ref.lock().await;
                        let count = c.entry("t1".to_string()).or_insert(0);
                        *count += 1;
                    }
                )
                .await,
            Ok(())
        );
        let task_counter_ref = Arc::clone(&tasks_counter);
        let dedupe_counter_ref = Arc::clone(&dedupe_counter);
        assert_eq!(
            queue
                .push_deduping(
                    &"t2".to_string(),
                    move || async move {
                        let mut c = task_counter_ref.lock().await;
                        let count = c.entry("t2".to_string()).or_insert(0);
                        *count += 1;
                        sleep(tokio::time::Duration::from_millis(50)).await;
                        Ok::<&str, Error>("result2")
                    },
                    move || async move {
                        let mut c = dedupe_counter_ref.lock().await;
                        let count = c.entry("t2".to_string()).or_insert(0);
                        *count += 1;
                    }
                )
                .await,
            Ok(())
        );
        let task_counter_ref = Arc::clone(&tasks_counter);
        let dedupe_counter_ref = Arc::clone(&dedupe_counter);
        assert_eq!(
            queue
                .push_deduping(
                    &"t3".to_string(),
                    move || async move {
                        let mut c = task_counter_ref.lock().await;
                        let count = c.entry("t3".to_string()).or_insert(0);
                        *count += 1;
                        sleep(tokio::time::Duration::from_millis(50)).await;
                        Ok::<&str, Error>("result3")
                    },
                    move || async move {
                        let mut c = dedupe_counter_ref.lock().await;
                        let count = c.entry("t3".to_string()).or_insert(0);
                        *count += 1;
                    }
                )
                .await,
            Ok(())
        );
        let task_counter_ref = Arc::clone(&tasks_counter);
        let dedupe_counter_ref = Arc::clone(&dedupe_counter);
        assert_eq!(
            queue
                .push_deduping(
                    &"t1".to_string(),
                    move || async move {
                        let mut c = task_counter_ref.lock().await;
                        let count = c.entry("t1".to_string()).or_insert(0);
                        *count += 1;
                        sleep(tokio::time::Duration::from_millis(50)).await;
                        Ok::<&str, Error>("result1")
                    },
                    move || async move {
                        let mut c = dedupe_counter_ref.lock().await;
                        let count = c.entry("t1".to_string()).or_insert(0);
                        *count += 1;
                    }
                )
                .await,
            Ok(())
        );
        queue.set_push_done().await.unwrap();

        let results = queue.wait_for_results().await.unwrap();

        let expected_results = HashMap::from([("t1".to_string(), Ok(Ok("result1"))), ("t2".to_string(), Ok(Ok("result2"))), ("t3".to_string(), Ok(Ok("result3")))]);
        assert_eq!(results, expected_results);

        let completion_order = queue._test_get_completion_order().await;
        assert_eq!(completion_order, vec!["t1", "t2", "t3"]);

        let task_counter_ref = Arc::clone(&tasks_counter);
        let counter = task_counter_ref.lock().await;
        assert_eq!(counter.get("t1").unwrap(), &1);
        assert_eq!(counter.get("t2").unwrap(), &1);
        assert_eq!(counter.get("t3").unwrap(), &1);

        let dedupe_counter_ref = Arc::clone(&dedupe_counter);
        let counter = dedupe_counter_ref.lock().await;
        assert_eq!(counter.get("t1").unwrap(), &2);
        assert!(counter.get("t2").is_none());
        assert!(counter.get("t3").is_none());
    }

    #[tokio::test]
    async fn on_deduped_should_be_called() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(2);
        let task = || async { Ok(1) };
        let deduped = Arc::new(AtomicBool::new(false));

        queue
            .push_deduping(&"task1".to_string(), task, || async {
                unreachable!("on_deduped on first task should not be called");
            })
            .await
            .unwrap();
        let d = Arc::clone(&deduped);
        queue
            .push_deduping(&"task1".to_string(), task, move || async move {
                // println!("on_deduped on second task");
                d.store(true, std::sync::atomic::Ordering::Release);
            })
            .await
            .unwrap();

        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();

        let d = Arc::try_unwrap(deduped).unwrap();
        assert!(d.into_inner(), "on_deduped on second task should be called");
    }

    #[tokio::test]
    async fn concurrent_push_many_tasks_completes() {
        let _s = crate::test_serial();
        let queue = Arc::new(Queue::<i32, Error, ()>::new(12));
        let n = 64;
        let mut join = tokio::task::JoinSet::new();
        for i in 0..n {
            let q = Arc::clone(&queue);
            join.spawn(async move {
                let id = format!("t{}", i);
                q.push(&id, move || async move {
                    sleep(Duration::from_millis(1 + (i % 7) as u64)).await;
                    Ok(i)
                })
                .await
                .unwrap();
            });
        }
        while let Some(res) = join.join_next().await {
            res.unwrap();
        }
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        let results = queue.wait_for_results().await.unwrap();
        assert_eq!(results.len(), n as usize);
        for i in 0..n {
            let id = format!("t{}", i);
            assert_eq!(results.get(&id), Some(&Ok(Ok(i))));
        }
    }

    #[tokio::test]
    async fn deduped_callback_can_wait_for_another_task_without_deadlock() {
        let _s = crate::test_serial();
        let queue = Arc::new(Queue::<i32, Error, ()>::new(4));
        queue
            .push(&"slow".to_string(), || async {
                sleep(Duration::from_millis(40)).await;
                Ok(1)
            })
            .await
            .unwrap();

        let q_wait = Arc::clone(&queue);
        queue
            .push_deduping(
                &"slow".to_string(),
                || async { Ok(1) },
                move || async move {
                    assert_eq!(q_wait.wait_for_task_done(&"other".to_string()).await.unwrap(), Ok(2));
                },
            )
            .await
            .unwrap();

        queue
            .push(&"other".to_string(), || async {
                sleep(Duration::from_millis(80)).await;
                Ok(2)
            })
            .await
            .unwrap();

        queue.set_push_done().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), queue.wait_for_tasks_done())
            .await
            .expect("deadlock or stall: wait_for_tasks_done timed out")
            .unwrap();
        assert_eq!(queue.wait_for_task_done(&"other".to_string()).await.unwrap(), Ok(2));
    }

    #[tokio::test]
    async fn stress_hundreds_of_tasks() {
        let _s = crate::test_serial();
        let queue = Arc::new(Queue::<u32, Error, ()>::new(16));
        let count = 256;
        let mut join = tokio::task::JoinSet::new();
        for i in 0..count {
            let q = Arc::clone(&queue);
            join.spawn(async move {
                let id = format!("job{}", i);
                q.push(&id, move || async move {
                    if i % 61 == 0 {
                        sleep(Duration::from_millis(2)).await;
                    }
                    Ok(i as u32)
                })
                .await
                .unwrap();
            });
        }
        while let Some(r) = join.join_next().await {
            r.unwrap();
        }
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        let results = queue.wait_for_results().await.unwrap();
        assert_eq!(results.len(), count);
    }

    #[tokio::test]
    async fn two_batches_without_reset_completes() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(2);
        queue.push(&"a".to_string(), || async { Ok(10) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();

        queue.push(&"b".to_string(), || async { Ok(20) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();

        assert_eq!(queue.wait_for_task_done(&"b".to_string()).await.unwrap(), Ok(20));
    }

    #[tokio::test]
    async fn wait_for_results_includes_failures() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(2);
        queue.push(&"ok".to_string(), || async { Ok(1) }).await.unwrap();
        queue.push(&"bad".to_string(), || async { Err(Error) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        let map = queue.wait_for_results().await.unwrap();
        assert_eq!(map.get("ok"), Some(&Ok(Ok(1))));
        assert!(matches!(map.get("bad"), Some(Ok(Err(_)))));
    }

    #[tokio::test]
    async fn mix_push_and_push_deduping_serializes_primary_task() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(2);
        let runs = Arc::new(AtomicU32::new(0));

        let r = Arc::clone(&runs);
        queue
            .push(&"x".to_string(), move || async move {
                r.fetch_add(1, AtomicOrdering::SeqCst);
                sleep(Duration::from_millis(20)).await;
                Ok(7)
            })
            .await
            .unwrap();

        let r = Arc::clone(&runs);
        queue
            .push_deduping(
                &"x".to_string(),
                move || async move {
                    r.fetch_add(1, AtomicOrdering::SeqCst);
                    Ok(7)
                },
                || async {},
            )
            .await
            .unwrap();

        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();

        assert_eq!(runs.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(queue.wait_for_task_done(&"x".to_string()).await.unwrap(), Ok(7));
    }

    #[tokio::test]
    async fn push_deduped_runs_when_primary_already_finished_before_dedupe_push() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        queue.push(&"p".to_string(), || async { Ok(99) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();

        let ran = Arc::new(AtomicBool::new(false));
        let ran_cb = Arc::clone(&ran);
        queue
            .push_deduping(
                &"p".to_string(),
                || async { Ok(99) },
                move || async move {
                    ran_cb.store(true, AtomicOrdering::SeqCst);
                },
            )
            .await
            .unwrap();

        assert!(ran.load(AtomicOrdering::SeqCst));
        assert_eq!(queue.wait_for_task_done(&"p".to_string()).await.unwrap(), Ok(99));
    }

    #[tokio::test]
    async fn state_transitions_to_done_then_allows_idle_after_reset() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        queue.push(&"only".to_string(), || async { Ok(1) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        assert_eq!(queue.state().await, QueueState::Done);
        queue.reset().await.unwrap();
        assert_eq!(queue.state().await, QueueState::Idle);
    }

    #[tokio::test]
    async fn push_rejects_duplicate_task_id() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        queue.push(&"id".to_string(), || async { Ok(1) }).await.unwrap();
        let err = queue.push(&"id".to_string(), || async { Ok(2) }).await.unwrap_err();
        assert_eq!(err, QueueError::Other("task_id already exists".to_string()));
    }

    #[tokio::test]
    async fn push_deduping_rejects_empty_task_id() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        let err = queue.push_deduping(&"".to_string(), || async { Ok(1) }, || async {}).await.unwrap_err();
        assert_eq!(err, QueueError::Other("task_id cannot be empty".to_string()));
    }

    #[tokio::test]
    async fn wait_for_task_done_returns_inner_error_when_task_fails() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        queue.push(&"f".to_string(), || async { Err(Error) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        let out = queue.wait_for_task_done(&"f".to_string()).await.unwrap();
        assert!(out.is_err());
    }

    #[tokio::test]
    async fn wait_for_task_done_not_found_after_reset() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        queue.push(&"gone".to_string(), || async { Ok(1) }).await.unwrap();
        queue.reset().await.unwrap();
        assert!(matches!(queue.wait_for_task_done(&"gone".to_string()).await, Err(QueueError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn concurrent_waiters_receive_same_result() {
        let _s = crate::test_serial();
        let queue = Arc::new(Queue::<i32, Error, ()>::new(2));
        queue
            .push(&"shared".to_string(), || async {
                sleep(Duration::from_millis(30)).await;
                Ok(7)
            })
            .await
            .unwrap();
        queue.set_push_done().await.unwrap();
        let q1 = Arc::clone(&queue);
        let q2 = Arc::clone(&queue);
        let (a, b) = tokio::join!(async move { q1.wait_for_task_done(&"shared".to_string()).await.unwrap() }, async move {
            q2.wait_for_task_done(&"shared".to_string()).await.unwrap()
        },);
        assert_eq!(a, Ok(7));
        assert_eq!(b, Ok(7));
    }

    #[tokio::test]
    async fn peak_concurrency_never_exceeds_semaphore_limit() {
        let _s = crate::test_serial();
        let limit: u32 = 3;
        let queue = Arc::new(Queue::<(), Error, ()>::new(limit as usize));
        let in_flight = Arc::new(AtomicU32::new(0));
        let peak = Arc::new(AtomicU32::new(0));
        let task_count = 20u32;

        for i in 0..task_count {
            let infl = Arc::clone(&in_flight);
            let pk = Arc::clone(&peak);
            queue
                .push(&format!("w{i}"), move || async move {
                    let n = infl.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                    pk.fetch_max(n, AtomicOrdering::SeqCst);
                    sleep(Duration::from_millis(15)).await;
                    infl.fetch_sub(1, AtomicOrdering::SeqCst);
                    Ok(())
                })
                .await
                .unwrap();
        }
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        assert_eq!(in_flight.load(AtomicOrdering::SeqCst), 0);
        assert!(peak.load(AtomicOrdering::SeqCst) <= limit, "peak {} > limit {}", peak.load(AtomicOrdering::SeqCst), limit);
        assert!(peak.load(AtomicOrdering::SeqCst) >= 1, "expected some overlap under load");
    }

    #[tokio::test]
    async fn dedupe_callbacks_run_fifo_relative_to_push_order() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        let order = Arc::new(Mutex::new(Vec::<u32>::new()));

        queue
            .push(&"k".to_string(), || async {
                sleep(Duration::from_millis(25)).await;
                Ok(1)
            })
            .await
            .unwrap();

        for seq in [1u32, 2, 3] {
            let ord = Arc::clone(&order);
            queue
                .push_deduping(
                    &"k".to_string(),
                    || async { Ok(1) },
                    move || async move {
                        ord.lock().await.push(seq);
                    },
                )
                .await
                .unwrap();
        }

        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();

        assert_eq!(*order.lock().await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn state_is_running_while_work_in_flight() {
        let _s = crate::test_serial();
        let queue = Arc::new(Queue::<i32, Error, ()>::new(1));
        let gate = Arc::new(tokio::sync::Barrier::new(2));
        assert!(matches!(queue.state().await, QueueState::Idle | QueueState::Running));

        queue
            .push(&"long".to_string(), {
                let gate = Arc::clone(&gate);
                move || async move {
                    gate.wait().await;
                    Ok(0)
                }
            })
            .await
            .unwrap();

        sleep(Duration::from_millis(5)).await;
        assert_eq!(queue.state().await, QueueState::Running);

        gate.wait().await;
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        assert_eq!(queue.state().await, QueueState::Done);
    }

    #[tokio::test]
    async fn concurrent_push_deduping_aggregates_many_callbacks() {
        let _s = crate::test_serial();
        let queue = Arc::new(Queue::<i32, Error, ()>::new(8));
        queue
            .push(&"one".to_string(), || async {
                sleep(Duration::from_millis(20)).await;
                Ok(1)
            })
            .await
            .unwrap();

        let hits = Arc::new(AtomicU32::new(0));
        let mut join = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let q = Arc::clone(&queue);
            let h = Arc::clone(&hits);
            join.spawn(async move {
                q.push_deduping(
                    &"one".to_string(),
                    || async { Ok(1) },
                    move || async move {
                        h.fetch_add(1, AtomicOrdering::SeqCst);
                    },
                )
                .await
                .unwrap();
            });
        }
        while let Some(r) = join.join_next().await {
            r.unwrap();
        }

        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 32);
        assert_eq!(queue.wait_for_task_done(&"one".to_string()).await.unwrap(), Ok(1));
    }

    #[tokio::test]
    async fn reset_then_push_runs_fresh_session() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(2);
        queue.push(&"first".to_string(), || async { Ok(1) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        queue.reset().await.unwrap();

        queue.push(&"first".to_string(), || async { Ok(42) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        assert_eq!(queue.wait_for_task_done(&"first".to_string()).await.unwrap(), Ok(42));
    }

    #[tokio::test]
    async fn second_tick_while_coordinator_running_returns_immediately() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue
            .push(&"a".to_string(), || async {
                sleep(Duration::from_millis(40)).await;
                Ok(1)
            })
            .await
            .unwrap();
        for _ in 0..5 {
            queue.tick().await.unwrap();
        }
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_task_done_polls_non_terminal_status() {
        let _s = crate::test_serial();
        let queue = Arc::new(Queue::<i32, Error, ()>::new(1));
        queue
            .push(&"slow".to_string(), || async {
                sleep(Duration::from_millis(200)).await;
                Ok(42)
            })
            .await
            .unwrap();
        let q = Arc::clone(&queue);
        let waiter = tokio::spawn(async move { q.wait_for_task_done(&"slow".to_string()).await.unwrap() });
        sleep(Duration::from_millis(30)).await;
        queue.set_push_done().await.unwrap();
        assert_eq!(waiter.await.unwrap(), Ok(42));
    }

    #[tokio::test]
    async fn first_push_deduping_inserts_fresh_task_via_dedupe_path() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        queue.push_deduping(&"only".to_string(), || async { Ok(99) }, || async {}).await.unwrap();
        queue.set_push_done().await.unwrap();
        assert_eq!(queue.wait_for_task_done(&"only".to_string()).await.unwrap(), Ok(99));
    }

    #[tokio::test]
    async fn push_deduping_immediate_callback_when_primary_failed() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, ()> = Queue::new(1);
        queue.push(&"f".to_string(), || async { Err(Error) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();

        let fired = Arc::new(AtomicBool::new(false));
        let f = Arc::clone(&fired);
        queue
            .push_deduping(
                &"f".to_string(),
                || async { Ok(0) },
                move || async move {
                    f.store(true, AtomicOrdering::SeqCst);
                },
            )
            .await
            .unwrap();
        assert!(fired.load(AtomicOrdering::SeqCst));
        assert!(queue.wait_for_task_done(&"f".to_string()).await.unwrap().is_err());
    }

    #[tokio::test]
    async fn reset_while_task_running_does_not_panic() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue
            .push(&"r".to_string(), || async {
                sleep(Duration::from_millis(80)).await;
                Ok(9)
            })
            .await
            .unwrap();
        sleep(Duration::from_millis(15)).await;
        queue.reset().await.unwrap();
        sleep(Duration::from_millis(150)).await;
    }

    #[tokio::test]
    async fn marker_task_id_constant_matches_push_done_marker() {
        let _s = crate::test_serial();
        assert_eq!(MARKER_TASK_ID_PUSH_DONE, "#");
    }

    #[tokio::test]
    async fn readme_basic_example() {
        let _s = crate::test_serial();
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
        queue.set_push_done().await.unwrap();

        // Wait for all tasks to complete and get results
        let results = queue.wait_for_results().await.unwrap();

        let end = tokio::time::Instant::now();
        println!("sprint done in {:?}", end - start);
        println!("results: {:?}", results);

        assert_eq!(
            results,
            HashMap::from([("task1".to_string(), Ok(Ok(1))), ("task2".to_string(), Ok(Ok(2))), ("task3".to_string(), Ok(Ok(3))),])
        );
        let completion_order = queue._test_get_completion_order().await;
        assert_eq!(completion_order, vec!["task2", "task3", "task1"]);
    }

    #[tokio::test]
    async fn hook_tasks_add_send_failure_roll_back_push() {
        let _h = HookTestSession::new();
        crate::failure_injection::FAIL_TASKS_SEND_ADD.store(true, AtomicOrdering::SeqCst);
        let queue = Queue::<i32, Error, ()>::new(1);
        assert!(queue.push(&"x".to_string(), || async { Ok(1) }).await.is_err());
        assert!(matches!(queue.wait_for_task_done(&"x".to_string()).await, Err(QueueError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn hook_tick_queue_state_failure_roll_back_push() {
        let _h = HookTestSession::new();
        crate::failure_injection::FAIL_QUEUE_STATE_ON_TICK.store(true, AtomicOrdering::SeqCst);
        let queue = Queue::<i32, Error, ()>::new(1);
        assert!(queue.push(&"x".to_string(), || async { Ok(1) }).await.is_err());
        assert!(matches!(queue.wait_for_task_done(&"x".to_string()).await, Err(QueueError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn hook_tasks_add_send_failure_roll_back_push_deduping_insert() {
        let _h = HookTestSession::new();
        crate::failure_injection::FAIL_TASKS_SEND_ADD.store(true, AtomicOrdering::SeqCst);
        let queue = Queue::<i32, Error, ()>::new(1);
        assert!(queue.push_deduping(&"x".to_string(), || async { Ok(1) }, || async {}).await.is_err());
        assert!(matches!(queue.wait_for_task_done(&"x".to_string()).await, Err(QueueError::TaskNotFound(_))));
    }

    #[tokio::test]
    async fn closed_semaphore_stalls_until_wait_timeout() {
        let _h = HookTestSession::new();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.test_close_semaphore();
        queue.push(&"x".to_string(), || async { Ok(1) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(2), queue.wait_for_tasks_done()).await.is_err());
    }

    #[tokio::test]
    async fn hook_worker_running_send_failure_never_finishes_task() {
        let _h = HookTestSession::new();
        crate::failure_injection::FAIL_WORKER_RUNNING_SEND.store(true, AtomicOrdering::SeqCst);
        let queue = Queue::<i32, Error, ()>::new(1);
        queue
            .push(&"x".to_string(), || async {
                sleep(Duration::from_millis(80)).await;
                Ok(1)
            })
            .await
            .unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(500), queue.wait_for_task_done(&"x".to_string())).await.is_err());
    }

    #[tokio::test]
    async fn hook_worker_final_send_failure_never_finishes_task() {
        let _h = HookTestSession::new();
        crate::failure_injection::FAIL_WORKER_FINAL_SEND.store(true, AtomicOrdering::SeqCst);
        let queue = Queue::<i32, Error, ()>::new(1);
        queue
            .push(&"x".to_string(), || async {
                sleep(Duration::from_millis(80)).await;
                Ok(1)
            })
            .await
            .unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(500), queue.wait_for_task_done(&"x".to_string())).await.is_err());
    }

    #[tokio::test]
    async fn hook_coordinator_tasks_changed_break_stalls_wait() {
        let _h = HookTestSession::new();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue
            .push(&"x".to_string(), || async {
                sleep(Duration::from_millis(400)).await;
                Ok(1)
            })
            .await
            .unwrap();
        sleep(Duration::from_millis(30)).await;
        crate::failure_injection::FAIL_COORD_TASKS_CHANGED.store(true, AtomicOrdering::SeqCst);
        queue.set_push_done().await.unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(2), queue.wait_for_tasks_done()).await.is_err());
    }

    #[tokio::test]
    async fn hook_wait_task_done_changed_returns_channel_error() {
        let _h = HookTestSession::new();
        crate::failure_injection::FAIL_WAIT_TASK_CHANGED.store(true, AtomicOrdering::SeqCst);
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.push(&"x".to_string(), || async { Ok(1) }).await.unwrap();
        let r = queue.wait_for_task_done(&"x".to_string()).await;
        assert!(matches!(r, Err(QueueError::Other(_))));
    }

    #[tokio::test]
    async fn hook_queue_state_wait_returns_error() {
        let _h = HookTestSession::new();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.push(&"x".to_string(), || async { Ok(1) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        crate::failure_injection::FAIL_QUEUE_STATE_WAIT.store(true, AtomicOrdering::SeqCst);
        assert!(queue.wait_for_tasks_done().await.is_err());
    }

    #[tokio::test]
    async fn hook_set_push_done_send_returns_error() {
        let _h = HookTestSession::new();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.push(&"x".to_string(), || async { Ok(1) }).await.unwrap();
        crate::failure_injection::FAIL_SET_PUSH_DONE_SEND.store(true, AtomicOrdering::SeqCst);
        assert!(queue.set_push_done().await.is_err());
    }

    #[tokio::test]
    async fn hook_reset_queue_state_send_returns_error() {
        let _h = HookTestSession::new();
        let queue = Queue::<i32, Error, ()>::new(1);
        crate::failure_injection::FAIL_RESET_QUEUE_STATE.store(true, AtomicOrdering::SeqCst);
        assert!(queue.reset().await.is_err());
    }

    #[tokio::test]
    async fn wait_for_task_done_other_when_terminal_without_result() {
        let _h = HookTestSession::new();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.push(&"c".to_string(), || async { Ok(7) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        queue.test_corrupt_terminal_without_result("c").await;
        let r = queue.wait_for_task_done(&"c".to_string()).await;
        assert!(matches!(r, Err(QueueError::Other(_))));
    }

    #[tokio::test]
    async fn wait_for_results_marks_missing_result_per_task() {
        let _h = HookTestSession::new();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.push(&"c".to_string(), || async { Ok(7) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        queue.test_corrupt_terminal_without_result("c").await;
        let m = queue.wait_for_results().await.unwrap();
        assert!(m.get("c").is_some_and(|r| matches!(r, Err(QueueError::Other(_)))));
    }

    #[tokio::test]
    async fn hook_queue_state_wait_errors_wait_for_results() {
        let _h = HookTestSession::new();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.push(&"x".to_string(), || async { Ok(1) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        crate::failure_injection::FAIL_QUEUE_STATE_WAIT.store(true, AtomicOrdering::SeqCst);
        assert!(queue.wait_for_results().await.is_err());
    }

    #[tokio::test]
    async fn wait_for_tasks_done_times_out_if_only_set_push_done_without_any_push() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.set_push_done().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), queue.wait_for_tasks_done())
                .await
                .is_err(),
            "coordinator is never started without a push, so Done is never signalled"
        );
    }

    #[tokio::test]
    async fn wait_for_tasks_done_times_out_without_set_push_done_even_when_work_finished() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.push(&"a".to_string(), || async { Ok(1) }).await.unwrap();
        sleep(Duration::from_millis(120)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), queue.wait_for_tasks_done())
                .await
                .is_err(),
            "queue stays incomplete until set_push_done even if all tasks returned"
        );
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
    }

    #[tokio::test]
    async fn zero_concurrency_signals_push_ok_but_never_executes_wait_stalls() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(0);
        queue.push(&"stuck".to_string(), || async { Ok(42) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(250), queue.wait_for_tasks_done())
                .await
                .is_err(),
            "semaphore has no permits, so no worker can run"
        );
    }

    #[tokio::test]
    async fn wait_for_task_done_succeeds_before_set_push_done_once_task_finishes() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.push(&"x".to_string(), || async { Ok(99) }).await.unwrap();
        sleep(Duration::from_millis(80)).await;
        assert_eq!(queue.wait_for_task_done(&"x".to_string()).await.unwrap(), Ok(99));
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
    }

    #[tokio::test]
    async fn push_rejected_when_id_reserved_by_prior_push_deduping() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue
            .push_deduping(&"x".to_string(), || async { Ok(1) }, || async {})
            .await
            .unwrap();
        let err = queue.push(&"x".to_string(), || async { Ok(2) }).await.unwrap_err();
        assert_eq!(err, QueueError::Other("task_id already exists".to_string()));
    }

    #[tokio::test]
    async fn cannot_push_same_id_again_after_task_completed_until_reset() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.push(&"only".to_string(), || async { Ok(1) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        let err = queue.push(&"only".to_string(), || async { Ok(2) }).await.unwrap_err();
        assert_eq!(err, QueueError::Other("task_id already exists".to_string()));
    }

    #[tokio::test]
    async fn three_sequential_sessions_on_one_queue_without_reset() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(1);
        for (id, v) in [("s1", 10i32), ("s2", 20), ("s3", 30)] {
            queue
                .push(&id.to_string(), move || async move { Ok(v) })
                .await
                .unwrap();
            queue.set_push_done().await.unwrap();
            queue.wait_for_tasks_done().await.unwrap();
        }
        assert_eq!(queue.wait_for_task_done(&"s3".to_string()).await.unwrap(), Ok(30));
    }

    #[tokio::test]
    async fn wait_for_results_excludes_internal_push_done_marker_id() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(2);
        queue.push(&"u1".to_string(), || async { Ok(1) }).await.unwrap();
        queue.push(&"u2".to_string(), || async { Ok(2) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        let map = queue.wait_for_results().await.unwrap();
        assert_eq!(map.len(), 2);
        assert!(!map.contains_key(MARKER_TASK_ID_PUSH_DONE));
    }

    #[tokio::test]
    async fn concurrent_wait_for_tasks_done_join_succeed() {
        let _s = crate::test_serial();
        let queue = Arc::new(Queue::<i32, Error, ()>::new(2));
        queue.push(&"j".to_string(), || async { Ok(1) }).await.unwrap();
        queue.set_push_done().await.unwrap();
        let a = Arc::clone(&queue);
        let b = Arc::clone(&queue);
        let (ra, rb) = tokio::join!(async move { a.wait_for_tasks_done().await }, async move { b.wait_for_tasks_done().await },);
        ra.unwrap();
        rb.unwrap();
    }

    #[tokio::test]
    async fn tick_without_tasks_returns_ok() {
        let _s = crate::test_serial();
        let queue = Queue::<i32, Error, ()>::new(1);
        queue.tick().await.unwrap();
    }

    #[tokio::test]
    async fn queue_accepts_alternate_ok_result_type_string() {
        let _s = crate::test_serial();
        let queue: Queue<String, Error, ()> = Queue::new(1);
        queue
            .push(&"k".to_string(), || async { Ok("payload".to_string()) })
            .await
            .unwrap();
        queue.set_push_done().await.unwrap();
        assert_eq!(
            queue.wait_for_task_done(&"k".to_string()).await.unwrap(),
            Ok("payload".to_string())
        );
    }

    #[tokio::test]
    async fn push_deduping_non_unit_deduped_output_still_completes() {
        let _s = crate::test_serial();
        let queue: Queue<i32, Error, u16> = Queue::new(1);
        queue
            .push_deduping(&"a".to_string(), || async { Ok(1) }, || async { 1u16 })
            .await
            .unwrap();
        queue
            .push_deduping(&"a".to_string(), || async { Ok(1) }, || async { 2u16 })
            .await
            .unwrap();
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        assert_eq!(queue.wait_for_task_done(&"a".to_string()).await.unwrap(), Ok(1));
    }

    #[tokio::test]
    async fn fifo_completion_order_under_serial_queue_matches_push_order() {
        let _s = crate::test_serial();
        let queue: Queue<u32, Error, ()> = Queue::new(1);
        let order = Arc::new(Mutex::new(Vec::<u32>::new()));
        for i in 0u32..5 {
            let ord = Arc::clone(&order);
            queue
                .push(&format!("t{i}"), move || async move {
                    ord.lock().await.push(i);
                    Ok(i)
                })
                .await
                .unwrap();
        }
        queue.set_push_done().await.unwrap();
        queue.wait_for_tasks_done().await.unwrap();
        assert_eq!(*order.lock().await, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn failure_injection_helpers_cover_real_watch_errors() {
        let _s = crate::test_serial();
        let (q_tx, q_rx) = tokio::sync::watch::channel(QueueState::Idle);
        drop(q_rx);
        assert!(crate::failure_injection::queue_state_running_send_failed(q_tx.send(QueueState::Running)));
        let (q_tx2, q_rx2) = tokio::sync::watch::channel(QueueState::Idle);
        drop(q_rx2);
        assert!(crate::failure_injection::reset_queue_state_send_failed(q_tx2.send(QueueState::Idle)));

        let (t_tx, t_rx) = tokio::sync::watch::channel((String::new(), TaskState::Pending));
        drop(t_rx);
        assert!(crate::failure_injection::tasks_add_send_failed(t_tx.send(("p".into(), TaskState::Add))));
        let (t_tx, t_rx) = tokio::sync::watch::channel((String::new(), TaskState::Pending));
        drop(t_rx);
        assert!(crate::failure_injection::worker_running_send_failed(t_tx.send(("p".into(), TaskState::Running))));
        let (t_tx, t_rx) = tokio::sync::watch::channel((String::new(), TaskState::Pending));
        drop(t_rx);
        assert!(crate::failure_injection::worker_final_send_failed(t_tx.send(("p".into(), TaskState::Succeed))));
        let (t_tx, t_rx) = tokio::sync::watch::channel((String::new(), TaskState::Pending));
        drop(t_rx);
        assert!(crate::failure_injection::set_push_done_send_failed(
            t_tx.send((MARKER_TASK_ID_PUSH_DONE.to_string(), TaskState::AllPushed)),
        ));

        let (tx, mut rx) = tokio::sync::watch::channel(QueueState::Idle);
        drop(tx);
        let ch = rx.changed().await;
        assert!(crate::failure_injection::tasks_changed_failed(ch));
        let (tx, mut rx) = tokio::sync::watch::channel(QueueState::Idle);
        drop(tx);
        let ch = rx.changed().await;
        assert!(crate::failure_injection::wait_task_changed_failed(ch));
    }
}
