use sprinter::Queue;
use std::fmt::Error;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("sprint start ...");
    let start = tokio::time::Instant::now();
    // Create a queue with concurrency of 2 and tick interval of 50ms
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
    let results = queue.wait_for_tasks_done().await;

    let end = tokio::time::Instant::now();
    println!("sprint done in {:?}", end - start);

    println!("results: {:?}", results);
}
