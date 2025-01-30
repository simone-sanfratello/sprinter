use sprinter::Queue;
use std::fmt::Error;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("sprint start ...");
    let start = tokio::time::Instant::now();
    // Create a queue with concurrency of 2 and tick interval of 50ms
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

    // Signal that all tasks have been pushed
    queue.set_push_done().await;

    // Wait for all tasks to complete and get results
    queue.wait_for_tasks_done().await.unwrap();

    let end = tokio::time::Instant::now();
    println!("sprint done in {:?}", end - start);
}
