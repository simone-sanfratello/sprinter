    // #[tokio::test]
    // async fn queue_task_done_should_wait_for_completion() {
    //     let mut queue: Queue<i32, Error> = Queue::new(&"test_task_done".to_string(), &1);
    //     let task = || async {
    //         sleep(Duration::from_millis(250));
    //         Ok(42)
    //     };

    //     let _ = queue.push(&"t1".to_string(), task).await.unwrap();
    //     let result = queue.task_done(&"t1".to_string()).await.unwrap();
    //     assert_eq!(result, Ok(42));
    // }

    // #[tokio::test]
    // async fn queue_task_done_should_handle_nonexistent_task() {
    //     let queue: Queue<i32, Error> = Queue::new(&"test_task_done_nonexistent".to_string(), &1);
    //     let result = queue.task_done(&"nonexistent".to_string()).await;
    //     assert!(result.is_err());
    //     assert!(matches!(result.unwrap_err(), QueueError::TaskNotFound(_)));
    // }