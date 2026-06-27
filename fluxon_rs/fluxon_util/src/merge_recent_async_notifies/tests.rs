use super::*;
use crate::init_log_test;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial_test::serial(log_init)]
async fn test_async_notification_merger_poll() {
    // Initialize test logs under the shared test workdir.
    init_log_test("merge_recent_async_notifies_poll");

    let (tx, rx) = mpsc::unbounded_channel::<i32>();
    let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

    let mut merger = AsyncNotificationMerger::new(
        stream,
        Duration::from_millis(10),
        Some(11), // max_batch_size = 2
    );

    // let mut interval = tokio::time::interval(Duration::from_millis(11));

    // 发送第一个数据
    for i in 0..10 {
        tx.send(i).unwrap();

        if i < 5 {
            match merger.poll().await {
                PollResult::DataCollected => {
                    assert_eq!(merger.pending_count(), (i + 1) as usize);
                }
                PollResult::BatchReady(_batch) => {
                    panic!("Expected DataCollected at {}, but got BatchReady", i);
                }
                PollResult::StreamEnded(_remaining) => {
                    panic!("Expected DataCollected at {}, but got StreamEnded", i);
                }
                PollResult::Pending => {
                    panic!("Expected DataCollected at {}, but got Pending", i);
                }
            }
        }
    }

    for i in 5..10 {
        match merger.poll().await {
            PollResult::DataCollected => {
                assert_eq!(merger.pending_count(), (i + 1) as usize);
            }
            _ => panic!("Expected DataCollected at {}", i),
        }
    }

    tokio::select! {
        _ = merger.poll() => {
            panic!("poll should be blocked")
        }
        _ = tokio::time::sleep(Duration::from_millis(1)) => {
            info!("poll blocked because of empty stream")
        }
    }

    // 等待计时器触发
    // interval.tick().await;
    tokio::time::sleep(Duration::from_millis(11)).await;

    tokio::select! {
        _ = merger.poll() => {
            info!("poll success")
        }
        _ = tokio::time::sleep(Duration::from_millis(1)) => {
            panic!("poll should return because timeout for 11s")
        }
    }

    for i in 0..10 {
        tx.send(i).unwrap();
        match merger.poll().await {
            PollResult::DataCollected => {
                assert_eq!(merger.pending_count(), (i + 1) as usize);
            }
            _ => panic!("Expected DataCollected at {}", i),
        }
    }
    tx.send(10).unwrap();
    match merger.poll().await {
        PollResult::BatchReady(batch) => {
            assert_eq!(batch, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        }
        _ => panic!("Expected BatchReady"),
    }

    // 关闭发送端
    drop(tx);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial_test::serial(log_init)]
async fn test_user_controlled_loop() {
    // Initialize test logs under the shared test workdir.
    init_log_test("merge_recent_async_notifies_user_loop");
    let (tx, rx) = mpsc::unbounded_channel::<i32>();
    let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);

    let mut merger = AsyncNotificationMerger::new(stream, Duration::from_millis(30), Some(3));

    let mut batches = Vec::new();

    // 模拟用户控制的循环
    let handle = tokio::spawn(async move {
        loop {
            match merger.poll().await {
                PollResult::BatchReady(batch) => {
                    batches.push(batch);
                }
                PollResult::StreamEnded(remaining) => {
                    if !remaining.is_empty() {
                        batches.push(remaining);
                    }
                    break;
                }
                PollResult::DataCollected | PollResult::Pending => {
                    // 继续循环
                }
            }
        }
        batches
    });

    // 发送测试数据
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap(); // 应该触发第一个批处理

    tokio::time::sleep(Duration::from_millis(10)).await;

    tx.send(4).unwrap();
    tx.send(5).unwrap();

    tokio::time::sleep(Duration::from_millis(40)).await; // 等待计时器触发

    drop(tx);

    let result = handle.await.unwrap();
    assert!(!result.is_empty());
    assert_eq!(result[0], vec![1, 2, 3]); // 第一个批次由大小触发
}
