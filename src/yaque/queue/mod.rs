// Copied from yaque 0.6.6, src/queue/mod.rs: https://github.com/tokahuke/yaque
// Copyright 2020 Pedro Bittencourt Arruda. Apache License 2.0, see /NOTICE.
// Modified for scalo.

//! Queue implementation and utility functions.

mod iter;
mod receiver;
mod sender;

pub use iter::QueueIter;
pub use receiver::{Receiver, ReceiverBuilder, RecvGuard};
pub use sender::{Sender, SenderBuilder};

pub(crate) use receiver::recv_lock_filename;
pub(crate) use sender::send_lock_filename;

use std::fs::*;
use std::io::{self};
use std::path::{Path, PathBuf};

use receiver::{acquire_recv_lock, try_acquire_recv_lock};
use sender::{acquire_send_lock, try_acquire_send_lock};

/// The name of segment file in the queue folder.
fn segment_filename<P: AsRef<Path>>(base: P, segment: u64) -> PathBuf {
    base.as_ref().join(format!("{}.q", segment))
}

/// The value of a header EOF.
const HEADER_EOF: [u8; 4] = [255, 255, 255, 255];

/// Convenience function for opening the queue for both sending and receiving.
pub fn channel<P: AsRef<Path>>(base: P) -> io::Result<(Sender, Receiver)> {
    Ok((Sender::open(base.as_ref())?, Receiver::open(base.as_ref())?))
}

/// Tries to deletes a queue at the given path. This function will fail if the
/// queue is in use either for sending or receiving.
pub fn try_clear<P: AsRef<Path>>(base: P) -> io::Result<()> {
    let mut send_lock = try_acquire_send_lock(base.as_ref())?;
    let mut recv_lock = try_acquire_recv_lock(base.as_ref())?;

    // Sets the the locks to ignore when their files magically disappear.
    send_lock.ignore();
    recv_lock.ignore();

    remove_dir_all(base.as_ref())?;

    Ok(())
}

/// Deletes a queue at the given path. This function will await the queue to
/// become available for both sending and receiving.
pub async fn clear<P: AsRef<Path>>(base: P) -> io::Result<()> {
    let mut send_lock = acquire_send_lock(base.as_ref()).await?;
    let mut recv_lock = acquire_recv_lock(base.as_ref()).await?;

    // Sets the the locks to ignore when their files magically disappear.
    send_lock.ignore();
    recv_lock.ignore();

    remove_dir_all(base.as_ref())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::yaque::error::{TryRecvError, TrySendError};
    use crate::yaque::{block_on, test_queue_dir, test_rng};

    use self::sender::get_queue_size;

    fn data_lots_of_data() -> impl Iterator<Item = Vec<u8>> {
        let mut rng = test_rng();
        (0..).map(move |_| {
            (0..rng() as usize % 128)
                .map(|_| rng() as u8)
                .collect::<Vec<_>>()
        })
    }

    fn delay(duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }

    #[test]
    fn create_and_clear() {
        let (_dir, path) = test_queue_dir();
        let _ = Sender::open(&path).unwrap();
        try_clear(&path).unwrap();
    }

    #[test]
    #[should_panic]
    fn create_and_clear_fails() {
        let (_dir, path) = test_queue_dir();
        let sender = Sender::open(&path).unwrap();
        try_clear(&path).unwrap();
        drop(sender);
    }

    #[test]
    fn create_and_clear_async() {
        let (_dir, path) = test_queue_dir();
        let _ = Sender::open(&path).unwrap();

        block_on(async { clear(&path).await.unwrap() });
    }

    #[test]
    fn test_enqueue() {
        let (_dir, path) = test_queue_dir();
        let mut sender = Sender::open(&path).unwrap();
        for data in data_lots_of_data().take(100_000) {
            sender.try_send(&data).unwrap();
        }
    }

    /// Test enqueuing everything and then dequeueing everything, with no persistence.
    #[test]
    fn test_enqueue_then_dequeue() {
        let (_dir, path) = test_queue_dir();

        // Enqueue:
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
        let mut sender = Sender::open(&path).unwrap();
        for data in &dataset {
            sender.try_send(data).unwrap();
        }

        log::trace!("enqueued");

        // Dequeue:
        block_on(async {
            let mut receiver = Receiver::open(&path).unwrap();
            let dataset_iter = dataset.iter();
            let mut i = 0u64;

            for should_be in dataset_iter {
                let data = receiver.recv().await.unwrap();
                assert_eq!(&*data, should_be, "at sample {}", i);
                i += 1;
                data.commit().unwrap();
            }
        });
    }

    /// Test enqueuing and dequeueing, round robin, with no persistence.
    #[test]
    fn test_enqueue_and_dequeue() {
        let (_dir, path) = test_queue_dir();

        // Enqueue:
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
        let mut sender = Sender::open(&path).unwrap();

        block_on(async {
            let mut receiver = Receiver::open(&path).unwrap();
            let mut i = 0;

            for data in &dataset {
                sender.try_send(data).unwrap();
                let received = receiver.recv().await.unwrap();
                assert_eq!(&*received, data, "at sample {}", i);

                i += 1;
                received.commit().unwrap();
            }
        });
    }

    /// Test enqueuing and dequeueing in parallel.
    #[test]
    fn test_enqueue_dequeue_parallel() {
        let (_dir, path) = test_queue_dir();
        let send_path = path.clone();

        // Generate data:
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
        let arc_sender = Arc::new(dataset);
        let arc_receiver = arc_sender.clone();

        // Enqueue (let's test async send!):
        let enqueue = std::thread::spawn(move || {
            block_on(async {
                let mut sender = Sender::open(&send_path).unwrap();
                for data in &*arc_sender {
                    sender.send(data).await.unwrap();
                }
            });
        });

        // Dequeue:
        let dequeue = std::thread::spawn(move || {
            block_on(async {
                let mut receiver = Receiver::open(&path).unwrap();
                let dataset_iter = arc_receiver.iter();
                let mut i = 0u64;

                for should_be in dataset_iter {
                    let data = receiver.recv().await.unwrap();
                    assert_eq!(&*data, should_be, "at sample {}", i);
                    i += 1;
                    data.commit().unwrap();
                }
            });
        });

        enqueue.join().expect("enqueue thread panicked");
        dequeue.join().expect("dequeue thread panicked");
    }

    /// Test enqueuing and dequeueing in parallel, using batches.
    #[test]
    fn test_enqueue_dequeue_parallel_with_batches() {
        let (_dir, path) = test_queue_dir();
        let send_path = path.clone();

        // Generate data:
        let mut dataset = vec![];
        let mut batch = vec![];

        for data in data_lots_of_data().take(100_000) {
            batch.push(data);

            if batch.len() >= 256 {
                dataset.push(batch);
                batch = vec![];
            }
        }

        let arc_sender = Arc::new(dataset);
        let arc_receiver = arc_sender.clone();

        // Enqueue:
        let enqueue = std::thread::spawn(move || {
            let mut sender = Sender::open(&send_path).unwrap();
            for batch in &*arc_sender {
                sender.try_send_batch(batch).unwrap();
            }
        });

        // Dequeue:
        let dequeue = std::thread::spawn(move || {
            block_on(async {
                let mut receiver = Receiver::open(&path).unwrap();
                let dataset_iter = arc_receiver.iter();
                let mut i = 0u64;

                for should_be in dataset_iter {
                    let batch = receiver.recv_batch(256).await.unwrap();
                    assert_eq!(&*batch, should_be, "at sample {}", i);
                    i += 1;
                    batch.commit().unwrap();
                }
            });
        });

        enqueue.join().expect("enqueue thread panicked");
        dequeue.join().expect("dequeue thread panicked");
    }

    #[test]
    fn test_dequeue_is_atomic() {
        let (_dir, path) = test_queue_dir();
        let mut sender = Sender::open(&path).unwrap();
        let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();

        block_on(async move {
            let mut receiver = Receiver::open(&path).unwrap();
            let dataset_iter = dataset.iter();
            let mut i = 0u64;

            for data in dataset_iter {
                sender.try_send(data).unwrap();
                // Try not to poll the future to the end.
                let incomplete =
                    futures::future::select(Box::pin(receiver.recv()), Box::pin(async {})).await;
                drop(incomplete); // need to force this explicitly.

                //
                let received = receiver.recv().await.unwrap();
                assert_eq!(&*received, data, "at sample {}", i);
                i += 1;
                received.commit().unwrap();
            }
        });
    }

    #[test]
    fn test_rollback() {
        let (_dir, path) = test_queue_dir();
        block_on(async move {
            let (mut sender, mut receiver) = channel(&path).unwrap();
            sender.try_send(b"123").unwrap();
            sender.try_send(b"456").unwrap();

            assert_eq!(&*receiver.recv().await.unwrap(), b"123");
            assert_eq!(&*receiver.recv().await.unwrap(), b"123");

            receiver.recv().await.unwrap().commit().unwrap();

            assert_eq!(&*receiver.recv().await.unwrap(), b"456");
            assert_eq!(&*receiver.recv().await.unwrap(), b"456");
        });
    }

    #[test]
    fn test_recv_timeout_nothing() {
        let (_dir, path) = test_queue_dir();
        block_on(async move {
            let (_, mut receiver) = channel(&path).unwrap();

            assert!(
                receiver
                    .recv_timeout(delay(Duration::from_secs(1)))
                    .await
                    .unwrap()
                    .is_none(),
            );
        });
    }

    #[test]
    fn test_recv_timeout_immediate() {
        let (_dir, path) = test_queue_dir();
        block_on(async move {
            let (mut sender, mut receiver) = channel(&path).unwrap();

            sender.try_send(b"123").unwrap();

            assert_eq!(
                &*receiver
                    .recv_timeout(delay(Duration::from_secs(1)))
                    .await
                    .unwrap()
                    .unwrap(),
                b"123"
            );
        });
    }

    #[test]
    fn test_recv_timeout_delayed() {
        let (_dir, path) = test_queue_dir();
        block_on(async move {
            let (mut sender, mut receiver) = channel(&path).unwrap();

            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(1));
                sender.try_send(b"123").unwrap();
            });

            assert_eq!(
                &*receiver
                    .recv_timeout(delay(Duration::from_secs(2)))
                    .await
                    .unwrap()
                    .unwrap(),
                b"123"
            );
        });
    }

    #[test]
    fn test_recv_batch_timeout_nothing() {
        let (_dir, path) = test_queue_dir();
        block_on(async move {
            let (_, mut receiver) = channel(&path).unwrap();

            assert!(
                receiver
                    .recv_batch_timeout(2, delay(Duration::from_secs(1)))
                    .await
                    .unwrap()
                    .is_empty(),
            );
        });
    }

    #[test]
    fn test_recv_batch_timeout_immediate() {
        let (_dir, path) = test_queue_dir();
        block_on(async move {
            let (mut sender, mut receiver) = channel(&path).unwrap();

            sender.try_send(b"123").unwrap();
            sender.try_send(b"456").unwrap();

            assert_eq!(
                &*receiver
                    .recv_batch_timeout(2, delay(Duration::from_secs(1)))
                    .await
                    .unwrap(),
                &[b"123", b"456"],
            );
        });
    }

    #[test]
    fn test_recv_batch_timeout_delayed_1() {
        let (_dir, path) = test_queue_dir();
        block_on(async move {
            let (mut sender, mut receiver) = channel(&path).unwrap();

            std::thread::spawn(move || {
                for i in 0..5usize {
                    std::thread::sleep(Duration::from_secs_f64(0.5));
                    sender.try_send(i.to_string().as_bytes()).unwrap();
                }
            });

            assert_eq!(
                &*receiver
                    .recv_batch_timeout(3, delay(Duration::from_secs(2)))
                    .await
                    .unwrap(),
                &[b"0", b"1", b"2"]
            );
        });
    }

    #[test]
    fn test_recv_batch_timeout_delayed_2() {
        let (_dir, path) = test_queue_dir();
        block_on(async move {
            let (mut sender, mut receiver) = channel(&path).unwrap();

            std::thread::spawn(move || {
                for i in 0..5usize {
                    std::thread::sleep(Duration::from_secs_f64(0.6));
                    sender.try_send(i.to_string().as_bytes()).unwrap();
                }
            });

            assert_eq!(
                &*receiver
                    .recv_batch_timeout(5, delay(Duration::from_secs(2)))
                    .await
                    .unwrap(),
                &[b"0", b"1", b"2"]
            );
        });
    }

    #[test]
    fn test_max_queue_size() {
        let (_dir, path) = test_queue_dir();
        let mut sender = SenderBuilder::new()
            .max_queue_size(Some(2048))
            .segment_size(512)
            .open(&path)
            .unwrap();
        let mut data = data_lots_of_data();

        loop {
            let item = data.next().unwrap();
            match sender.try_send(&item) {
                Ok(_) => {}
                Err(TrySendError::Io(err)) => Err(err).unwrap(),
                Err(TrySendError::QueueFull { .. }) => break,
            }
        }

        let size = get_queue_size(&path).unwrap().in_bytes;
        assert!(
            size >= 2048,
            "size was {}; should be at least {}",
            size,
            2048
        );
    }

    #[test]
    fn test_max_queue_size_with_drain() {
        let (_dir, path) = test_queue_dir();
        let mut sender = SenderBuilder::new()
            .max_queue_size(Some(2048))
            .segment_size(512)
            .open(&path)
            .unwrap();
        let mut receiver = Receiver::open(&path).unwrap();
        let mut data = data_lots_of_data();

        loop {
            let item = data.next().unwrap();
            match sender.try_send(&item) {
                Ok(_) => {}
                Err(TrySendError::Io(err)) => Err(err).unwrap(),
                Err(TrySendError::QueueFull { .. }) => break,
            }
        }

        let size = get_queue_size(&path).unwrap().in_bytes;
        assert!(
            size >= 2048,
            "size was {}; should be at least {}",
            size,
            2048
        );

        // Drain queue:
        loop {
            match receiver.try_recv() {
                Ok(thing) => thing.commit().unwrap(),
                Err(TryRecvError::QueueEmpty) => break,
                Err(TryRecvError::Io(err)) => Err(err).unwrap(),
            }
        }

        for _ in 0..8 {
            sender.try_send(data.next().unwrap()).unwrap();
        }
    }

    /// Test enqueuing and dequeueing in parallel.
    #[test]
    fn test_enqueue_dequeue_parallel_with_max_queue_size() {
        fn test(queue_size: u64) {
            let (_dir, path) = test_queue_dir();
            let send_path = path.clone();
            let clear_path = path.clone();

            // Generate data:
            let dataset = data_lots_of_data().take(100_000).collect::<Vec<_>>();
            let arc_sender = Arc::new(dataset);
            let arc_receiver = arc_sender.clone();

            // Enqueue (let's test async send!):
            let enqueue = std::thread::spawn(move || {
                block_on(async {
                    let mut sender = SenderBuilder::new()
                        .max_queue_size(Some(queue_size))
                        .open(&send_path)
                        .unwrap();
                    for data in &*arc_sender {
                        sender.send(data).await.unwrap();
                    }
                });
            });

            // Dequeue:
            let dequeue = std::thread::spawn(move || {
                block_on(async {
                    let mut receiver = Receiver::open(&path).unwrap();
                    let dataset_iter = arc_receiver.iter();
                    let mut i = 0u64;

                    for should_be in dataset_iter {
                        let data = receiver.recv().await.unwrap();
                        assert_eq!(&*data, should_be, "at sample {}", i);
                        i += 1;
                        data.commit().unwrap();
                    }
                });
            });

            enqueue.join().expect("enqueue thread panicked");
            dequeue.join().expect("dequeue thread panicked");

            try_clear(&clear_path).unwrap();
        }

        test(2 * 1024 * 1024); // smaller than segment
        test(4 * 1024 * 1024); // equal to segment
        test(8 * 1024 * 1024); // bigger than segment
    }

    #[test]
    #[should_panic]
    fn test_small_queue_size() {
        SenderBuilder::new().max_queue_size(Some(0));
    }

    #[test]
    #[should_panic]
    fn test_small_segment_size() {
        SenderBuilder::new().segment_size(0);
    }

    // test small segment size + big batch transaction: commit and rollback.
    #[test]
    fn test_trans_segment_transactions() {
        let (_dir, path) = test_queue_dir();
        let data = data_lots_of_data().take(100).collect::<Vec<_>>();

        // Populate a queue:
        let mut sender = SenderBuilder::new().segment_size(512).open(&path).unwrap();

        sender.try_send_batch(&data).unwrap();

        block_on(async move {
            let mut receiver = Receiver::open(&path).unwrap();

            // Do some rollbacks:
            for _ in 0..7 {
                let batch = receiver.recv_batch(50).await.unwrap();

                for (batch_item, item) in batch.iter().zip(&data) {
                    assert_eq!(batch_item, item);
                }

                batch.rollback().unwrap();
            }

            // Now commit:
            let batch = receiver.recv_batch(50).await.unwrap();

            for (batch_item, item) in batch.iter().zip(&data) {
                assert_eq!(batch_item, item);
            }

            batch.commit().unwrap();

            // And now do some more rollbacks:
            for _ in 0..7 {
                let batch = receiver.recv_batch(50).await.unwrap();

                for (batch_item, item) in batch.iter().zip(&data[50..]) {
                    assert_eq!(batch_item, item);
                }

                batch.rollback().unwrap();
            }
        });
    }

    // test simple try_recv uses.
    #[test]
    fn test_try_recv() {
        let (_dir, path) = test_queue_dir();
        let data = data_lots_of_data().take(100).collect::<Vec<_>>();

        // Populate a queue:
        let mut sender = SenderBuilder::new().segment_size(512).open(&path).unwrap();

        sender.try_send_batch(&data[..25]).unwrap();

        let mut receiver = Receiver::open(&path).unwrap();

        let mut count = 0;
        loop {
            match receiver.try_recv() {
                Ok(item) => {
                    assert_eq!(&*item, &data[count]);
                    item.commit().unwrap();
                    count += 1;
                }
                Err(TryRecvError::Io(err)) => Err(err).unwrap(),
                Err(TryRecvError::QueueEmpty) => break,
            }
        }

        assert_eq!(count, 25);
    }

    // test simple try_recv_batch uses.
    #[test]
    fn test_try_recv_batch() {
        let (_dir, path) = test_queue_dir();
        let data = data_lots_of_data().take(100).collect::<Vec<_>>();

        // Populate a queue:
        let mut sender = SenderBuilder::new().segment_size(512).open(&path).unwrap();

        let mut receiver = Receiver::open(&path).unwrap();

        // only works for multiples of 25, up to 25
        for recv_size in [1, 5, 25] {
            sender.try_send_batch(&data[..25]).unwrap();

            let mut count = 0;
            loop {
                match receiver.try_recv_batch(recv_size) {
                    Ok(items) => {
                        for item in items.iter() {
                            assert_eq!(&*item, &data[count]);
                            count += 1;
                        }
                        items.commit().unwrap();
                    }
                    Err(TryRecvError::Io(err)) => Err(err).unwrap(),
                    Err(TryRecvError::QueueEmpty) => break,
                }
            }

            assert_eq!(count, 25);
        }
    }

    // test simple try_recv_batch_up_to uses.
    #[test]
    fn test_try_recv_batch_up_to() {
        let (_dir, path) = test_queue_dir();
        let data = data_lots_of_data().take(100).collect::<Vec<_>>();

        // Populate a queue:
        let mut sender = SenderBuilder::new().segment_size(512).open(&path).unwrap();

        let mut receiver = Receiver::open(&path).unwrap();

        for send_size in [1, 10, 25, 100] {
            for recv_size in [1, 7, 25, 30, 100] {
                sender.try_send_batch(&data[..send_size]).unwrap();

                let mut count = 0;
                loop {
                    match receiver.try_recv_batch_up_to(recv_size) {
                        Ok(items) => {
                            for item in items.iter() {
                                assert_eq!(&*item, &data[count]);
                                count += 1;
                            }
                            items.commit().unwrap();
                        }
                        Err(TryRecvError::Io(err)) => Err(err).unwrap(),
                        Err(TryRecvError::QueueEmpty) => break,
                    }
                }

                assert_eq!(
                    count, send_size,
                    "send_size: {send_size}, recv_size: {recv_size}"
                );
            }
        }
    }

    #[test]
    fn test_try_recv_batch_up_to_zero() {
        let (_dir, path) = test_queue_dir();

        // Populate a queue:
        let mut sender = SenderBuilder::new().segment_size(512).open(&path).unwrap();

        let mut receiver = Receiver::open(&path).unwrap();
        sender.try_send_batch([[1]]).unwrap();

        let no_items = receiver
            .try_recv_batch_up_to(0)
            .unwrap()
            .try_into_inner()
            .unwrap();
        assert!(no_items.is_empty());
    }

    #[test]
    fn test_iterate() {
        let (_dir, path) = test_queue_dir();
        let data = data_lots_of_data().take(10_000).collect::<Vec<_>>();

        // Populate a queue:
        let mut sender = SenderBuilder::new().segment_size(512).open(&path).unwrap();

        sender.try_send_batch(&data).unwrap();

        let iterated = QueueIter::open(&path)
            .unwrap()
            .map(|item| item.unwrap())
            .collect::<Vec<_>>();

        assert_eq!(data, iterated);
    }

    #[test]
    fn test_try_recv_empty_msg() {
        let (_dir, path) = test_queue_dir();

        // Populate a queue:
        let mut sender = SenderBuilder::new().segment_size(512).open(&path).unwrap();

        sender.try_send(&[]).unwrap();

        let mut receiver = Receiver::open(&path).unwrap();

        let item = receiver.try_recv().unwrap();
        assert!(item.is_empty());
        item.commit().unwrap();
    }
}
