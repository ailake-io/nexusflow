use nexus_core::{ProgressEvent, ProgressSender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Maps a running pipeline execution (`run_id`, from `PipelineStore::start_run`)
/// to the broadcast channel its progress events go out on — every WebSocket
/// client subscribed to that run gets every event (ARCHITECTURE.md §8 /
/// IMPLEMENTATION_PLAN.md Marco 7 task #9). Entries are removed once the run
/// finishes: a client connecting after that gets a 404, not stale/no events —
/// a known MVP limitation (no replay of a run already in progress before
/// the client subscribed, beyond the channel's buffer).
#[derive(Clone, Default)]
pub struct ProgressHub {
    channels: Arc<Mutex<HashMap<i64, ProgressSender>>>,
}

const CHANNEL_CAPACITY: usize = 256;

impl ProgressHub {
    pub fn start(&self, run_id: i64) -> ProgressSender {
        let (tx, _rx) = broadcast::channel(CHANNEL_CAPACITY);
        self.channels.lock().unwrap().insert(run_id, tx.clone());
        tx
    }

    pub fn subscribe(&self, run_id: i64) -> Option<broadcast::Receiver<ProgressEvent>> {
        self.channels
            .lock()
            .unwrap()
            .get(&run_id)
            .map(|tx| tx.subscribe())
    }

    pub fn finish(&self, run_id: i64) {
        self.channels.lock().unwrap().remove(&run_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_before_start_returns_none() {
        let hub = ProgressHub::default();
        assert!(hub.subscribe(1).is_none());
    }

    #[tokio::test]
    async fn subscriber_receives_events_sent_after_start() {
        let hub = ProgressHub::default();
        let tx = hub.start(1);
        let mut rx = hub.subscribe(1).unwrap();

        tx.send(ProgressEvent {
            partition_id: "p0".to_string(),
            batches_written: 1,
            rows_written: 10,
            bytes_written: 100,
        })
        .unwrap();

        let event = rx.recv().await.unwrap();
        assert_eq!(event.rows_written, 10);
    }

    #[test]
    fn subscribe_after_finish_returns_none() {
        let hub = ProgressHub::default();
        hub.start(1);
        hub.finish(1);
        assert!(hub.subscribe(1).is_none());
    }

    #[tokio::test]
    async fn multiple_subscribers_each_get_every_event() {
        let hub = ProgressHub::default();
        let tx = hub.start(1);
        let mut rx_a = hub.subscribe(1).unwrap();
        let mut rx_b = hub.subscribe(1).unwrap();

        tx.send(ProgressEvent {
            partition_id: "p0".to_string(),
            batches_written: 1,
            rows_written: 5,
            bytes_written: 50,
        })
        .unwrap();

        assert_eq!(rx_a.recv().await.unwrap().rows_written, 5);
        assert_eq!(rx_b.recv().await.unwrap().rows_written, 5);
    }
}
