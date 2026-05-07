use std::sync::{mpsc, Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use log::{debug, error};
use wal::wal::{Checkpoint, WAL};
use crate::flush_worker::FlushWorker;
use crate::memtable::Memtable;

pub struct FlushService {
    sender: mpsc::Sender<FlushCommand>,
    is_flushing: Arc<AtomicBool>,
}

pub enum FlushCommand {
    Flush(Memtable, Checkpoint),
}

impl FlushService {
    pub fn new(wal: Arc<Mutex<WAL>>, flush_worker: Arc<Mutex<Box<dyn FlushWorker + Send + Sync>>>) -> Self {
        let (sender, receiver) = mpsc::channel::<FlushCommand>();

        let is_flushing = Arc::new(AtomicBool::new(false));
        let worker_flag = Arc::clone(&is_flushing);
        let worker_wal = Arc::clone(&wal);

        thread::spawn(move || {
            while let Ok(command) = receiver.recv() {
                match command {
                    FlushCommand::Flush(memtable, checkpoint) => {
                        let mut flusher = flush_worker.lock().unwrap();
                        let result = flusher.flush(memtable);

                        if let Err(error) = result {
                            error!("Error while flushing memtable: {:?}", error)
                        }

                        let checkpoint_position = checkpoint.checkpoint_id();

                        let wal_result = {
                            let mut wal = worker_wal.lock().unwrap();
                            debug!("Checkpoint position: {}", checkpoint_position);
                            wal.commit_checkpoint()
                        };

                        if let Err(error) = wal_result {
                            error!("Error while writing WAL checkpoint: {:?}", error)
                        }
                        {
                            let mut wal = worker_wal.lock().unwrap();
                            let wal_result = wal.cleanup();

                            if let Err(error) = wal_result {
                                error!("Error while clean up WAL before checkpoint {}: {:?}", checkpoint_position, error)
                            }
                        }

                        worker_flag.store(false, Ordering::SeqCst);
                    }
                }
            }
        });

        Self {
            sender,
            is_flushing
        }
    }

    pub fn request_flush(&self, memtable: Memtable, checkpoint: Checkpoint) {
        let already_flushing = self.is_flushing
            .swap(true, Ordering::SeqCst);

        if already_flushing {
            return;
        }

        let _ = self.sender.send(FlushCommand::Flush(memtable, checkpoint));
    }

    pub fn is_flushing(&self) -> bool {
        self.is_flushing.load(Ordering::SeqCst)
    }
}