use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

use crate::db::DbManager;

pub fn start_downsampler(db: DbManager) {
    tokio::spawn(async move {
        // Runs every 5 minutes
        let mut ticker = interval(Duration::from_secs(300));

        loop {
            ticker.tick().await;
            info!("[Rollup] Running time-series downsampling (1m -> 1h -> 1d) & pruning...");

            let res = db.with_conn(|conn| {
                crate::db::queries::run_downsampling_rollups(conn)
            });

            match res {
                Ok(_) => info!("[Rollup] Downsampling and cascade pruning completed successfully."),
                Err(e) => error!("[Rollup] Downsampling failed: {}", e),
            }
        }
    });
}
