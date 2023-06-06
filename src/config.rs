use crate::Error;
use std::str::FromStr;
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use std::cell::Cell;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use cron::Schedule;

#[derive(Clone)]
/// Configuration for a job
pub struct JobConfig {
    /// An arbitrary identifier used for logging
    pub name: String,
    /// The Quartz cron expression to use, for defining job run times (e.g.: `"* 0 0 ? * * *"`)
    pub(crate) schedule: Option<Schedule>,
    pub(crate) cron_str: Option<String>,
    pub(crate) interval: Option<chrono::Duration>,
    pub(crate) oneshot: bool,
    pub(crate) last_tick: Arc<Mutex<Cell<Option<DateTime<Utc>>>>>,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) have_run: Arc<AtomicBool>,
}

/// Configuration for a single widdle job
///
/// Initialized with the name and sync_key of the job.
///
/// The name is an arbitrary identifier for the job.
///
/// For a job to be valid, either the `interval`, or the `cron` configuration need to be set.
///
/// If neither are set, the job_runner will exit with an Error.
impl JobConfig {
    /// Create a new job with an arbitrary name and a unique sync key
    ///
    /// After creating a job, you need to set either `interval`, or `cron` for the job to be valid
    pub fn new_schedule(name: &str, cron_str: &str) -> Self {
        Self {
            name: name.to_owned(),
            schedule:  Some(Schedule::from_str(cron_str).expect("invalid cron expression")),
            cron_str: Some(cron_str.to_owned()),
            interval: None,
            oneshot: false,
            last_tick: Arc::new(Mutex::new(Cell::new(None))),
            running: Arc::new(AtomicBool::new(false)),
            have_run: Arc::new(AtomicBool::new(false))
        }
    }

    pub fn new_interval(name: &str, duration: &chrono::Duration) -> Self {
        Self {
            name: name.to_owned(),
            schedule:  None,
            cron_str: None,
            interval: Some(duration.clone()),
            oneshot: false,
            last_tick: Arc::new(Mutex::new(Cell::new(None))),
            running: Arc::new(AtomicBool::new(false)),
            have_run: Arc::new(AtomicBool::new(false))
        }
    }

    pub fn is_cron_powered(&self) -> bool {
        self.schedule.is_some()
    }

    pub fn set_oneshot(&mut self) {
        self.oneshot = true;
    }

    pub fn set_have_run(&self, have_run: bool) {
        self.have_run.store(have_run, Ordering::Relaxed);
    }
    
    pub fn has_run(&self) -> bool {
        self.have_run.load(Ordering::Relaxed)
    }

    pub fn set_running(&self, running: bool) {
        self.running.store(running, Ordering::Relaxed);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn update_last_tick(&self) {

        let v = self.last_tick.lock().unwrap();
        v.set(Some(Utc::now()));
    }

    pub fn get_last_tick(&self) -> Option<DateTime<Utc>> {

        let v = self.last_tick.lock().unwrap();
        v.get().clone()
    }

    pub fn get_next_event(&self) -> Option<DateTime<Utc>> {

        assert!(self.schedule.is_some(), "No scheduler defined! Is a duration / interval being used ?");

        // let events = self.schedule.upcoming(Utc);
        self.schedule.as_ref().unwrap().upcoming(Utc).next()
    }

    pub fn job_should_run(&self, check_interval: chrono::Duration) -> bool {

        let now = Utc::now();

        let is_running : bool = self.is_running();

        let last_tick_option = self.get_last_tick();

        if last_tick_option.is_none() || is_running {
            self.update_last_tick();
            return false;
        }

        let last_tick: DateTime<Utc> = last_tick_option.unwrap();

        if self.is_cron_powered() {
            // Calculate the next time this event will take place.
            let event: Option<DateTime<Utc>> = self.get_next_event();

            if event.is_some() {

                let t = event.unwrap() - check_interval;

                // println!("event: {:?}  last_tick: {:?}", t, self.get_last_tick().unwrap());

                if now >= t {
                    self.update_last_tick();
                    // println!("should be running scheduled task for time {:?}", t);
                    return true;
                } else {
                    return false;
                }
            }
        }
        else {
            // We have intervals not cron
            let duration: chrono::Duration = self.interval.unwrap();
            // Calculate the difference between now and the last tick
            let time_elapsed = now.signed_duration_since(last_tick);

            return time_elapsed >= duration;
        }
        
        // self.update_last_tick();

        false
    }
}

/// Configuration for the widdle job runner.
///
/// Holds the database configuration and the interval at which to check
/// for new job runs.
///
/// Database configuration defaults to `host=localhost user=postgres port=5432`
///
/// Check Interval defaults to 60 seconds. This means, that every 60 seconds, the system checks, if
/// a job should be run.
///
/// Initial delay defaults to 0 seconds. This means, that the runner starts immediately
#[derive(Clone)]
pub struct RunnerConfig {
    /// Interval for checking and running jobs
    pub(crate) check_interval: Duration,
    /// Amounts of time to wait, before starting to check and run jobs after startup
    pub(crate) initial_delay: Option<Duration>,
}

impl RunnerConfig {
    /// Creates a new RunnerConfig with the given connection String
    ///
    /// Example: `host=localhost user=postgres port=5432 password=postgres`
    ///
    /// Anything, which can be used to create a [Config](https://docs.rs/tokio-postgres/0.5/tokio_postgres/config/struct.Config.html) is valid.
    pub fn new(_db_config: &str) -> Result<Self, Error> {
        let res = Self {
            ..Default::default()
        };
        Ok(res)
    }

    /// Sets the interval to check for job runs to the given Duration
    pub fn check_interval(mut self, check_interval: Duration) -> Self {
        self.check_interval = check_interval;
        self
    }

    /// Sets the initial delay, before checking and running jobs to the given Duration
    pub fn initial_delay(mut self, initial_delay: Duration) -> Self {
        self.initial_delay = Some(initial_delay);
        self
    }
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(60),
            initial_delay: None,
        }
    }
}
