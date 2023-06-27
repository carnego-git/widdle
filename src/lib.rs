//! An async, synchronized, database-backed Rust job scheduler
//!
//! This library provides an async job runner, which can run user-defined jobs in an interval, or
//! based on a cron schedule.
//!
//! Also, the library automatically synchronizes multiple instances of the runner via PostgreSQL.
//! This is important to ensure, that a job is only run once for each interval, or schedule.
//!
//! A `Job` in this library can be created by implementing the `Job` trait. There the user can
//! define a custom `run` function, which is executed for each interval, or schedule of the job.
//!
//! This interval, as well as other relevant metadata, needs to be configured using a `JobConfig`
//! for each job.
//!
//! Then, once all your jobs are defined, you can create a `JobRunner`. This is the main mechanism
//! underlying this scheduling library. It will check, at a user-defined interval, if a job needs
//! to run, or not.
//!
//! This `JobRunner` is configured using the `RunnerConfig`, where the user can define database
//! configuration, as well as an initial delay and the interval for checking for job runs.
//!
//! Once everything is configured, you can run the `JobRunner` and, if it doesn't return an error
//! during job validation, it will run forever, scheduling and running your jobs asynchronously
//! using Tokio.
#![cfg_attr(feature = "docs", feature(doc_cfg))]
mod config;

pub use async_trait::async_trait;
use chrono::DateTime;
pub use config::JobConfig;
pub use config::RunnerConfig;
use futures::future::join_all;
use log::{info};
use std::fmt;
use std::sync::Arc;
use chrono::Utc;
use cron::Schedule;
use std::str::FromStr;
use humantime::format_duration;

type BoxedJob = Box<dyn Job + Send + Sync>;

/// The error type returned by methods in this crate
#[derive(Debug)]
pub enum Error {
    InvalidJobError,
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidJobError => write!(
                f,
                "invalid job found - check if all jobs have interval or cron set"
            ),
        }
    }
}

#[async_trait]
/// A trait for implementing a widdle job
///
/// Example implementation:
///
/// ```ignore
/// use std::time::Duration;
/// use crate::{JobConfig, Job, async_trait};
///
/// #[derive(Clone)]
/// struct MyJob {
///     config: JobConfig,
/// }
///
/// #[async_trait]
/// impl Job for MyJob {
///     async fn run(&self) {
///         log::info!("running my job!");
///     }
///
///     fn get_config(&self) -> &JobConfig {
///         &self.config
///     }
/// }
///
/// fn main() {
///     let job_cfg = JobConfig::new("my_job", "someSyncKey").interval(Duration::from_secs(5));

///     let my_job = MyJob {
///         config: job_cfg,
///     };
/// }
/// ```
pub trait Job: JobClone {
    /// Runs the job
    ///
    /// This is an async function, so if you plan to do long-running, blocking operations, you
    /// should spawn them on [Tokio's Blocking Threadpool](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).
    ///
    /// You need the `blocking` feature to be active, for this to work.
    ///
    /// Otherwise, you might block the scheduler threads, slowing down your whole application.
    async fn run(&self);

    /// Exposes the configuration of the job
    fn get_config(&self) -> &JobConfig;


}

#[doc(hidden)]
pub trait JobClone {
    fn box_clone(&self) -> BoxedJob;
}

impl<T> JobClone for T
where
    T: 'static + Job + Clone + Send + Sync,
{
    fn box_clone(&self) -> BoxedJob {
        Box::new((*self).clone())
    }
}

impl Clone for Box<dyn Job> {
    fn clone(&self) -> Box<dyn Job> {
        self.box_clone()
    }
}

/// The runner, which holds the jobs and runner configuration
pub struct JobRunner {
    jobs: Vec<BoxedJob>,
    config: RunnerConfig,
}

impl JobRunner {
    /// Creates a new runner based on the given RunnerConfig
    pub fn new(config: RunnerConfig) -> Self {
        Self {
            config,
            jobs: Vec::new(),
        }
    }

    /// Creates a new runner based on the given RunnerConfig and vector of jobs
    pub fn new_with_vec(
        config: RunnerConfig,
        jobs: Vec<impl Job + Send + Sync + Clone + 'static>,
    ) -> Self {
        let mut boxed_jobs = vec![];
        for j in jobs {
            boxed_jobs.push(Box::new(j) as BoxedJob);
        }
        Self {
            config,
            jobs: boxed_jobs,
        }
    }

    /// Adds a job to the Runner
    pub fn add_job(mut self, job: impl Job + Send + Sync + Clone + 'static) -> Self {
        self.jobs.push(Box::new(job) as BoxedJob);
        self
    }

    /// Starts the runner
    ///
    /// This will:
    ///
    /// * Validate the added jobs
    /// * Initialize the database state, creating the `widdle_jobs` table
    /// * Announce all registered jobs with their timers
    /// * Start checking and running jobs
    pub async fn start(self) -> Result<(), Error> {

        if let Some(initial_delay) = self.config.initial_delay {
            tokio::time::sleep(initial_delay).await;
        }

        let mut job_interval = tokio::time::interval(self.config.check_interval);
        let jobs = Arc::new(&self.jobs);
        loop {
            job_interval.tick().await;
            self.check_and_run_jobs(jobs.clone()).await;
        }
    }

    pub fn get_next_events(cron_str: &str, count: usize) -> Vec<DateTime<Utc>> {
        let schedule = match Schedule::from_str(cron_str) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
    
        let mut events = Vec::with_capacity(count);
    
        let now = Utc::now();
        let mut iter = schedule.after(&now);
        while let Some(event) = iter.next() {
            events.push(event);
            if events.len() >= count {
                break;
            }
        }
    
        events
    }
    
    // Logs an announcement for all registered jobs
    pub fn announce_jobs(&self) {
        for job in &self.jobs {
            info!("job '{}' with cron-schedule: {:?} next event: {:?} registered successfully",
                job.get_config().name, job.get_config().cron_str, job.get_config().get_next_event());
        }
    }

    pub fn announce_jobs_stdout(&self) {
        for job in &self.jobs {

            if job.get_config().is_cron_powered() {
                let cron_str = job.get_config().cron_str.as_ref().unwrap().to_string();

                println!("job '{}' with cron-schedule: {:?} next event: {:?} registered successfully",
                    job.get_config().name, cron_str, job.get_config().get_next_event());

                let schedule = Schedule::from_str(&cron_str).unwrap();
                println!("Upcoming fire times:");
                for datetime in schedule.upcoming(Utc).take(10) {
                    println!("-> {}", datetime);
                }
            }
            else {
  
                let chrono_duration = job.get_config().interval.unwrap();
                let std_duration: std::time::Duration = chrono_duration.to_std().unwrap();

                let human_readable: humantime::FormattedDuration = format_duration(std_duration);

                println!("job '{}' called every {} registered successfully", job.get_config().name, human_readable);
            }
        }
    }

    // Checks and runs, if necessary, all jobs concurrently
    async fn check_and_run_jobs(&self, jobs: Arc<&Vec<BoxedJob>>) {
        let job_futures = jobs
            .iter()
            .map(|job: &Box<dyn Job + Send + Sync>| {
                let j: Box<dyn Job + Send + Sync> = job.box_clone();
                self.check_and_run_job(j)
            })
            .collect::<Vec<_>>();
        join_all(job_futures).await;
    }

    // Checks and runs a single [Job](crate::Job)
    //
    // Connects to the database, checks if the given job should be run again and if so, sets the
    // `last_run` of the job to `now()` and executes the job.
    async fn check_and_run_job(&self, job: BoxedJob) -> Result<tokio::task::JoinHandle<()>, Error> {
        if job.get_config().job_should_run(chrono::Duration::from_std(self.config.check_interval).expect("expected duration conversion")) {
            job.get_config().update_last_tick();
    
            if job.get_config().oneshot && job.get_config().has_run() {
                return Ok(tokio::task::spawn(async move {}));
            }
    
            let handle = tokio::spawn(async move {
                job.get_config().set_running(true);
                job.run().await;
                job.get_config().set_running(false);
                job.get_config().set_have_run(true);
                job.get_config().update_last_tick();
            });
    
            return Ok(handle);
        }
    
        Ok(tokio::task::spawn(async move {}))
    }
}
