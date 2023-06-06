use std::time::Duration;
use widdle::{async_trait, Job, JobConfig, JobRunner, RunnerConfig};

use colored::{Color, Colorize};
use rand::seq::SliceRandom;

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct MyJobContext {
    pub name: String,
}

#[allow(dead_code)]
#[derive(Clone)]
struct MyJob {
    delay: u64,
    ctx: MyJobContext,
    config: JobConfig,
}

#[async_trait]
impl Job for MyJob {
    async fn run(&self) {
        
        let colors: &[Color] = &[Color::Red, Color::Green, Color::Blue, Color::Yellow];
        let random_color = colors.choose(&mut rand::thread_rng()).unwrap();

        println!("starting run {} delaying for {}", self.config.name.color(*random_color), self.delay.to_string().color(*random_color));
        tokio::time::sleep(Duration::from_secs(self.delay)).await;
        println!("ending run {}", self.config.name.color(*random_color));
    }

    fn get_config(&self) -> &JobConfig {
        &self.config
    }
}

#[test]
fn test_cron() {

    use cron::Schedule;
    use chrono::Utc;
    use std::str::FromStr;

    let expression = "0 0 6 * * * *";
    let schedule = Schedule::from_str(expression).unwrap();
    println!("Upcoming fire times:");
    for datetime in schedule.upcoming(Utc).take(10) {
        println!("-> {}", datetime);
    }
}


#[test]
fn test_seconds_cron() {

    use cron::Schedule;
    use chrono::Utc;
    use std::str::FromStr;

    let expression = "0 */30 * * * * *";
    let schedule = Schedule::from_str(expression).unwrap();
    println!("Upcoming fire times:");
    for datetime in schedule.upcoming(Utc).take(10) {
        println!("-> {}", datetime);
    }
}

#[tokio::test]
async fn test_basic() {

    let mut jobs = vec![];
    let job_cfg = JobConfig::new_schedule("my_job", "*/2 * * * * * *");
    let my_job = MyJob {
        delay: 5,
        ctx: MyJobContext {
            name: "my context".to_string(),
        },
        config: job_cfg,
    };

    let job_cfg2 = JobConfig::new_schedule("my_job2", "*/10 * * * * * *");
    let my_job2 = MyJob {
        delay: 40,
        ctx: MyJobContext {
            name: "my context".to_string(),
        },
        config: job_cfg2,
    };

    jobs.push(my_job);
    jobs.push(my_job2);

    let config = RunnerConfig::default().check_interval(Duration::from_millis(100));
    let job_runner = JobRunner::new_with_vec(config, jobs);

    // tokio::spawn(async move {
    //     if let Err(e) = job_runner.start().await {
    //         eprintln!("error: {}", e);
    //     }
    // });


    // tokio::time::sleep(Duration::from_secs(60)).await;

    if let Err(e) = job_runner.start().await {
        eprintln!("error: {}", e);
    }
}


#[tokio::test]
async fn test_duration() {

    let mut jobs = vec![];
    let job_cfg = JobConfig::new_interval("my_job", &chrono::Duration::seconds(2));
    let my_job = MyJob {
        delay: 5,
        ctx: MyJobContext {
            name: "my context".to_string(),
        },
        config: job_cfg,
    };

    let job_cfg2 = JobConfig::new_interval("my_job2", &chrono::Duration::seconds(10));
    let my_job2 = MyJob {
        delay: 40,
        ctx: MyJobContext {
            name: "my context".to_string(),
        },
        config: job_cfg2,
    };

    jobs.push(my_job);
    jobs.push(my_job2);

    let config = RunnerConfig::default().check_interval(Duration::from_millis(100));
    let job_runner = JobRunner::new_with_vec(config, jobs);

    // tokio::spawn(async move {
    //     if let Err(e) = job_runner.start().await {
    //         eprintln!("error: {}", e);
    //     }
    // });


    // tokio::time::sleep(Duration::from_secs(60)).await;

    if let Err(e) = job_runner.start().await {
        eprintln!("error: {}", e);
    }
}
