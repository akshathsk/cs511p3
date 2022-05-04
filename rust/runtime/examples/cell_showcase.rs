use rand::Rng;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::time::Instant;

use runtime::forecast::cell::CellEstimator;
use runtime::forecast::cell::ForecastSelector;
use runtime::forecast::Series;
use runtime::forecast::TimeType;
use runtime::forecast::ValueType;


struct Dataset {
    pub times: Vec<TimeType>,
    pub values: Vec<ValueType>,
    pub final_time: TimeType,
    pub final_answer: ValueType,
}

fn make_batches() -> Vec<Vec<i32>> {
    let mut rng = rand::thread_rng();
    (0..100).map(|_| {
        let batch_size: usize = rng.gen_range(9000..11000);
        (0..batch_size).map(|_| rng.gen_range(0..10000)).collect()
    }).collect()
}

fn make_sum_data() -> Dataset {
    let mut total_rows = 0.0;
    let mut total_sum = 0.0;
    let mut proc_rows = Vec::new();
    let mut sums = Vec::new();
    for batch in make_batches() {
        // aggregate within batch
        let sum: ValueType = batch.iter().sum::<i32>().into();

        // online aggregate so far
        total_rows += batch.len() as f64;
        total_sum += sum;

        // record online aggregate series
        proc_rows.push(total_rows);
        sums.push(total_sum);
    }
    Dataset {
        times: proc_rows,
        values: sums,
        final_time: total_rows,
        final_answer: total_sum,
    }
}

fn make_avg_data() -> Dataset {
    let mut total_rows = 0.0;
    let mut total_sum = 0.0;
    let mut proc_rows = Vec::new();
    let mut avgs = Vec::new();
    for batch in make_batches() {
        // aggregate within batch
        let sum: ValueType = batch.iter().sum::<i32>().into();

        // online aggregate so far
        total_rows += batch.len() as f64;
        total_sum += sum;

        // record online aggregate series
        proc_rows.push(total_rows);
        avgs.push(total_sum / total_rows);
    }
    Dataset {
        times: proc_rows,
        values: avgs,
        final_time: total_rows,
        final_answer: total_sum / total_rows,
    }
}

fn make_count_distinct_data() -> Dataset {
    let mut total_rows = 0.0;
    let mut total_set = HashSet::new();
    let mut proc_rows = Vec::new();
    let mut counts = Vec::new();
    for batch in make_batches() {
        batch.iter().for_each(|elem| {
            total_set.insert(elem.clone());
        });

        // online aggregate so far
        total_rows += batch.len() as TimeType;

        // record online aggregate series
        proc_rows.push(total_rows);
        counts.push(total_set.len() as ValueType);
    }
    Dataset {
        times: proc_rows,
        values: counts,
        final_time: total_rows,
        final_answer: total_set.len() as ValueType,
    }
}

fn read_q1_data() -> Vec<Dataset> {
    const FILENAME: &str = "src/resources/q1_flatten.txt";
    let file = File::open(FILENAME).expect("file wasn't found.");
    let reader = BufReader::new(file);
    reader.lines()
        .map(|line| {
            let values: Vec<ValueType> = line.unwrap()
                .split(' ')
                .map(|num_str| num_str.parse::<ValueType>().unwrap())
                .collect();
            let times: Vec<TimeType> = (1..values.len()+1).map(|t| t as TimeType).collect();
            let final_time = times[times.len()-1];
            let final_answer = values[values.len()-1];
            Dataset {
                times,
                values,
                final_time,
                final_answer,
            }
        })
        .collect()
}

fn do_test(dataset: &Dataset, mut est: Box<dyn CellEstimator>) {
    const SUCCEED_PERR: f64 = 1.0;
    let mut succeed_flag = false;
    let series = Series::new(&dataset.times, &dataset.values);
    for (idx, tv) in series.iter().enumerate() {
        est.consume(&tv);
        let f = est.produce();
        let pred_v = f.predict(dataset.final_time);
        let perr = 100.0 * (dataset.final_answer - pred_v).abs() / dataset.final_answer;
        if idx % (series.len() / 10) == 0 || idx == series.len() {
            log::info!(
                "t= {}: ans= {}, pred_t= {}, perr= {}",
                tv.t,
                dataset.final_answer,
                pred_v,
                perr,
            );
        }
        if !succeed_flag && perr < SUCCEED_PERR {
            succeed_flag = true;
            log::info!(
                "t= {}: ans= {}, pred_t= {}, perr= {}  <SUCCESS>",
                tv.t,
                dataset.final_answer,
                pred_v,
                perr,
            );
        }
        log::debug!("\tusing {:?}", f);
    }
}

fn test_sum() {
    log::info!("Generating on sum dataset");
    let dataset = make_sum_data();
    let est = ForecastSelector::make_with_default_candidates();
    let start_time = Instant::now();
    do_test(&dataset, est);
    log::info!("Forecasting took {:?}", start_time.elapsed());
}

fn test_avg() {
    log::info!("Generating on avg dataset");
    let dataset = make_avg_data();
    let est = ForecastSelector::make_with_default_candidates();
    let start_time = Instant::now();
    do_test(&dataset, est);
    log::info!("Forecasting took {:?}", start_time.elapsed());
}

fn test_count_distinct() {
    log::info!("Generating on count-distinct dataset");
    let dataset = make_count_distinct_data();
    let est = ForecastSelector::make_with_default_candidates();
    let start_time = Instant::now();
    do_test(&dataset, est);
    log::info!("Forecasting took {:?}", start_time.elapsed());
}

fn test_q1() {
    log::info!("Testing q1");
    let q1_start_time = Instant::now();
    let datasets = read_q1_data();
    for (idx, dataset) in datasets.iter().enumerate() {
        log::info!("Testing q1: series {} / {}", idx + 1, datasets.len());
        let est = ForecastSelector::make_with_default_candidates();
        let start_time = Instant::now();
        do_test(dataset, est);   
        log::info!("Forecasting took {:?}", start_time.elapsed());
    }
    log::info!("Testing q1 took {:?}", q1_start_time.elapsed());
}

fn main() {
    // execution init
    env_logger::Builder::from_default_env()
        .format_timestamp_micros()
        .init();

    // run showcase tests
    test_sum();
    test_avg();
    test_count_distinct();
    test_q1();
}