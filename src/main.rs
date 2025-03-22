use std::{fs::File, io::Write, str::FromStr};

use chrono::prelude::*;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json;
use clap::Parser;

#[derive(Debug, Serialize, Deserialize)]
pub struct LocationRecord {
    #[serde(rename = "_type")]
    record_type: String,
    tid: String,
    tst: i64, //TimeStamp seconds
    #[serde(skip_serializing)]
    timestamp_nanos: i64,
    lat: f64,
    lon: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    acc: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    alt: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vac: Option<i64>,
}

impl LocationRecord {
    fn get_timestamp(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_nanos(self.timestamp_nanos)
    }
    fn create_owntrack_line(&self) -> String {
        let timestamp = self.get_timestamp().format("%Y-%m-%dT%H:%M:%SZ");
        let record_json = serde_json::to_string(&self).unwrap();
        return format!("{timestamp}\t*                 \t{record_json}\n");
    }
}

#[derive(Parser)]
#[command(version, about, long_about=None)]
struct Cli {
    #[arg(short = 'f')]
    input_file: String,
    #[arg(short = 'i')]
    tracker_id: String, // Arbritry two character code for OT
    #[arg(short = 'e')]
    exclude_device: i32 // Probably should be an optional list, i only had the one
}

fn main() {
    let args = Cli::parse();
    let record_location = args.input_file;
    let tracker_id = args.tracker_id;
    let exclude_device = args.exclude_device;

    //Read
    let mut file = std::fs::File::open(record_location).unwrap();
    let mut df = JsonReader::new(&mut file).finish().unwrap();
    df = df.explode(["locations"]).unwrap();
    df = df.unnest(["locations"]).unwrap();
    //Transform
    let output = df
        .clone()
        .lazy()
        .with_columns([
            (col("latitudeE7").cast(DataType::Float64) / lit(10_000_000.0)).alias("lat"),
            (col("longitudeE7").cast(DataType::Float64) / lit(10_000_000.0)).alias("long"),
            (col("timestamp").cast(DataType::Datetime(
                TimeUnit::Nanoseconds,
                Some(PlSmallStr::from("UTC")),
            ))),
            (col("timestamp")
                .cast(DataType::Datetime(
                    TimeUnit::Nanoseconds,
                    Some(PlSmallStr::from("UTC")),
                ))
                .cast(DataType::Int64)
                / lit(1_000_000_000))
            .alias("tst"),
        ])
        .select([
            col("lat"),
            col("long"),
            col("accuracy").alias("acc"),
            col("altitude").alias("alt"),
            col("verticalAccuracy").alias("vac"),
            col("timestamp"),
            col("tst"),
            col("deviceTag"),
        ])
        .filter(
            col("deviceTag")
                .neq(lit(exclude_device))
                .or(col("deviceTag").is_null()),
        )
        .filter(col("lat").is_not_null())
        .sort(["tst"], Default::default())
        .collect()
        .unwrap();
    let lines: Vec<LocationRecord> = output
        .into_struct(PlSmallStr::from_str("struct"))
        .into_series()
        .iter()
        .map(|row: AnyValue<'_>| {
            let row_vals: Vec<_> = row._iter_struct_av().collect();
            LocationRecord {
                lat: row_vals[0].try_extract().unwrap(),
                lon: row_vals[1].try_extract().unwrap(),
                acc: row_vals[2].clone().try_into().unwrap(),
                alt: row_vals[3].clone().try_into().unwrap(),
                vac: row_vals[4].clone().try_into().unwrap(),
                timestamp_nanos: row_vals[5].try_extract().unwrap(),
                tst: row_vals[6].try_extract().unwrap(),
                tid: tracker_id.clone(),
                record_type: String::from("location")
            }
        })
        .collect();
    //Write
    //Not sure if this is the most efficient way to write out
    let mut active_file = String::from_str("").unwrap();
    let mut active_lines: Vec<String> = Vec::new();
    for lr in lines {
        // Records are ordered by date so we can fill active lines until the active file changes then write out to the next one
        let timestamp = lr.get_timestamp();
        let year = timestamp.year().to_string();
        let month = timestamp.month().to_string();
        let line_file = format!("rust_output/{year}-{month}.rec");
        match line_file == active_file {
            true => {}
            _ => {
                if active_lines.len() > 0 {
                    //Dont open file if we have nothing to write
                    let mut f = File::create(active_file).expect("unable to open file");
                    for line in active_lines.drain(..) {
                        f.write(line.as_bytes()).expect("failed to write to file");
                    }
                }
                active_file = line_file.clone();
            }
        }
        active_lines.push(lr.create_owntrack_line());
    }
    print!("{active_file}");
}
