use std::{fs::File, io::Write};

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

fn read(record_location:&str) -> Result<DataFrame, PolarsError>{
    let mut file = std::fs::File::open(record_location)?;
    let mut df = JsonReader::new(&mut file).finish()?;
    df = df.explode(["locations"])?;
    df = df.unnest(["locations"])?;
    Ok(df)
}

fn transform(df:DataFrame, tracker_id:&str, exclude_device:i32) -> Result<Vec<LocationRecord>, PolarsError>{
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
        .collect()?;
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
                tid: String::from(tracker_id),
                record_type: String::from("location")
            }
        })
        .collect();
    return Ok(lines);
}

fn main() {
    let args = Cli::parse();
    let record_location = args.input_file;
    let tracker_id = args.tracker_id;
    //TODO this should be an optional list to be more genericly useful
    let exclude_device = args.exclude_device;

    //Read
    let df = read(&record_location).expect("Failed to read in provided file");
    //Transform
    let lines = transform(df, &tracker_id, exclude_device).expect("Error working with sheet data");
    //Write
    //Not sure if this is the most efficient way to write out
    let mut active_file = String::new();
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

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf};

    use super::*;
    #[test]
    fn test_record_to_ot_line() {
        let test_time = NaiveDateTime::new(NaiveDate::from_ymd_opt(2015, 01, 11).unwrap(), NaiveTime::from_hms_opt(12, 12, 0).unwrap()).and_utc();
        let test_tst = test_time.timestamp();
        let test_record = LocationRecord{
            record_type: String::from("location"),
            tid: String::from("tt"),
            tst: test_tst,
            timestamp_nanos: test_time.timestamp_nanos_opt().unwrap(),
            lat: 42.0,
            lon: 64.0,
            acc: Some(20),
            alt: None,
            vac: None
        };
        let expected = format!("2015-01-11T12:12:00Z\t*                 \t{{\"_type\":\"location\",\"tid\":\"tt\",\"tst\":{test_tst},\"lat\":42.0,\"lon\":64.0,\"acc\":20}}\n");
        assert_eq!(test_record.create_owntrack_line(), expected)
    }
    #[test]
    fn test_transform_records(){
        let synthetic_data: PathBuf = [env!("CARGO_MANIFEST_DIR"), "src", "test_data", "synthetic_data.json"].iter().collect();
        println!("{:?}", synthetic_data);
        let df = read(synthetic_data.to_str().unwrap()).unwrap();
        let data = transform(df, "tt", 1).unwrap();
        assert_eq!(data.len(), 9)
    }
}
