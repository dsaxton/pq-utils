use clap::{Arg, Command};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use prettytable::{Cell, Row as PrettyTableRow, Table};
use std::fs::File;
use std::io::Result;

fn cli() -> Command {
    Command::new("pq-utils")
        .version("0.3.0")
        .author("Daniel Saxton")
        .about("A utility tool for reading parquet files")
        .arg_required_else_help(true)
        .subcommand(
            Command::new("cat")
                .about("Display the contents of a file")
                .arg(
                    Arg::new("file")
                        .help("The name of the file to display")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::new("format")
                        .short('f')
                        .long("format")
                        .help("Output format: csv or json")
                        .value_parser(["csv", "json"])
                        .default_value("csv"),
                ),
        )
        .subcommand(
            Command::new("head")
                .about("Display the first n rows of a file")
                .arg(
                    Arg::new("file")
                        .help("The name of the file to display")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::new("format")
                        .short('f')
                        .long("format")
                        .help("Output format: csv or json")
                        .value_parser(["csv", "json"])
                        .default_value("csv"),
                )
                .arg(
                    Arg::new("n_rows")
                        .short('n')
                        .long("n_rows")
                        .help("Number of rows to display")
                        .value_parser(clap::value_parser!(u64))
                        .default_value("10"),
                ),
        )
        .subcommand(
            Command::new("schema")
                .about("Display the schema of a file")
                .arg(
                    Arg::new("file")
                        .help("The name of the file to display the schema for")
                        .required(true)
                        .index(1),
                ),
        )
}

fn display_parquet_data(file: &str, format: &str, num_records: Option<u64>) -> Result<()> {
    let file = File::open(file)?;
    match format {
        "csv" => display_parquet_data_csv(file, num_records),
        "json" => display_parquet_data_json(file, num_records),
        _ => unreachable!("Handled by value_parser"),
    }
}

fn display_parquet_data_csv(file: File, num_records: Option<u64>) -> Result<()> {
    let reader = SerializedFileReader::new(file)?;
    let iter = reader.get_row_iter(None)?;
    let iter = iter.take(num_records.unwrap_or(u64::MAX) as usize);

    let mut writer = csv::Writer::from_writer(std::io::stdout());

    let schema_descr = reader.metadata().file_metadata().schema_descr();
    let headers: Vec<String> = schema_descr
        .columns()
        .iter()
        .map(|col| col.name().to_string())
        .collect();
    writer.write_record(&headers)?;

    for record in iter {
        let row: Row = record?;
        let values: Vec<String> = row
            .get_column_iter()
            .map(|field| match field.1 {
                parquet::record::Field::Str(s) => s.clone(),
                _ => field.1.to_string(),
            })
            .collect();
        writer.write_record(&values)?;
    }

    writer.flush()?;
    Ok(())
}

fn display_parquet_data_json(file: File, num_records: Option<u64>) -> Result<()> {
    let reader = SerializedFileReader::new(file)?;
    let iter = reader.get_row_iter(None)?;
    let iter = iter.take(num_records.unwrap_or(u64::MAX) as usize);

    let mut rows = Vec::new();

    for record in iter {
        let row: Row = record?;
        let mut obj = serde_json::Map::new();

        for (i, field) in row.get_column_iter().enumerate() {
            let col_name = reader.metadata().file_metadata().schema_descr().columns()[i].name();
            obj.insert(
                col_name.to_string(),
                match field.1 {
                    parquet::record::Field::Str(s) => serde_json::Value::String(s.clone()),
                    parquet::record::Field::Int(i) => serde_json::Value::Number((*i).into()),
                    parquet::record::Field::Long(i) => serde_json::Value::Number((*i).into()),
                    parquet::record::Field::Float(f) => serde_json::Value::Number(
                        serde_json::Number::from_f64(f64::from(*f)).unwrap(),
                    ),
                    parquet::record::Field::Double(f) => {
                        serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap())
                    }
                    parquet::record::Field::Bool(b) => serde_json::Value::Bool(*b),
                    _ => serde_json::Value::String(field.1.to_string()),
                },
            );
        }
        rows.push(obj);
    }

    serde_json::to_writer(std::io::stdout(), &rows)?;
    Ok(())
}

fn display_parquet_schema(file: &str) -> Result<()> {
    let file = File::open(file)?;
    let reader = SerializedFileReader::new(file)?;
    let schema_descr = reader.metadata().file_metadata().schema_descr();

    let mut table = Table::new();

    table.add_row(PrettyTableRow::new(vec![
        Cell::new("Column name"),
        Cell::new("Physical type"),
        Cell::new("Logical type"),
    ]));

    for col in schema_descr.columns() {
        let logical_type = col.logical_type().map(|lt| format!("{:?}", lt)).unwrap_or_default();
        table.add_row(PrettyTableRow::new(vec![
            Cell::new(col.name()),
            Cell::new(&col.physical_type().to_string()),
            Cell::new(&logical_type),
        ]));
    }

    table.printstd();

    Ok(())
}

fn main() {
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("cat", subcommand)) => {
            let file = subcommand.get_one::<String>("file").unwrap();
            let format = subcommand.get_one::<String>("format").unwrap();
            if let Err(e) = display_parquet_data(file, format, None) {
                eprintln!("Error displaying file: {}", e);
            }
        }
        Some(("head", subcommand)) => {
            let file = subcommand.get_one::<String>("file").unwrap();
            let format = subcommand.get_one::<String>("format").unwrap();
            let num_records = subcommand.get_one::<u64>("n_rows").copied();
            if let Err(e) = display_parquet_data(file, format, num_records) {
                eprintln!("Error displaying file: {}", e);
            }
        }
        Some(("schema", subcommand)) => {
            let file = subcommand.get_one::<String>("file").unwrap();
            if let Err(e) = display_parquet_schema(file) {
                eprintln!("Error displaying schema: {}", e);
            }
        }
        _ => unreachable!("Handled by clap"),
    }
}
