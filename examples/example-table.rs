use std::{thread, time::Duration};

use arrow::array::record_batch;
use fluss_rust::{
    Result,
    args::Args,
    connection::{ConnectionConfig, FlussConnection},
    metadata::TablePath,
    metadata::{DataTypes, Schema, TableDescriptor},
    record::row::InternalRow,
};

#[tokio::main]
pub async fn main() -> Result<()> {
    // 1: create the table;
    let mut args = Args::default();
    args.bootstrap_server = "127.0.0.1:9123".to_string();
    let conn_config = ConnectionConfig::from_args(args);
    let conn = FlussConnection::new(conn_config).await;

    let admin = conn.get_admin();

    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("c1", DataTypes::int())
                .column("c2", DataTypes::string())
                .build(),
        )
        .build();

    let table_path = TablePath::new("fluss".to_owned(), "rust_test".to_owned());

    admin
        .create_table(&table_path, &table_descriptor, true)
        .await
        .unwrap();

    // 2: get the table
    let table_info = admin.get_table(&table_path).await.unwrap();
    print!("Get created table:\n {}\n", table_info);

    // let's sleep 2 seconds to wait leader ready
    thread::sleep(Duration::from_secs(2));

    // 3: append log to the table
    let table = conn.get_table(&table_path).await;
    let append_writer = table.new_append().create_writer();
    let batch = record_batch!(("c1", Int32, [1, 2, 3, 4, 5, 6]), ("c2", Utf8, ["a1", "a2", "a3", "a4", "a5", "a6"])).unwrap();
    append_writer.append(batch).await?;
    println!("Start to scan log records......");
    // 4: scan the records
    let log_scanner = table.new_scan().create_log_scanner();
    log_scanner.subscribe(0, 0).await;

    loop {
        let scan_records = log_scanner.poll(Duration::from_secs(10)).await?;
        println!("Start to poll records......");
        for record in scan_records {
            let row = record.row();
            println!(
                "{{{}, {}}}@{}",
                row.get_int(0),
                row.get_string(1),
                record.offset()
            );
        }
    }
    Ok(())
}
