use fluss_rust::args::Args;
use fluss_rust::connection::{ConnectionConfig, FlussConnection};
use fluss_rust::metadata::TablePath;
use fluss_rust::record::row::InternalRow;
use std::time::Duration;

#[tokio::main]
pub async fn main() -> fluss_rust::Result<()> {
    // 1: create the table;
    let mut args = Args::default();
    args.bootstrap_server = "127.0.0.1:55367".to_string();
    let conn_config = ConnectionConfig::from_args(args);
    let conn = FlussConnection::new(conn_config).await;

    let table_path = TablePath::new("fluss".to_owned(), "rust_test".to_owned());
    let table = conn.get_table(&table_path).await;

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
