use fluss_rust::new::args::Config;
use fluss_rust::new::client::connection::FlussConnection;
use fluss_rust::new::error::Result;

#[tokio::main]
pub async fn main() -> Result<()> {
    let config = Config {
        bootstrap_server: Some("127.0.0.1:60198".to_string()),
        ..Config::default()
    };

    let conn = FlussConnection::new(config).await?;

    Ok(())
}
