use dtm_client::barrier::BranchBarrier;
use dtm_client::constants::OP_ACTION;

#[tokio::main]
async fn main() -> dtm_client::Result<()> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());

    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let key = "dtm_client:barrier_example:balance";
    let _: () = redis::cmd("SET")
        .arg(key)
        .arg(100)
        .query_async(&mut conn)
        .await?;

    let mut bb = BranchBarrier::from("saga", "gid-1", "00", OP_ACTION)?;
    bb.redis_check_adjust_amount(&mut conn, key, -30, 3600)
        .await?;

    let v: i64 = redis::cmd("GET").arg(key).query_async(&mut conn).await?;
    println!("balance after check_adjust_amount: {v}");

    Ok(())
}
