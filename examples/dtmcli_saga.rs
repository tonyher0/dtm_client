use dtm_client::dtmcli;

#[tokio::main]
async fn main() -> dtm_client::Result<()> {
    let dtm = std::env::var("DTM_HTTP_URL")
        .unwrap_or_else(|_| "http://localhost:36789/api/dtmsvr".to_string());
    let busi = std::env::var("BUSI_HTTP_BASE")
        .unwrap_or_else(|_| "http://localhost:8082/api/busi_start".to_string());

    let gid = match std::env::var("GID") {
        Ok(gid) if !gid.is_empty() => gid,
        _ => dtmcli::gen_gid(&dtm).await?,
    };

    let req = serde_json::json!({ "amount": 30 });

    let mut saga = dtmcli::Saga::new(&dtm, &gid);
    saga.add(
        format!("{busi}/TransOut"),
        format!("{busi}/TransOutCompensate"),
        &req,
    )?;
    saga.add(
        format!("{busi}/TransIn"),
        format!("{busi}/TransInCompensate"),
        &req,
    )?;

    saga.submit().await?;
    println!("submitted saga gid={gid}");
    Ok(())
}
