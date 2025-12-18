use dtm_client::{dtmcli, workflow};

#[tokio::main]
async fn main() -> dtm_client::Result<()> {
    let dtm = std::env::var("DTM_HTTP_URL")
        .unwrap_or_else(|_| "http://localhost:36789/api/dtmsvr".to_string());
    let busi = std::env::var("BUSI_HTTP_BASE")
        .unwrap_or_else(|_| "http://localhost:8082/api/busi_start".to_string());
    let callback = std::env::var("WORKFLOW_HTTP_CALLBACK")
        .unwrap_or_else(|_| "http://localhost:8080/workflowResume".to_string());

    workflow::init_http(&dtm, callback);

    let wf_name = "workflow-http";
    let busi_for_handler = busi.clone();
    workflow::register2(wf_name, move |wf, data| {
        let busi = busi_for_handler.clone();
        async move {
            let req: serde_json::Value = serde_json::from_slice(&data)?;

            let req1 = req.clone();
            let url_rollback = format!("{busi}/TransOutCompensate");
            let branch = wf.new_branch()?;
            branch.on_rollback(move |wf, bb| {
                let req = req1.clone();
                let url_rollback = url_rollback.clone();
                async move {
                    let _ = wf
                        .http_post_json(&bb.branch_id, &bb.op, &url_rollback, &req)
                        .await?;
                    Ok(())
                }
            })?;
            let _ = branch
                .http_post_json(&format!("{busi}/TransOut"), &req)
                .await?;

            let req2 = req.clone();
            let url_rollback = format!("{busi}/TransInCompensate");
            let branch = wf.new_branch()?;
            branch.on_rollback(move |wf, bb| {
                let req = req2.clone();
                let url_rollback = url_rollback.clone();
                async move {
                    let _ = wf
                        .http_post_json(&bb.branch_id, &bb.op, &url_rollback, &req)
                        .await?;
                    Ok(())
                }
            })?;
            let _ = branch
                .http_post_json(&format!("{busi}/TransIn"), &req)
                .await?;

            Ok(Vec::new())
        }
    })?;

    let gid = match std::env::var("GID") {
        Ok(gid) if !gid.is_empty() => gid,
        _ => dtmcli::gen_gid(&dtm).await?,
    };
    let data = serde_json::to_vec(&serde_json::json!({ "amount": 30 }))?;
    let _ = workflow::execute(wf_name, &gid, data).await?;
    println!("executed workflow gid={gid}");

    Ok(())
}
