use std::net::SocketAddr;

use dtm_client::{dtmgrpc, workflow};
use prost::Message;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BusiReq {
    #[prost(int64, tag = "1")]
    pub amount: i64,
    #[prost(int64, tag = "2")]
    pub user_id: i64,
    #[prost(string, tag = "3")]
    pub trans_out_result: String,
    #[prost(string, tag = "4")]
    pub trans_in_result: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BusiReply {
    #[prost(string, tag = "1")]
    pub message: String,
}

#[tokio::main]
async fn main() -> dtm_client::Result<()> {
    let dtm = std::env::var("DTM_GRPC_ADDR").unwrap_or_else(|_| "localhost:36790".to_string());
    let busi = std::env::var("BUSI_GRPC_ADDR").unwrap_or_else(|_| "localhost:50589".to_string());

    let listen: SocketAddr = std::env::var("WORKFLOW_GRPC_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()
        .map_err(|e| dtm_client::DtmError::InvalidInput {
            message: format!("invalid WORKFLOW_GRPC_LISTEN: {e}"),
        })?;
    let client_host = std::env::var("WORKFLOW_GRPC_CLIENT_HOST")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string());

    workflow::init_grpc(&dtm, client_host);

    let wf_name = "workflow-grpc";
    let busi_for_handler = busi.clone();
    workflow::register2(wf_name, move |wf, data| {
        let busi = busi_for_handler.clone();
        async move {
            let req = BusiReq::decode(data.as_slice())?;

            let url_rollback = format!("{busi}/busi.Busi/TransOutRevert");
            let branch = wf.new_branch()?;
            let req1 = req.clone();
            branch.on_rollback(move |wf, bb| {
                let req = req1.clone();
                let url_rollback = url_rollback.clone();
                async move {
                    let _: BusiReply = wf
                        .grpc_invoke(&bb.branch_id, &bb.op, &url_rollback, req)
                        .await?;
                    Ok(())
                }
            })?;
            let _: BusiReply = branch
                .grpc_invoke(&format!("{busi}/busi.Busi/TransOut"), req.clone())
                .await?;

            let url_rollback = format!("{busi}/busi.Busi/TransInRevert");
            let branch = wf.new_branch()?;
            let req2 = req.clone();
            branch.on_rollback(move |wf, bb| {
                let req = req2.clone();
                let url_rollback = url_rollback.clone();
                async move {
                    let _: BusiReply = wf
                        .grpc_invoke(&bb.branch_id, &bb.op, &url_rollback, req)
                        .await?;
                    Ok(())
                }
            })?;
            let _: BusiReply = branch
                .grpc_invoke(&format!("{busi}/busi.Busi/TransIn"), req)
                .await?;

            Ok(Vec::new())
        }
    })?;

    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(workflow::grpc_service())
            .serve(listen)
            .await
        {
            eprintln!("workflow grpc server exited: {e}");
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let gid = match std::env::var("GID") {
        Ok(gid) if !gid.is_empty() => gid,
        _ => dtmgrpc::gen_gid(&dtm).await?,
    };
    let data = BusiReq {
        amount: 30,
        user_id: 0,
        trans_out_result: String::new(),
        trans_in_result: String::new(),
    }
    .encode_to_vec();
    let _ = workflow::execute(wf_name, &gid, data).await?;
    println!("executed grpc workflow gid={gid}");

    Ok(())
}
