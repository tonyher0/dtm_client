# Go→Rust Parity Matrix (baseline: dtm-labs/client main@4be5512)

This document tracks feature parity between the upstream Go client and this Rust crate.

Baseline:
- Go repo: https://github.com/dtm-labs/client
- Commit: `4be5512` (main)

## dtmcli (HTTP)
- [x] Constants: status/result/op/protocol values (`dtm_client::constants`)
- [x] Errors: `ErrFailure`, `ErrOngoing`, `ErrDuplicated` (`dtm_client::DtmError`)
- [x] Helpers: `MustGenGid`, HTTP response→DTM error mapping (`dtm_client::dtmcli::{gen_gid,must_gen_gid,http_resp_to_dtm_error}`)
- [x] Saga: `NewSaga`, `Add`, `AddBranchOrder`, `SetConcurrent`, `Submit` (`dtm_client::dtmcli::Saga`)
- [x] Msg: `NewMsg`, `Add`, `AddTopic`, `SetDelay`, `Prepare`, `Submit`, `DoAndSubmit` (`dtm_client::dtmcli::Msg`)
- [x] TCC: `TccGlobalTransaction`, `TccGlobalTransaction2`, `TccFromQuery`, `CallBranch` (`dtm_client::dtmcli::Tcc`)
- [x] XA: `XaGlobalTransaction`, `XaGlobalTransaction2`, `XaFromQuery`, `CallBranch` (`dtm_client::dtmcli::Xa`)
- [x] Barrier (Redis only): `BarrierFromQuery`, `BarrierFrom`, `RedisCheckAdjustAmount`, `RedisQueryPrepared` (`dtm_client::barrier::BranchBarrier`)

## dtmgrpc (gRPC)
- [x] Proto: `dtmgimp.proto` codegen via `tonic` (`dtm_client::dtmgrpc::pb`)
- [x] Errors: DTM↔gRPC error mapping (`dtm_client::dtmgrpc::{dtm_error_to_grpc_status,grpc_status_to_dtm_error}`)
- [x] Helpers: `MustGenGid`, metadata trans info helpers (`dtm_client::dtmgrpc::{gen_gid,must_gen_gid,invoke_branch}`)
- [x] Saga: `NewSagaGrpc`, `Add`, `AddBranchOrder`, `EnableConcurrent`, `Submit` (`dtm_client::dtmgrpc::SagaGrpc`)
- [x] Msg: `NewMsgGrpc`, `Add`, `AddTopic`, `SetDelay`, `Prepare`, `Submit`, `DoAndSubmit` (`dtm_client::dtmgrpc::MsgGrpc`)
- [x] TCC: `TccGlobalTransaction`, `TccGlobalTransaction2`, `TccFromGrpc`, `CallBranch` (`dtm_client::dtmgrpc::TccGrpc`)
- [x] XA: `XaGlobalTransaction`, `XaGlobalTransaction2`, `XaGrpcFromRequest`, `CallBranch` (`dtm_client::dtmgrpc::XaGrpc`)
- [x] Barrier: `BarrierFromGrpc` (creates a `BranchBarrier` from gRPC metadata) (`dtm_client::dtmgrpc::barrier_from_grpc_metadata`)

## workflow
- [x] Proto: `wf.proto` codegen via `tonic` (`dtm_client::workflow::pb`)
- [x] Init: `InitHTTP`, `InitGrpc` (`dtm_client::workflow::{init_http,init_grpc}`)
- [x] Registry: `Register`, `Register2` (`dtm_client::workflow::{register,register2}`)
- [x] Execute: `ExecuteCtx` (`dtm_client::workflow::{execute,execute_by_query_string}`)
- [x] Workflow API: `NewBranch`, `OnCommit`, `OnRollback`, `Do` (`dtm_client::workflow::{Workflow,WorkflowBranch}`)
- [x] gRPC callback server: `workflow.Workflow/Execute` (`dtm_client::workflow::grpc_service`)
- [x] Examples + docs for `dtmcli`, `dtmgrpc`, `workflow` (`examples/`, `README.md`)
