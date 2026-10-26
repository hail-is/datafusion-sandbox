//! Decode a datafusion-python plan carrying inline Python UDFs with datafusion-python's own
//! codec (from crates.io) in a Rust binary that embeds Python, then execute it.
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::protobuf::LogicalPlanNode;
use datafusion_python::codec::PythonLogicalCodec;
use prost::Message;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> datafusion::error::Result<()> {
    for path in std::env::args().skip(1) {
        let bytes = std::fs::read(&path)?;
        let ctx = SessionContext::new();
        let node = LogicalPlanNode::decode(bytes.as_slice()).unwrap();
        let plan = node.try_into_logical_plan(&ctx.task_ctx(), &PythonLogicalCodec::default())?;
        println!("== {path}");
        ctx.execute_logical_plan(plan).await?.show().await?;
    }
    Ok(())
}
