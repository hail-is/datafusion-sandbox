use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_sandbox::*;

#[tokio::main]
async fn main() -> Result<()> {
    // create the dataframe
    let ctx = SessionContext::new();
    let df = make_table_range_join(&ctx, 1_000_000, 1_000, 8192)?;
    // .sort_by(vec![col("idx")])?;

    // execute and print results
    df.explain(false, false)?.show().await?;
    // df.limit(0, Some(20))?.show().await?;
    Ok(())
}
