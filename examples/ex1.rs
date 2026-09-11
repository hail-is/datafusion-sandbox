// `debug_assertions` here is a proxy for dev builds. Optimized builds don't trigger the linker warning.
#![cfg_attr(
    all(target_os = "macos", debug_assertions),
    allow(
        linker_messages,
        reason = "Apple ld falls back to DWARF when the CLI exceeds compact unwind's 16 MiB offset range; rust-lang/rust#159105 tracks this diagnostic"
    )
)]

use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_sandbox::generated::make_table_range_join;

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
