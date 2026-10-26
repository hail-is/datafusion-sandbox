//! Throwaway probe for notes/df-ordering-physical-planning.md: prints DataFusion 55.1 physical plans
//! for queries over tables declared sorted, under a few session settings. Not part of the crate.

use datafusion::error::Result;
use datafusion::physical_plan::displayable;
use datafusion::prelude::*;

const QUERIES: &[(&str, &str)] = &[
    (
        "inner equi-join on full key",
        "SELECT a.contig, a.pos, v, w FROM a JOIN b ON a.contig = b.contig AND a.pos = b.pos",
    ),
    (
        "inner equi-join, ORDER BY key above",
        "SELECT a.contig, a.pos, v, w FROM a JOIN b ON a.contig = b.contig AND a.pos = b.pos ORDER BY a.contig, a.pos",
    ),
    (
        "inner equi-join, keys written in permuted order",
        "SELECT a.contig, a.pos, v, w FROM a JOIN b ON a.pos = b.pos AND a.contig = b.contig ORDER BY a.contig, a.pos",
    ),
    (
        "left join, ORDER BY left key",
        "SELECT a.contig, a.pos, v, w FROM a LEFT JOIN b ON a.contig = b.contig AND a.pos = b.pos ORDER BY a.contig, a.pos",
    ),
    (
        "right join, ORDER BY right key",
        "SELECT b.contig, b.pos, v, w FROM a RIGHT JOIN b ON a.contig = b.contig AND a.pos = b.pos ORDER BY b.contig, b.pos",
    ),
    (
        "full join, ORDER BY coalesced key",
        "SELECT coalesce(a.contig, b.contig) c, coalesce(a.pos, b.pos) p, v, w FROM a FULL JOIN b ON a.contig = b.contig AND a.pos = b.pos ORDER BY c, p",
    ),
    (
        "left semi join (IN), ORDER BY key",
        "SELECT contig, pos, v FROM a WHERE EXISTS (SELECT 1 FROM b WHERE a.contig = b.contig AND a.pos = b.pos) ORDER BY contig, pos",
    ),
    (
        "equi-join on key prefix (contig) with residual filter",
        "SELECT a.contig, a.pos, v, w FROM a JOIN b ON a.contig = b.contig AND a.pos < b.pos ORDER BY a.contig, a.pos",
    ),
    (
        "interval join: contig equality plus range",
        "SELECT a.contig, a.pos, v, label FROM a JOIN i ON a.contig = i.contig AND a.pos >= i.start_pos AND a.pos < i.end_pos ORDER BY a.contig, a.pos",
    ),
    (
        "range-only join (no equality)",
        "SELECT a.pos, v, label FROM a JOIN i ON a.pos >= i.start_pos AND a.pos < i.end_pos ORDER BY a.pos",
    ),
    (
        "single range predicate join",
        "SELECT a.pos, v, label FROM a JOIN i ON a.pos >= i.start_pos ORDER BY a.pos",
    ),
    (
        "aggregate on full key, ORDER BY key",
        "SELECT contig, pos, sum(v) FROM a GROUP BY contig, pos ORDER BY contig, pos",
    ),
    (
        "aggregate on key prefix",
        "SELECT contig, sum(v) FROM a GROUP BY contig ORDER BY contig",
    ),
    (
        "aggregate on key in permuted order",
        "SELECT pos, contig, sum(v) FROM a GROUP BY pos, contig ORDER BY contig, pos",
    ),
    (
        "aggregate on key plus extra column (partially sorted)",
        "SELECT contig, v, count(*) FROM a GROUP BY contig, v",
    ),
    (
        "ordered aggregate first_value ORDER BY pos",
        "SELECT contig, first_value(v ORDER BY pos) FROM a GROUP BY contig",
    ),
    (
        "window: running sum partition by contig order by pos",
        "SELECT contig, pos, sum(v) OVER (PARTITION BY contig ORDER BY pos ROWS UNBOUNDED PRECEDING) FROM a",
    ),
    (
        "window: order by full key, range frame",
        "SELECT contig, pos, sum(v) OVER (ORDER BY contig, pos) FROM a",
    ),
    (
        "window: descending order (reverse of input)",
        "SELECT contig, pos, sum(v) OVER (ORDER BY contig DESC, pos DESC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM a",
    ),
    (
        "window: unbounded following frame",
        "SELECT contig, pos, sum(v) OVER (PARTITION BY contig ORDER BY pos ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM a",
    ),
    (
        "projection: pos + 1",
        "SELECT contig, pos + 1 AS p FROM a ORDER BY contig, p",
    ),
    (
        "projection: CAST(pos AS DOUBLE)",
        "SELECT contig, CAST(pos AS DOUBLE) AS p FROM a ORDER BY contig, p",
    ),
    (
        "projection: pos * -1 (antitone)",
        "SELECT contig, pos * -1 AS p FROM a ORDER BY contig, p DESC",
    ),
    (
        "projection: concat(contig, ':') (not monotone)",
        "SELECT concat(contig, ':') AS c, pos FROM a ORDER BY c, pos",
    ),
    (
        "projection: struct key, ORDER BY field",
        "SELECT k['contig'] AS c, k['pos'] AS p FROM (SELECT named_struct('contig', contig, 'pos', pos) AS k FROM a) ORDER BY c, p",
    ),
    (
        "projection: ORDER BY struct column",
        "SELECT k FROM (SELECT named_struct('contig', contig, 'pos', pos) AS k FROM a) ORDER BY k",
    ),
    (
        "filter keeps order",
        "SELECT contig, pos FROM a WHERE v > 3 ORDER BY contig, pos",
    ),
    (
        "filter contig = const, ORDER BY pos",
        "SELECT pos FROM a WHERE contig = '1' ORDER BY pos",
    ),
    (
        "union all, ORDER BY key",
        "SELECT contig, pos FROM a UNION ALL SELECT contig, pos FROM b ORDER BY contig, pos",
    ),
    (
        "union all, no ORDER BY",
        "SELECT contig, pos FROM a UNION ALL SELECT contig, pos FROM b",
    ),
    (
        "limit on ordered table",
        "SELECT contig, pos FROM a ORDER BY contig, pos LIMIT 5",
    ),
    (
        "ORDER BY that contradicts the declared order",
        "SELECT contig, pos FROM a ORDER BY pos",
    ),
];

async fn context(target_partitions: usize, prefer_hash_join: bool, prefer_existing_sort: bool, dir: &str) -> Result<SessionContext> {
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .set_bool("datafusion.optimizer.prefer_hash_join", prefer_hash_join)
        .set_bool("datafusion.optimizer.prefer_existing_sort", prefer_existing_sort);
    let ctx = SessionContext::new_with_config(config);
    for (name, cols, order) in [
        ("a", "contig VARCHAR NOT NULL, pos BIGINT NOT NULL, v BIGINT", "contig ASC, pos ASC"),
        ("b", "contig VARCHAR NOT NULL, pos BIGINT NOT NULL, w BIGINT", "contig ASC, pos ASC"),
        (
            "i",
            "contig VARCHAR NOT NULL, start_pos BIGINT NOT NULL, end_pos BIGINT NOT NULL, label VARCHAR",
            "contig ASC, start_pos ASC",
        ),
    ] {
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE {name} ({cols}) STORED AS CSV LOCATION '{dir}/{name}.csv' \
             OPTIONS ('format.has_header' 'true') WITH ORDER ({order})"
        ))
        .await?;
    }
    Ok(ctx)
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let d = dir.path().to_str().unwrap().to_string();
    std::fs::write(format!("{d}/a.csv"), "contig,pos,v\n1,1,1\n1,5,2\n2,3,3\n2,9,4\n")?;
    std::fs::write(format!("{d}/b.csv"), "contig,pos,w\n1,1,10\n1,6,20\n2,3,30\n2,8,40\n")?;
    std::fs::write(
        format!("{d}/i.csv"),
        "contig,start_pos,end_pos,label\n1,0,4,x\n1,4,10,y\n2,0,5,z\n",
    )?;

    let filter = std::env::args().nth(1);
    for (target_partitions, prefer_hash_join, prefer_existing_sort) in
        [(1, false, true), (4, false, true), (4, true, true), (4, false, false)]
    {
        let ctx = context(target_partitions, prefer_hash_join, prefer_existing_sort, &d).await?;
        for (title, sql) in QUERIES {
            if filter.as_deref().is_some_and(|f| !title.contains(f)) {
                continue;
            }
            println!(
                "=== {title} [target_partitions={target_partitions}, prefer_hash_join={prefer_hash_join}, prefer_existing_sort={prefer_existing_sort}]"
            );
            println!("{sql}");
            let df = ctx.sql(sql).await?;
            let plan = df.clone().create_physical_plan().await?;
            print!("{}", displayable(plan.as_ref()).indent(true));
            df.collect().await?;
            println!();
        }
    }
    Ok(())
}
