// `debug_assertions` here is a proxy for dev builds. Optimized builds don't trigger the linker warning.
#![cfg_attr(
    all(target_os = "macos", debug_assertions),
    allow(
        linker_messages,
        reason = "Apple ld falls back to DWARF when the CLI exceeds compact unwind's 16 MiB offset range; rust-lang/rust#159105 tracks this diagnostic"
    )
)]

use clap::{Args, Parser, Subcommand, ValueEnum};
use datafusion::error::{DataFusionError, Result};

use datafusion_sandbox::combiner_run::{Action, CombinerRun, WriteTarget};
use datafusion_sandbox::format::{InputFormat, OutputFormat};
use datafusion_sandbox::formulation::Formulation;
use std::{num::NonZeroUsize, path::Path, thread::available_parallelism};

const DEFAULT_SHOW_LIMIT: usize = 20;

#[derive(Parser)]
#[command(about = "Run hail-style pipelines built on datafusion")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Number of worker threads to execute on.
    ///
    /// Defaults to available parallelism. Independent of the session's target-partition setting.
    #[arg(long, short = 'j', global = true, value_parser = parse_thread_count)]
    threads: Option<usize>,
}

/// Rejects 0, which `Builder::worker_threads` panics on, with a message rather than clap's
/// generated `0 is not in 1..18446744073709551615`.
fn parse_thread_count(value: &str) -> std::result::Result<usize, String> {
    match value.parse::<usize>() {
        Ok(0) => Err("thread count must be at least 1".to_string()),
        Ok(threads) => Ok(threads),
        Err(e) => Err(e.to_string()),
    }
}

#[derive(Subcommand)]
enum Command {
    /// Combine reference data for the dataset's sample set under PATH.
    CombineRefs(CombineRefsArgs),
    /// Combine alleles for the dataset's sample set under PATH.
    CombineAlleles(CombinerArgs),
}

#[derive(Args)]
struct CombineRefsArgs {
    #[command(flatten)]
    combiner: CombinerArgs,
    /// How to build the reference combiner's plan.
    #[arg(long, value_enum, default_value = "union")]
    formulation: CombineRefsFormulationArg,
    /// Number of sample groups the grouped-merge formulation merges before merging the groups.
    ///
    /// Defaults to the thread count. One fewer gives the final merge a thread of its own.
    /// Accepted only with `--formulation grouped-merge`.
    #[arg(long, value_name = "N")]
    groups: Option<NonZeroUsize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CombineRefsFormulationArg {
    Union,
    GroupedMerge,
}

impl CombineRefsFormulationArg {
    /// The formulation for this argument, given `--groups` and the run's thread count.
    fn formulation(self, groups: Option<NonZeroUsize>, threads: usize) -> Result<Formulation> {
        match (self, groups) {
            (Self::Union, None) => Ok(Formulation::CombineRefsUnion),
            (Self::Union, Some(_)) => Err(DataFusionError::Configuration(
                "--groups applies only to the grouped-merge formulation".to_string(),
            )),
            (Self::GroupedMerge, groups) => Ok(Formulation::CombineRefsGroupedMerge {
                groups: groups
                    .unwrap_or_else(|| NonZeroUsize::new(threads).unwrap_or(NonZeroUsize::MIN)),
            }),
        }
    }
}

#[derive(Args)]
struct CombinerArgs {
    path: String,
    #[command(flatten)]
    formats: FormatArgs,
    #[command(flatten)]
    action: ActionArgs,
    /// Compression to use when writing.
    ///
    /// Parquet accepts `uncompressed`, `snappy`, `gzip(LEVEL)`, `brotli(LEVEL)`, `lz4`,
    /// `zstd(LEVEL)`, or `lz4_raw`. Vortex accepts `standard` or `compact`.
    // The explicit conflict with `--show` is needed: clap drops a `requires` whose target
    // conflicts with a present argument, so `--compression --show` would otherwise parse.
    #[arg(
        long,
        value_name = "COMPRESSION",
        requires = "write",
        conflicts_with = "show"
    )]
    compression: Option<String>,
    /// Return at most ROWS combined rows. Defaults to 20 with --show and unlimited otherwise.
    #[arg(long, value_name = "ROWS")]
    limit: Option<usize>,
    /// Restrict the dataset's sample set to comma-separated sample ids.
    #[arg(long = "samples", value_delimiter = ',', value_name = "SAMPLE,...")]
    sample_set: Vec<String>,
}

#[derive(Args)]
struct FormatArgs {
    /// Format of the input tables.
    #[arg(long, value_enum, default_value = "vortex")]
    input_format: InputFormatArg,
    /// Format to write. Defaults to the input format.
    #[arg(long, value_enum)]
    output_format: Option<OutputFormatArg>,
}

impl FormatArgs {
    fn output_format(&self) -> OutputFormatArg {
        self.output_format
            .unwrap_or_else(|| self.input_format.default_output_format())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum InputFormatArg {
    Parquet,
    Vortex,
}

impl InputFormatArg {
    const fn format(self) -> InputFormat {
        match self {
            Self::Parquet => InputFormat::PARQUET,
            Self::Vortex => InputFormat::VORTEX,
        }
    }

    const fn default_output_format(self) -> OutputFormatArg {
        match self {
            Self::Parquet => OutputFormatArg::Parquet,
            Self::Vortex => OutputFormatArg::Vortex,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum OutputFormatArg {
    Parquet,
    Vortex,
}

impl OutputFormatArg {
    const fn format(self) -> OutputFormat {
        match self {
            Self::Parquet => OutputFormat::PARQUET,
            Self::Vortex => OutputFormat::VORTEX,
        }
    }
}

/// The action flags. At least one is required. `--explain` and `--explain-analyze` may combine
/// with `--write`, in which case they render or analyze the write's plan; every other pair
/// conflicts.
#[derive(Args)]
#[group(required = true, multiple = true)]
struct ActionArgs {
    /// Write the combined rows to PATH. With --explain or --explain-analyze, the plan shown is
    /// the write's, and --explain-analyze performs the write.
    #[arg(long, value_name = "PATH")]
    write: Option<String>,
    /// Print the combined rows.
    #[arg(long, conflicts_with_all = ["write", "explain", "explain_analyze"])]
    show: bool,
    /// Print the logical and physical plan without executing it.
    #[arg(long, conflicts_with = "explain_analyze")]
    explain: bool,
    /// Execute the plan and print it with per-operator metrics.
    #[arg(long)]
    explain_analyze: bool,
}

enum CliAction {
    Write(String),
    Show,
    Explain { write: Option<String> },
    ExplainAnalyze { write: Option<String> },
}

impl CliAction {
    fn row_limit(&self, explicit_limit: Option<usize>) -> Option<usize> {
        explicit_limit.or_else(|| matches!(self, Self::Show).then_some(DEFAULT_SHOW_LIMIT))
    }
}

impl TryFrom<ActionArgs> for CliAction {
    type Error = DataFusionError;

    fn try_from(args: ActionArgs) -> Result<Self> {
        let ActionArgs {
            write,
            show,
            explain,
            explain_analyze,
        } = args;
        match (write, show, explain, explain_analyze) {
            (Some(path), false, false, false) => Ok(Self::Write(path)),
            (None, true, false, false) => Ok(Self::Show),
            (write, false, true, false) => Ok(Self::Explain { write }),
            (write, false, false, true) => Ok(Self::ExplainAnalyze { write }),
            _ => Err(DataFusionError::Configuration(
                "an action is required: --write, --show, --explain, or --explain-analyze, where \
                 --explain and --explain-analyze may combine with --write"
                    .to_string(),
            )),
        }
    }
}

fn resolve(cli: Cli) -> Result<CombinerRun> {
    let Cli { command, threads } = cli;
    let threads = threads.unwrap_or_else(|| available_parallelism().map_or(1, NonZeroUsize::get));
    let (formulation, args) = match command {
        Command::CombineRefs(args) => (
            args.formulation.formulation(args.groups, threads)?,
            args.combiner,
        ),
        Command::CombineAlleles(args) => (Formulation::CombineAllelesUnion, args),
    };
    let CombinerArgs {
        path,
        formats,
        action,
        compression,
        limit,
        sample_set,
    } = args;
    let cli_action = CliAction::try_from(action)?;
    let input_format = formats.input_format.format();
    let limit = cli_action.row_limit(limit);
    let write_target = |output_path: String| -> Result<WriteTarget> {
        let output_format = formats.output_format().format();
        validate_output_extension(&output_path, &output_format)?;
        let output_format = match &compression {
            Some(compression) => output_format.with_compression(compression)?,
            None => output_format,
        };
        Ok(WriteTarget {
            output_path,
            output_format,
        })
    };
    let action = match cli_action {
        CliAction::Write(output_path) => Action::Write(write_target(output_path)?),
        CliAction::Show => Action::Collect,
        CliAction::Explain { write } => Action::Explain {
            write: write.map(write_target).transpose()?,
        },
        CliAction::ExplainAnalyze { write } => Action::ExplainAnalyze {
            write: write.map(write_target).transpose()?,
        },
    };
    let sample_set = (!sample_set.is_empty()).then_some(sample_set);
    Ok(CombinerRun {
        formulation,
        input_path: path,
        input_format,
        action,
        sample_set,
        row_limit: limit,
        threads,
    })
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let run = resolve(cli)?;
    println!("formulation: {}", run.formulation);
    let outcome = run.execute()?;
    println!("{}", outcome.render()?);
    Ok(())
}

fn validate_output_extension(output_path: &str, output_format: &OutputFormat) -> Result<()> {
    let Some(extension) = Path::new(output_path)
        .extension()
        .and_then(|ext| ext.to_str())
    else {
        return Ok(());
    };
    if extension != output_format.extension() {
        return Err(datafusion::error::DataFusionError::Configuration(format!(
            "output path '{output_path}' has extension '.{extension}', which contradicts output format '{output_format}'"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_action_is_required() {
        let error = Cli::try_parse_from(["datafusion-sandbox", "combine-refs", "input"])
            .err()
            .unwrap();
        let diagnostic = error.to_string();

        for action in ["--write", "--show", "--explain", "--explain-analyze"] {
            assert!(diagnostic.contains(action), "diagnostic:\n{diagnostic}");
        }
    }

    #[test]
    fn show_conflicts_with_every_other_action_and_explain_with_explain_analyze() {
        for pair in [
            ["--show", "--explain"],
            ["--show", "--explain-analyze"],
            ["--show", "--write=out.vortex"],
            ["--explain", "--explain-analyze"],
        ] {
            let error = Cli::try_parse_from(
                ["datafusion-sandbox", "combine-alleles", "input"]
                    .into_iter()
                    .chain(pair),
            )
            .err()
            .unwrap();
            let diagnostic = error.to_string();

            assert!(
                diagnostic.contains("cannot be used with"),
                "{pair:?} diagnostic:\n{diagnostic}"
            );
        }
    }

    #[test]
    fn explain_and_explain_analyze_may_combine_with_write() {
        let run = resolve(parse_combiner(["--explain", "--write", "out.vortex"])).unwrap();
        let Action::Explain {
            write: Some(target),
        } = run.action
        else {
            panic!("expected an explained write");
        };
        assert_eq!(target.output_path, "out.vortex");
        assert_eq!(run.row_limit, None);

        let run = resolve(parse_combiner([
            "--explain-analyze",
            "--write",
            "out.parquet",
            "--output-format",
            "parquet",
            "--compression",
            "snappy",
        ]))
        .unwrap();
        let Action::ExplainAnalyze {
            write: Some(target),
        } = run.action
        else {
            panic!("expected an analyzed write");
        };
        assert_eq!(target.output_path, "out.parquet");
        assert_eq!(target.output_format.extension(), "parquet");
    }

    #[test]
    fn explain_actions_without_a_write_resolve_to_no_write_target() {
        let run = resolve(parse_combiner(["--explain"])).unwrap();
        assert!(matches!(run.action, Action::Explain { write: None }));

        let run = resolve(parse_combiner(["--explain-analyze"])).unwrap();
        assert!(matches!(run.action, Action::ExplainAnalyze { write: None }));
    }

    #[test]
    fn compression_requires_a_write_action() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-refs",
            "input",
            "--compression",
            "snappy",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("--compression"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(diagnostic.contains("--write"), "diagnostic:\n{diagnostic}");
    }

    /// `--show` conflicts with `--compression` outright; the explain actions may take a write,
    /// so there the diagnostic names the missing `--write`.
    #[test]
    fn compression_still_requires_a_write_under_each_non_write_action() {
        for (action, expected) in [
            ("--show", "cannot be used with"),
            ("--explain", "--write"),
            ("--explain-analyze", "--write"),
        ] {
            let error = Cli::try_parse_from([
                "datafusion-sandbox",
                "combine-refs",
                "input",
                "--compression",
                "snappy",
                action,
            ])
            .err()
            .unwrap();
            let diagnostic = error.to_string();

            assert!(
                diagnostic.contains(expected),
                "{action} diagnostic:\n{diagnostic}"
            );
        }
    }

    #[test]
    fn allele_combiner_rejects_a_groups_argument() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-alleles",
            "input",
            "--groups",
            "2",
            "--show",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(diagnostic.contains("--groups"), "diagnostic:\n{diagnostic}");
        assert!(
            diagnostic.contains("unexpected argument"),
            "diagnostic:\n{diagnostic}"
        );
    }

    #[test]
    fn groups_default_to_the_thread_count_under_grouped_merge() {
        let run = resolve(
            Cli::try_parse_from([
                "datafusion-sandbox",
                "--threads",
                "3",
                "combine-refs",
                "input",
                "--formulation",
                "grouped-merge",
                "--show",
            ])
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            run.formulation,
            Formulation::CombineRefsGroupedMerge {
                groups: NonZeroUsize::new(3).unwrap()
            }
        );
        assert_eq!(run.threads, 3);
    }

    #[test]
    fn an_explicit_group_count_overrides_the_thread_count() {
        let run = resolve(parse_combiner([
            "--formulation",
            "grouped-merge",
            "--groups",
            "5",
            "--show",
        ]))
        .unwrap();

        assert_eq!(
            run.formulation,
            Formulation::CombineRefsGroupedMerge {
                groups: NonZeroUsize::new(5).unwrap()
            }
        );
    }

    #[test]
    fn groups_are_rejected_with_the_union_formulation() {
        let error = resolve(parse_combiner(["--groups", "2", "--show"]))
            .err()
            .unwrap();
        let diagnostic = error.to_string();

        assert!(diagnostic.contains("--groups"), "diagnostic:\n{diagnostic}");
        assert!(
            diagnostic.contains("grouped-merge"),
            "diagnostic:\n{diagnostic}"
        );
    }

    #[test]
    fn a_group_count_of_zero_is_rejected() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-refs",
            "input",
            "--formulation",
            "grouped-merge",
            "--groups",
            "0",
            "--show",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(diagnostic.contains("--groups"), "diagnostic:\n{diagnostic}");
    }

    #[test]
    fn allele_combiner_rejects_a_formulation_argument() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-alleles",
            "input",
            "--formulation",
            "union",
            "--show",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("--formulation"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(
            diagnostic.contains("unexpected argument"),
            "diagnostic:\n{diagnostic}"
        );
    }

    #[test]
    fn reference_combiner_names_accepted_formulations_when_rejecting_a_removed_one() {
        let error = Cli::try_parse_from([
            "datafusion-sandbox",
            "combine-refs",
            "input",
            "--formulation",
            "one-scan",
            "--show",
        ])
        .err()
        .unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("invalid value 'one-scan'"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(
            diagnostic.contains("possible values: union, grouped-merge"),
            "diagnostic:\n{diagnostic}"
        );
    }

    #[test]
    fn output_extension_must_match_the_resolved_output_format() {
        let cli = parse_combiner(["--input-format", "parquet", "--write", "output.vortex"]);

        let error = resolve(cli).err().unwrap();
        let diagnostic = error.to_string();

        assert!(
            diagnostic.contains("output.vortex"),
            "diagnostic:\n{diagnostic}"
        );
        assert!(diagnostic.contains("parquet"), "diagnostic:\n{diagnostic}");
    }

    #[test]
    fn output_format_must_accept_the_compression_value() {
        let cli = parse_combiner([
            "--output-format",
            "vortex",
            "--compression",
            "zstd(3)",
            "--write",
            "output.vortex",
        ]);

        let error = resolve(cli).err().unwrap();
        let diagnostic = error.to_string();

        assert!(diagnostic.contains("zstd(3)"), "diagnostic:\n{diagnostic}");
        assert!(diagnostic.contains("vortex"), "diagnostic:\n{diagnostic}");
    }

    #[test]
    fn show_resolves_to_collecting_twenty_rows() {
        let run = resolve(parse_combiner(["--show"])).unwrap();

        assert!(matches!(run.action, Action::Collect));
        assert_eq!(run.row_limit, Some(20));
    }

    #[test]
    fn an_explicit_show_limit_overrides_the_default() {
        let run = resolve(parse_combiner(["--show", "--limit", "7"])).unwrap();

        assert!(matches!(run.action, Action::Collect));
        assert_eq!(run.row_limit, Some(7));
    }

    #[test]
    fn comma_separated_sample_ids_resolve_to_a_sample_set() {
        let run = resolve(parse_combiner(["--samples", "HG00308,HG00309", "--show"])).unwrap();

        assert_eq!(
            run.sample_set,
            Some(vec!["HG00308".to_string(), "HG00309".to_string()])
        );
    }

    fn parse_combiner<const N: usize>(args: [&str; N]) -> Cli {
        Cli::try_parse_from(
            [
                "datafusion-sandbox",
                "--threads",
                "1",
                "combine-refs",
                "input",
            ]
            .into_iter()
            .chain(args),
        )
        .unwrap()
    }

    #[test]
    fn rejects_a_thread_count_of_zero() {
        assert_eq!(parse_thread_count("1"), Ok(1));
        assert_eq!(parse_thread_count("8"), Ok(8));

        let err = parse_thread_count("0").unwrap_err();
        assert!(err.contains("at least 1"), "got: {err}");

        parse_thread_count("banana").expect_err("non-numeric thread counts must be rejected");
    }
}
