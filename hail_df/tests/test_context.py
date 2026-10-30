import hail_df
import pytest
from conftest import write_mixed_dataset
from datafusion import SessionContext


# On disk, as every `hail_df` dataset fixture is: see conftest.py.
@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    path = write_mixed_dataset(tmp_path_factory.mktemp("context"), "parquet")
    return hail_df.Backend().open_dataset(str(path), "parquet", "locus")


def test_a_plan_over_scans_encodes_in_a_hail_df_context(dataset):
    ctx = hail_df.context()

    assert dataset.read(ctx).logical_plan().to_bytes(ctx)


def test_a_plan_over_scans_does_not_encode_without_the_backend_s_codec(dataset):
    ctx = SessionContext()

    with pytest.raises(Exception, match="(?i)codec|provider"):
        dataset.read(ctx).logical_plan().to_bytes(ctx)


def test_each_context_is_a_fresh_session(dataset):
    first, second = hail_df.context(), hail_df.context()

    assert first.session_id() != second.session_id()
