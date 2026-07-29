import glob
import os
import pytest
import tarfile


def _darshan_available():
    """Whether the [darshan] extra is present *and* usable.

    `Exception`, not `ImportError`: pydarshan raises a plain RuntimeError when
    it cannot find libdarshan-util.so, so `importorskip` would not catch it.
    That is the case on Python 3.13, where the extra resolves to nothing --
    pydarshan publishes no cp313 wheel. See the marker in pyproject.toml.
    """
    try:
        import darshan  # noqa: F401
    except Exception:
        return False
    return True


# Lives here rather than in one test module so that every darshan-dependent
# test opts in the same way. The e2e cases in test_main.py were parametrized
# with a darshan analyzer and no guard, which passed for as long as every
# supported Python could install the extra, and started failing the moment one
# could not.
requires_darshan = pytest.mark.skipif(
    not _darshan_available(), reason='requires a working [darshan] extra'
)


@pytest.fixture(scope='session', autouse=True)
def extract_test_data():
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    tar_files = glob.glob(os.path.join(data_dir, '*.tar.gz'))

    for tar_path in tar_files:
        tar_name = os.path.basename(tar_path)
        extract_folder_name = tar_name.replace('.tar.gz', '')
        extract_path = os.path.join(data_dir, 'extracted', extract_folder_name)

        if not os.path.exists(extract_path):
            os.makedirs(extract_path)

        if not any(os.scandir(extract_path)):
            with tarfile.open(tar_path, 'r:gz') as tar:
                tar.extractall(path=extract_path)
