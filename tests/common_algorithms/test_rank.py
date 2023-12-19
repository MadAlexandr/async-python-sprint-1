import pytest

from common_algorithms.rank import get_rank


@pytest.mark.parametrize(
    'seq, expected_rank',
    [
        ([1, 2, 3, 4, 5], (1, 2, 3, 4, 5)),
        ([1, 2, 2, 4, 5], (1, 2, 2, 4, 5)),
        ([4, 2, 2, 1, 7], (4, 2, 2, 1, 5)),
        ([4, 2, 2, 4, 7], (3, 1, 1, 3, 5)),
    ]
)
def test_get_rank(seq, expected_rank):
    assert get_rank(seq) == expected_rank
