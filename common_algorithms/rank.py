from typing import Sequence, Protocol, Hashable, Any


class Comparable(Hashable, Protocol):
    def __lt__(self, other: Any) -> bool:
        ...


def get_rank(seq: Sequence[Comparable]) -> tuple[Comparable, ...]:
    sorted_seq = sorted(seq, reverse=True)
    ranks = {}
    prev_value = None
    for i, x in enumerate(sorted_seq, 1):
        if x == prev_value:
            continue

        curr_rank = i
        ranks[x] = curr_rank
        prev_value = x

    return tuple(ranks[x] for x in seq)
