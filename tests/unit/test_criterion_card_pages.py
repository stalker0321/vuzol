"""Criterion wording is split so a status card stays inside Telegram's text limit."""

from vuzol.telegram.projections import TELEGRAM_TEXT_LIMIT, _paginate_criterion_lines


def _html(head: tuple[str, ...], page: tuple[tuple[int, str], ...], tail: tuple[str, ...]) -> str:
    return "\n".join((*head, *(line for _index, line in page), *tail))


def test_many_long_criteria_are_split_under_the_telegram_limit() -> None:
    head = ("<b>Задача</b>", "Статус: остановлена")
    entries = tuple((f"c{index}", "y" * 800) for index in range(5))
    suffix = ("Каждая кнопка подтверждает только один критерий.",)
    pages = _paginate_criterion_lines(
        head=head, entries=entries, suffix=suffix, limit=TELEGRAM_TEXT_LIMIT
    )
    assert len(pages) >= 2
    seen: list[int] = []
    for number, page in enumerate(pages, start=1):
        tail = suffix
        if len(pages) > 1:
            tail = (*suffix, f"Страница {number}/{len(pages)}")
        assert len(_html(head, page, tail)) <= TELEGRAM_TEXT_LIMIT
        seen.extend(index for index, _line in page)
        assert all("y" * 800 in line for _index, line in page)
    assert seen == [0, 1, 2, 3, 4]


def test_one_oversized_criterion_is_cut_without_passing_the_limit() -> None:
    head = ("title",)
    pages = _paginate_criterion_lines(
        head=head,
        entries=(("only", "q" * 5000),),
        suffix=(),
        limit=TELEGRAM_TEXT_LIMIT,
    )
    assert len(pages) == 1
    line = pages[0][0][1]
    assert len(_html(head, pages[0], ())) <= TELEGRAM_TEXT_LIMIT
    assert "only" in line
    assert line.endswith("...")


def test_short_criteria_stay_on_one_page() -> None:
    pages = _paginate_criterion_lines(
        head=("title",),
        entries=(("a", "saved"), ("b", "logs")),
        suffix=("one each",),
        limit=TELEGRAM_TEXT_LIMIT,
    )
    assert len(pages) == 1
    assert [index for index, _line in pages[0]] == [0, 1]
