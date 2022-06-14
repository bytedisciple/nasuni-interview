import pytest
from textsearcher import TextSearcher

small_file = "../files/Siddhartha.opening.txt"
large_file = "../files/Siddhartha.txt"

def test_instantiate(searcher):
    assert isinstance(searcher, TextSearcher)

@pytest.mark.parametrize("file_path,should_succeed", [
    ("dont.exist", False),
    ("../Readme.md", True),
    (None, False),
    ("../files/Siddhartha.opening.txt", True),
    ("../files/Siddhartha.txt", True)
])
def test_load_file(searcher, file_path, should_succeed):
    assert searcher.load(file_path) == should_succeed

@pytest.mark.parametrize("word,expected_result", [
    ("riverbank", ["riverbank"]),
    ("potato", []),
])
def test_matches_single_occurance(searcher, word, expected_result):
    assert searcher.load(small_file)
    result = searcher.search(word)
    assert type(result) is list
    assert result == expected_result


@pytest.mark.parametrize("word,expected_result", [
    ("shade", ["shade", "shade", "shade", "shade"]),
])
def test_matches_multiple_occurance(searcher, word, expected_result):
    assert searcher.load(small_file)
    result = searcher.search(word)
    assert type(result) is list
    assert result == expected_result

@pytest.mark.parametrize("word,context,expected_result", [
    ("universe.", 0, ["universe."]),
    ("evening's", 0, ["evening's"]),
    ("Sal-wood", 3, ["shade of the Sal-wood forest, in the"]),
])
def test_matches_punctuation(searcher, word, context, expected_result):
    assert searcher.load(large_file)
    result = searcher.search(word, context)
    assert type(result) is list
    assert result == expected_result

@pytest.mark.parametrize("word,context,expected_result", [
    ("Atman", 2, ["to feel Atman in the"]),
    ("black", 8, ["In the mango grove, shade poured into his black eyes, when playing as a boy, when his"]),
])
def test_matches_with_context(searcher, word, context, expected_result):
    assert searcher.load(small_file)
    result = searcher.search(word, context)
    assert type(result) is list
    assert result == expected_result

@pytest.mark.parametrize("word,context,expected_result", [
    ("evening's", 1, ["the evening's ablution."]),
    ("Early", 1, [
        "Govinda: \"Early tomorrow",
        "Very early in",
        "and early in",
        "an early pre-birth",
        "sleeping. Early in",
    ])
])
def test_matches_quotes_correctly(searcher, word, context, expected_result):
    assert searcher.load(large_file)
    result = searcher.search(word, context)
    assert type(result) is list
    assert result == expected_result

@pytest.mark.parametrize("word,context,expected_result", [
    ("90", 1, ["within 90 days", "within 90 days"])
])
def test_matches_numbers_correctly(searcher, word, context, expected_result):
    assert searcher.load(large_file)
    result = searcher.search(word, context)
    assert type(result) is list
    assert result == expected_result

@pytest.mark.parametrize("word,context,expected_result", [
    ("2500-8.txt", 1, ["named 2500-8.txt or"])
])
def test_matches_alphanumeric_correctly(searcher, word, context, expected_result):
    assert searcher.load(large_file)
    result = searcher.search(word, context)
    assert type(result) is list
    assert result == expected_result

@pytest.mark.parametrize("word,context,expected_result", [
    ("shade", 4, [
        "In the shade of the house, in",
        "the boats, in the shade of the Sal-wood forest,",
        "Sal-wood forest, in the shade of the fig tree",
        "In the mango grove, shade poured into his black",
    ])
])
def test_matches_beginning_of_file(searcher, word, context, expected_result):
    assert searcher.load(small_file)
    result = searcher.search(word, context)
    assert type(result) is list
    assert result == expected_result

@pytest.mark.parametrize("word,context,expected_result", [
    ("universe", 3, ["one with the universe."]),
])
def test_matches_end_of_file(searcher, word, context, expected_result):
    assert searcher.load(small_file)
    result = searcher.search(word, context)
    assert type(result) is list
    assert result == expected_result


@pytest.mark.parametrize("word,expected_count", [
    ("in", 7),
    ("In", 7),
    ("IN", 7),
    ("iN", 7),
])
def test_matches_are_case_insenstive(searcher, word, expected_count):
    assert searcher.load(small_file)
    result = searcher.search(word)
    assert len(result) == expected_count

def test_multiple_searches_on_one_file(searcher):
    assert searcher.load(small_file)
    result = searcher.search("shade", 4)
    expected = [
        "In the shade of the house, in",
        "the boats, in the shade of the Sal-wood forest,",
        "Sal-wood forest, in the shade of the fig tree",
        "In the mango grove, shade poured into his black"
    ]
    assert result == expected

    result = searcher.search("universe", 3)
    expected = [
        "one with the universe."
    ]
    assert result == expected

@pytest.mark.parametrize("word,context,expected_result", [
    ("universe", 3, ["one with the universe."]),
])
def test_matches_overlapping_results(searcher, word, context, expected_result):
    assert searcher.load(small_file)
    result = searcher.search(word, context)
    assert type(result) is list
    assert result == expected_result
