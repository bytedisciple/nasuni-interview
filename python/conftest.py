import pytest
from textsearcher import TextSearcher

@pytest.fixture()
def searcher():
    return TextSearcher() 
