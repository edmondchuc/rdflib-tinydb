from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest
from rdflib import Graph, ConjunctiveGraph, Dataset


@pytest.fixture(scope="function")
def get_json_storage_graph():
    # TODO: Change to ConjunctiveGraph.
    g = Graph("TinyDB")
    cwd = Path.cwd()
    print(cwd)
    db_file = cwd / "test.json"
    g.open(db_file)
    assert db_file.is_file()

    yield g

    # Clean up
    db_file.unlink()
    assert not db_file.is_file()


@pytest.fixture(scope="function")
def get_memory_storage_graph():
    # TODO: Change to ConjunctiveGraph.
    g = Graph("TinyDBMemory")
    g.open(None)

    yield g

    g.close()


@pytest.fixture(scope="function")
def input_data():
    return """
        <https://example.com/person-1> a <https://schema.org/Person>, <https://schema.org/Thing> ;
            <https://schema.org/name> "Person 1" ;
            <https://schema.org/jobTitle> "software engineer" ;
            <https://schema.org/affiliation> [
                a <https://schema.org/Organization> ;
                <https://schema.org/name> "RDFLib" ;
            ] .

        <https://example.com/person-2> a <https://schema.org/Person>, <https://schema.org/Thing> ;
            <https://schema.org/name> "Person 2" ;
            <https://schema.org/jobTitle> "ontologist"@en ;
            <https://schema.org/affiliation> [
               a <https://schema.org/Organization> ;
               <https://schema.org/name> "W3C"^^<http://www.w3.org/2001/XMLSchema#string> ; 
            ] .
    """


@pytest.mark.parametrize("g", ["get_json_storage_graph", "get_memory_storage_graph"])
def test_json_store_with_context(g: str, input_data, request: FixtureRequest):
    g: ConjunctiveGraph = request.getfixturevalue(g)
    g.parse(data=input_data)
    print(len(g))
    assert len(g) > 0
