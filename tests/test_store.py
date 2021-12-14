from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest
from rdflib import Graph, Namespace, URIRef, BNode, Literal, RDF
from tinydb import TinyDB


SDO = Namespace("https://schema.org/")


@pytest.fixture(scope="function")
def get_json_storage_graph():
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


def query_graph(g: Graph, input_data: str):
    spo = g.store._store.table("spo")
    assert len(spo) > 0

    person_1 = URIRef("https://example.com/person-1")
    person_2 = URIRef("https://example.com/person-2")

    # Test triple matching with (s, p, o).
    statement = (person_1, RDF.type, SDO.Person)
    statement_found = False
    for s, p, o in g.triples(statement):
        assert (s, p, o) == statement
        statement_found = True
    assert statement_found

    # Test triple matching with (s, p, None).
    statement = (person_1, RDF.type, None)
    statement_found = False
    for s, p, o in g.triples(statement):
        assert s == statement[0]
        assert p == statement[1]
        statement_found = True
    assert statement_found

    # Test triple matching with (s, None, o).
    statement = (
        person_1,
        None,
        Literal("software engineer"),
    )
    statement_found = False
    for s, p, o in g.triples(statement):
        assert s == statement[0]
        assert o == statement[2]
        assert p == SDO.jobTitle
        statement_found = True
    assert statement_found

    # Test triple matching with (s, None, None).
    statement = (person_1, None, None)
    statement_found = False
    for s, p, o in g.triples(statement):
        assert s == statement[0]
        statement_found = True
    assert statement_found

    # Test triple matching with (None, p, o).
    statement = (None, RDF.type, SDO.Person)
    statement_found = False
    for s, p, o in g.triples(statement):
        assert s == person_1 or s == person_2
        assert p == statement[1]
        assert o == statement[2]
        statement_found = True
    assert statement_found

    # Test triple matching with (None, p, None)
    statement = (None, RDF.type, None)
    statement_found = False
    expected_statements = [
        (person_1, RDF.type, SDO.Person),
        (person_1, RDF.type, SDO.Thing),
        (person_2, RDF.type, SDO.Person),
        (person_2, RDF.type, SDO.Thing),
        (None, RDF.type, SDO.Organization),
    ]
    for s, p, o in g.triples(statement):
        if not isinstance(s, BNode):
            assert (s, p, o) in expected_statements
        else:
            # s is a BNode, test only for existence of p and o.
            assert (p, o) in [(x[1], x[2]) for x in expected_statements]
        statement_found = True
    assert statement_found

    # Test triple matching with (None, None, o).
    statement = (None, None, SDO.Thing)
    statement_found = False
    expected_statements = [
        (person_1, RDF.type, SDO.Thing),
        (person_2, RDF.type, SDO.Thing),
    ]
    for s, p, o in g.triples(statement):
        assert (s, p, o) in expected_statements
        statement_found = True
    assert statement_found

    # Test triple matching with (None, None, None)
    statement = (None, None, None)
    statement_found = False
    expected_statements = [
        (person_1, RDF.type, SDO.Person),
        (person_1, RDF.type, SDO.Thing),
        (person_1, SDO.name, Literal("Person 1")),
        (person_1, SDO.jobTitle, Literal("software engineer")),
        (person_1, SDO.affiliation, None),
        (person_2, RDF.type, SDO.Person),
        (person_2, RDF.type, SDO.Thing),
        (person_2, SDO.name, Literal("Person 2")),
        (person_2, SDO.jobTitle, Literal("ontologist", lang="en")),
        (person_2, SDO.affiliation, None),
        (None, RDF.type, SDO.Organization),
        (None, SDO.name, Literal("RDFLib")),
        (
            None,
            SDO.name,
            Literal("W3C", datatype=URIRef("http://www.w3.org/2001/XMLSchema#string")),
        ),
    ]
    for s, p, o in g.triples(statement):
        if not isinstance(s, BNode):
            if isinstance(o, BNode):
                # s is not a BNode. o is a BNode.
                assert (s, p) in [(x[0], x[1]) for x in expected_statements]
            else:
                # s is not a BNode. o is not a BNode.
                assert (s, p, o) in expected_statements
        else:
            # s is a BNode, test only for existence of p and o.
            assert (p, o) in [(x[1], x[2]) for x in expected_statements]
        statement_found = True
    assert statement_found

    # Test Store.add() where quoted is True.
    with pytest.raises(ValueError):
        g.store.add(statement, g.identifier, True)

    # Test adding a str as the subject node to Store.add() results in a
    # value error in _convert_to_store_term().
    with pytest.raises(ValueError):
        g.store.add(
            ("https://example.com/person-1", RDF.type, SDO.Person), g.identifier
        )

    # Test that Graph.triples() returns RDFLib values URIRef, BNode and Literal.
    graph_triples_found = False
    for s, p, o in g.triples((None, None, None)):
        assert isinstance(s, URIRef) or isinstance(s, BNode)
        assert isinstance(p, URIRef)
        assert isinstance(o, URIRef) or isinstance(o, BNode) or isinstance(o, Literal)
        graph_triples_found = True
    assert graph_triples_found

    # Ensure the reference implementation (Memory store) has the same number of triples
    # as the TinyDB-backed store.
    triples_in_tinydb_store = len(g)
    assert triples_in_tinydb_store > 0
    memory_graph = Graph()
    memory_graph.parse(data=input_data)
    assert len(memory_graph) == triples_in_tinydb_store

    # Store.add() must be idempotent.
    triples_count = len(g)
    statement = (URIRef("https://example.com/resource-1"), RDF.type, SDO.Thing)
    g.add(statement)
    g.add(statement)
    assert len(g) == triples_count + 1

    # Test Store.close().
    assert isinstance(g.store._store, TinyDB)
    g.close()
    assert g.store._store is None

    # Test Store.destroy().
    with pytest.raises(NotImplementedError):
        g.destroy(None)


@pytest.mark.parametrize("g", ["get_json_storage_graph", "get_memory_storage_graph"])
def test_store(g: str, input_data: str, request: FixtureRequest):
    g: Graph = request.getfixturevalue(g)
    g.parse(data=input_data)
    assert len(g) > 0
    # TODO: Once the tests within query_graph() have been pulled out into
    #  separate functions, remove this.
    query_graph(g, input_data)


def test_json_store_without_providing_configuration():
    g = Graph(store="TinyDB")

    # TinyDB store must have a configuration value.
    with pytest.raises(ValueError):
        g.open(None)

    # TinyDB store throws OSError when an invalid configuration value is given.
    with pytest.raises(OSError):
        g.open(123)
