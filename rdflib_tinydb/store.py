from abc import ABC, abstractmethod
from typing import Union, Dict, Optional, Tuple

from rdflib import URIRef, BNode, Literal, store, Graph
from rdflib.store import Store
from tinydb import TinyDB, Query
from tinydb.table import Table, Document
from tinydb.storages import MemoryStorage


class _BaseTinyDBStore(Store, ABC):
    # Base store settings.
    context_aware: bool = False
    formula_aware: bool = False
    transaction_aware: bool = False
    graph_aware: bool = False

    # TinyDB store
    _store: TinyDB = None
    _spo: Table = None
    _pos: Table = None
    _osp: Table = None

    # Prefixes and namespaces
    _namespace: dict
    _prefix: dict

    def __init__(self, configuration: str = None, identifier: str = None):
        """Create a TinyDB-backed RDFLib store.

        :param configuration: Path to JSON db file and use JSONStorage. If None, use MemoryStorage.
        :param identifier: URIRef of the Store. Defaults to CWD
        """
        # TODO: namespace and prefix should be saved in the store.
        self.__namespace = {}
        self.__prefix = {}
        super(_BaseTinyDBStore, self).__init__(configuration, identifier)

    @abstractmethod
    def open(self, configuration, create=False):
        """Create TinyDB database with indices tables."""
        pass

    def close(self, commit_pending_transaction: bool = False):
        if self._store is not None:
            del self._store

    def gc(self):
        pass

    def destroy(self, configuration: Union[str, None]):
        """Delete the TinyDB database file."""
        raise NotImplementedError("TinyDBStore.destroy() is not implemented yet.")

    def add(
        self,
        triple: Tuple[Union[URIRef, BNode], URIRef, Union[URIRef, BNode, Literal]],
        context: Union[str, URIRef, Graph],
        quoted: bool = False,
    ):
        if quoted:
            raise ValueError("TinyDBStore is not formula-aware.")

        # TODO: Make store context-aware.

        s, p, o = triple

        s_type = _convert_to_store_term(s)
        p_type = _convert_to_store_term(p)
        o_type = _convert_to_store_term(o)

        # Only add statement to store if it does not already exist.
        statement_exists = self._spo.contains(
            (Query().s == _convert_to_store_term(s))
            & (Query().p == _convert_to_store_term(p))
            & (Query().o == _convert_to_store_term(o))
        )
        if not statement_exists:
            self._spo.insert(
                {str(s): {str(p): str(o)}, "s": s_type, "p": p_type, "o": o_type}
            )
            self._pos.insert(
                {str(p): {str(o): str(s)}, "s": s_type, "p": p_type, "o": o_type}
            )
            self._osp.insert(
                {str(o): {str(s): str(p)}, "s": s_type, "p": p_type, "o": o_type}
            )

            super(_BaseTinyDBStore, self).add(triple, context)

    def remove(self, triple, context=None):
        raise NotImplementedError()

    def triples(
        self,
        triple_pattern: Tuple[
            Union[URIRef, BNode, None],
            Union[URIRef, None],
            Union[URIRef, BNode, Literal, None],
        ],
        context=None,
    ):
        s, p, o = triple_pattern

        if s is not None:
            if p is not None:
                # s, p, o
                if o is not None:
                    documents = self._spo.search(Query()[str(s)][str(p)] == str(o))
                    for document in documents:
                        yield _convert_document_to_rdflib_result(document)
                # s, p, None
                else:
                    documents = self._spo.search(Query()[str(s)][str(p)].matches(".*"))
                    for document in documents:
                        yield _convert_document_to_rdflib_result(document)
            else:
                # s, None, o
                if o is not None:
                    documents = self._osp.search(Query()[str(o)][str(s)].matches(".*"))
                    for document in documents:
                        yield _convert_document_to_rdflib_result(document)
                # s, None, None
                else:
                    documents = self._spo.search(Query()[str(s)].all([]))
                    for document in documents:
                        yield _convert_document_to_rdflib_result(document)
        else:
            if p is not None:
                # None, p, o
                if o is not None:
                    documents = self._pos.search(Query()[str(p)][str(o)].matches(".*"))
                    for document in documents:
                        yield _convert_document_to_rdflib_result(document)
                # None, p, None
                else:
                    documents = self._pos.search(Query()[str(p)].all([]))
                    for document in documents:
                        yield _convert_document_to_rdflib_result(document)
            else:
                # None, None, o
                if o is not None:
                    documents = self._osp.search(Query()[str(o)].all([]))
                    for document in documents:
                        yield _convert_document_to_rdflib_result(document)
                # None, None, None
                else:
                    documents = self._spo.all()
                    for document in documents:
                        yield _convert_document_to_rdflib_result(document)

    def __len__(self, context: str = None):
        return len(self._spo)

    def query(self, query, initNs, initBindings, queryGraph, **kwargs):
        super(_BaseTinyDBStore, self).query(
            query, initNs, initBindings, queryGraph, **kwargs
        )

    def update(self, update, initNs, initBindings, queryGraph, **kwargs):
        super(_BaseTinyDBStore, self).update(
            update, initNs, initBindings, queryGraph, **kwargs
        )

    def bind(self, prefix, namespace):
        self.__prefix[namespace] = prefix
        self.__namespace[prefix] = namespace

    def namespace(self, prefix):
        return self.__namespace.get(prefix, None)

    def prefix(self, namespace):
        return self.__prefix.get(namespace, None)

    def namespaces(self):
        for prefix, namespace in self.__namespace.items():
            yield prefix, namespace


class TinyDBStore(_BaseTinyDBStore):
    def open(self, configuration: str, create: bool = False) -> Optional[int]:
        if configuration is None:
            raise ValueError("TinyDB store must have a configuration string.")
        self._store = TinyDB(configuration)
        self._spo = self._store.table("spo")
        self._pos = self._store.table("pos")
        self._osp = self._store.table("osp")
        return store.VALID_STORE


class TinyDBMemoryStore(_BaseTinyDBStore):
    def open(self, configuration: Optional[str], create: bool = False) -> Optional[int]:
        self._store = TinyDB(storage=MemoryStorage)
        self._spo = self._store.table("spo")
        self._pos = self._store.table("pos")
        self._osp = self._store.table("osp")
        return store.VALID_STORE


def _convert_to_store_term(node: Union[URIRef, BNode, Literal]) -> Dict:
    term = {}

    if isinstance(node, URIRef):
        term["type"] = "URIRef"
        term["value"] = str(node)
    elif isinstance(node, BNode):
        term["type"] = "BNode"
        term["value"] = str(node)
    elif isinstance(node, Literal):
        term["type"] = "Literal"
        term["datatype"] = str(node.datatype) if node.datatype else ""
        term["lang"] = node.language if node.language else ""
        term["value"] = str(node.value)
    else:
        raise ValueError(f'Expected URIRef, BNode or Literal. Got "{type(node)}".')
    return term


def _convert_to_rdflib_term(node: dict) -> Union[URIRef, BNode, Literal]:
    if node["type"] == "URIRef":
        return URIRef(node["value"])
    elif node["type"] == "BNode":
        return BNode(node["value"])
    elif node["type"] == "Literal":
        return Literal(
            node["value"],
            datatype=node["datatype"] if node["datatype"] else None,
            lang=node["lang"] if node["lang"] else None,
        )
    else:
        raise ValueError(f'Unexpected store term type: "{node["type"]}".')


def _convert_document_to_rdflib_result(document: Document, context=None):
    return (
        _convert_to_rdflib_term(document["s"]),
        _convert_to_rdflib_term(document["p"]),
        _convert_to_rdflib_term(document["o"]),
    ), context
