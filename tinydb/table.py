"""
This module implements tables, the central place for accessing and manipulating
data in TinyDB.
"""
from typing import Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Union, cast, Tuple
from .queries import QueryLike
from .storages import Storage
from .utils import LRUCache
__all__ = ('Document', 'Table')

class Document(dict):
    """
    A document stored in the database.

    This class provides a way to access both a document's content and
    its ID using ``doc.doc_id``.
    """

    def __init__(self, value: Mapping, doc_id: int):
        super().__init__(value)
        self.doc_id = doc_id

class Table:
    """
    Represents a single TinyDB table.

    It provides methods for accessing and manipulating documents.

    .. admonition:: Query Cache

        As an optimization, a query cache is implemented using a
        :class:`~tinydb.utils.LRUCache`. This class mimics the interface of
        a normal ``dict``, but starts to remove the least-recently used entries
        once a threshold is reached.

        The query cache is updated on every search operation. When writing
        data, the whole cache is discarded as the query results may have
        changed.

    .. admonition:: Customization

        For customization, the following class variables can be set:

        - ``document_class`` defines the class that is used to represent
          documents,
        - ``document_id_class`` defines the class that is used to represent
          document IDs,
        - ``query_cache_class`` defines the class that is used for the query
          cache
        - ``default_query_cache_capacity`` defines the default capacity of
          the query cache

        .. versionadded:: 4.0


    :param storage: The storage instance to use for this table
    :param name: The table name
    :param cache_size: Maximum capacity of query cache
    """
    document_class = Document
    document_id_class = int
    query_cache_class = LRUCache
    default_query_cache_capacity = 10

    def __init__(self, storage: Storage, name: str, cache_size: int=default_query_cache_capacity):
        """
        Create a table instance.
        """
        self._storage = storage
        self._name = name
        self._query_cache: LRUCache[QueryLike, List[Document]] = self.query_cache_class(capacity=cache_size)
        self._next_id = None

    def __repr__(self):
        args = ['name={!r}'.format(self.name), 'total={}'.format(len(self)), 'storage={}'.format(self._storage)]
        return '<{} {}>'.format(type(self).__name__, ', '.join(args))

    @property
    def name(self) -> str:
        """
        Get the table name.
        """
        return self._name

    @property
    def storage(self) -> Storage:
        """
        Get the table storage instance.
        """
        return self._storage

    def insert(self, document: Mapping) -> int:
        """
        Insert a new document into the table.

        :param document: the document to insert
        :returns: the inserted document's ID
        """
        if not isinstance(document, Mapping):
            raise ValueError('Document is not a Mapping')

        if isinstance(document, Document):
            doc_id = document.doc_id
            document = dict(document)
        else:
            doc_id = self._get_next_id()

        data = dict(document)
        final_doc_id = doc_id

        def updater(table: Dict[int, Mapping]):
            nonlocal final_doc_id
            if final_doc_id in table:
                if isinstance(document, Document):
                    raise ValueError('Document ID already exists')
                else:
                    final_doc_id = self._get_next_id()
            table[final_doc_id] = data

        self._update_table(updater)
        self._query_cache.clear()

        return final_doc_id

    def insert_multiple(self, documents: Iterable[Mapping]) -> List[int]:
        """
        Insert multiple documents into the table.

        :param documents: an Iterable of documents to insert
        :returns: a list containing the inserted documents' IDs
        """
        doc_ids = []
        data = []
        documents = list(documents)

        if len(documents) == 1 and not isinstance(documents[0], Mapping):
            raise ValueError('Document is not a Mapping')

        for doc in documents:
            if not isinstance(doc, Mapping):
                raise ValueError('Document is not a Mapping')

            if isinstance(doc, Document):
                doc_id = doc.doc_id
                doc = dict(doc)
            else:
                doc_id = self._get_next_id()
            doc_ids.append(doc_id)
            data.append((doc_id, dict(doc)))

        final_doc_ids = doc_ids.copy()

        def updater(table: Dict[int, Mapping]):
            nonlocal final_doc_ids
            for i, (doc_id, doc) in enumerate(data):
                if doc_id in table:
                    if isinstance(documents[i], Document):
                        raise ValueError('Document ID already exists')
                    else:
                        new_id = self._get_next_id()
                        final_doc_ids[i] = new_id
                        table[new_id] = doc
                else:
                    table[doc_id] = doc

        self._update_table(updater)
        self._query_cache.clear()

        return final_doc_ids

    def all(self) -> List[Document]:
        """
        Get all documents stored in the table.

        :returns: a list with all documents.
        """
        table = self._read_table()
        return [self.document_class(doc, self.document_id_class(doc_id))
                for doc_id, doc in table.items()]

    def search(self, cond: QueryLike) -> List[Document]:
        """
        Search for all documents matching a 'where' cond.

        :param cond: the condition to check against
        :returns: list of matching documents
        """
        if cond in self._query_cache:
            return self._query_cache[cond]

        docs = [doc for doc in self.all() if cond(doc)]
        self._query_cache[cond] = docs

        return docs

    def get(self, cond: Optional[QueryLike]=None, doc_id: Optional[int]=None, doc_ids: Optional[List]=None) -> Optional[Union[Document, List[Document]]]:
        """
        Get exactly one document specified by a query or a document ID.
        However, if multiple document IDs are given then returns all
        documents in a list.
        
        Returns ``None`` if the document doesn't exist.

        :param cond: the condition to check against
        :param doc_id: the document's ID
        :param doc_ids: the document's IDs(multiple)

        :returns: the document(s) or ``None``
        """
        if cond is None and doc_id is None and doc_ids is None:
            raise RuntimeError('Cannot get documents without a condition or document ID')

        if doc_id is not None:
            table = self._read_table()
            if doc_id in table:
                return self.document_class(table[doc_id], self.document_id_class(doc_id))
            return None

        if doc_ids is not None:
            docs = []
            table = self._read_table()
            for did in doc_ids:
                if did in table:
                    docs.append(self.document_class(table[did], self.document_id_class(did)))
            return docs if docs else None

        if cond is not None:
            docs = self.search(cond)
            if docs:
                return docs[0]

        return None

    def contains(self, cond: Optional[QueryLike]=None, doc_id: Optional[int]=None) -> bool:
        """
        Check whether the database contains a document matching a query or
        an ID.

        If ``doc_id`` is set, it checks if the db contains the specified ID.

        :param cond: the condition use
        :param doc_id: the document ID to look for
        """
        if cond is None and doc_id is None:
            raise RuntimeError('Cannot check for documents without a condition or document ID')

        if doc_id is not None:
            return doc_id in self._read_table()

        return bool(self.get(cond))

    def update(self, fields: Union[Mapping, Callable[[Mapping], None]], cond: Optional[QueryLike]=None, doc_ids: Optional[Iterable[int]]=None) -> List[int]:
        """
        Update all matching documents to have a given set of fields.

        :param fields: the fields that the matching documents will have
                       or a method that will update the documents
        :param cond: which documents to update
        :param doc_ids: a list of document IDs
        :returns: a list containing the updated document's ID
        """
        if doc_ids is not None:
            doc_ids = list(doc_ids)

        def updater(table: Dict[int, Mapping]):
            updated_ids = []

            if doc_ids is not None:
                for doc_id in doc_ids:
                    if doc_id in table:
                        updated_ids.append(doc_id)
                        if callable(fields):
                            doc = table[doc_id].copy()
                            fields(doc)
                            table[doc_id] = doc
                        else:
                            table[doc_id].update(fields)
            else:
                for doc_id, doc in list(table.items()):
                    if cond is None or cond(doc):
                        updated_ids.append(doc_id)
                        if callable(fields):
                            doc = doc.copy()
                            fields(doc)
                            table[doc_id] = doc
                        else:
                            table[doc_id].update(fields)

            return updated_ids

        updated_ids = self._update_table(updater)
        self._query_cache.clear()

        return updated_ids

    def update_multiple(self, updates: Iterable[Tuple[Union[Mapping, Callable[[Mapping], None]], QueryLike]]) -> List[int]:
        """
        Update all matching documents to have a given set of fields.

        :returns: a list containing the updated document's ID
        """
        updated_ids = []
        for fields, cond in updates:
            updated_ids.extend(self.update(fields, cond))
        return updated_ids

    def upsert(self, document: Mapping, cond: Optional[QueryLike]=None) -> List[int]:
        """
        Update documents, if they exist, insert them otherwise.

        Note: This will update *all* documents matching the query. Document
        argument can be a tinydb.table.Document object if you want to specify a
        doc_id.

        :param document: the document to insert or the fields to update
        :param cond: which document to look for, optional if you've passed a
        Document with a doc_id
        :returns: a list containing the updated documents' IDs
        """
        if isinstance(document, Document):
            doc_id = document.doc_id
            updated = self.update(document, doc_ids=[doc_id])
            if updated:
                return updated

            document = dict(document)
            return [self.insert(document)]

        updated = self.update(document, cond)
        if updated:
            return updated

        return [self.insert(document)]

    def remove(self, cond: Optional[QueryLike]=None, doc_ids: Optional[Iterable[int]]=None) -> List[int]:
        """
        Remove all matching documents.

        :param cond: the condition to check against
        :param doc_ids: a list of document IDs
        :returns: a list containing the removed documents' ID
        """
        if cond is None and doc_ids is None:
            raise RuntimeError('Cannot remove documents without a condition or document IDs')

        if doc_ids is not None:
            doc_ids = list(doc_ids)

        def updater(table: Dict[int, Mapping]):
            removed = []

            if doc_ids is not None:
                for doc_id in doc_ids:
                    if doc_id in table:
                        removed.append(doc_id)
                        del table[doc_id]
            else:
                for doc_id, doc in list(table.items()):
                    if cond(doc):
                        removed.append(doc_id)
                        del table[doc_id]

            return removed

        removed_ids = self._update_table(updater)
        self._query_cache.clear()

        return removed_ids

    def truncate(self) -> None:
        """
        Truncate the table by removing all documents.
        """
        def updater(table: Dict[int, Mapping]):
            table.clear()
            return []

        self._update_table(updater)
        self._query_cache.clear()
        self._next_id = 1

    def count(self, cond: QueryLike) -> int:
        """
        Count the documents matching a query.

        :param cond: the condition use
        """
        return len(self.search(cond))

    def clear_cache(self) -> None:
        """
        Clear the query cache.
        """
        self._query_cache.clear()

    def __len__(self):
        """
        Count the total number of documents in this table.
        """
        return len(self._read_table())

    def __iter__(self) -> Iterator[Document]:
        """
        Iterate over all documents stored in the table.

        :returns: an iterator over all documents.
        """
        for doc_id, doc in self._read_table().items():
            yield self.document_class(doc, self.document_id_class(doc_id))

    def _get_next_id(self):
        """
        Return the ID for a newly inserted document.
        """
        if self._next_id is None:
            table = self._read_table()
            if table:
                self._next_id = max(int(key) for key in table.keys()) + 1
            else:
                self._next_id = 1
            return self._next_id

        next_id = self._next_id
        self._next_id = next_id + 1
        return next_id

    def _read_table(self) -> Dict[int, Mapping]:
        """
        Read the table data from the underlying storage.

        Documents and doc_ids are NOT yet transformed, as 
        we may not want to convert *all* documents when returning
        only one document for example.
        """
        raw_data = self._storage.read()
        if raw_data is None:
            raw_data = {}

        table = raw_data.get(self._name, {})
        if not isinstance(table, dict):
            table = {}
            raw_data[self._name] = table

        return table

    def _update_table(self, updater: Callable[[Dict[int, Mapping]], None]):
        """
        Perform a table update operation.

        The storage interface used by TinyDB only allows to read/write the
        complete database data, but not modifying only portions of it. Thus,
        to only update portions of the table data, we first perform a read
        operation, perform the update on the table data and then write
        the updated data back to the storage.

        As a further optimization, we don't convert the documents into the
        document class, as the table data will *not* be returned to the user.
        """
        raw_data = self._storage.read()
        if raw_data is None:
            raw_data = {}

        table = raw_data.get(self._name, {})
        result = updater(table)
        raw_data[self._name] = table
        self._storage.write(raw_data)

        return result