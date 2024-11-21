"""
A collection of update operations for TinyDB.

They are used for updates like this:

>>> db.update(delete('foo'), where('foo') == 2)

This would delete the ``foo`` field from all documents where ``foo`` equals 2.
"""

def delete(field):
    """
    Delete a given field from the document.
    """
    def transform(doc):
        if field in doc:
            del doc[field]
        return doc
    return transform

def add(field, n):
    """
    Add ``n`` to a given field in the document.
    """
    def transform(doc):
        if field not in doc:
            doc[field] = n
        else:
            doc[field] += n
        return doc
    return transform

def subtract(field, n):
    """
    Subtract ``n`` to a given field in the document.
    """
    def transform(doc):
        if field not in doc:
            doc[field] = -n
        else:
            doc[field] -= n
        return doc
    return transform

def set(field, val):
    """
    Set a given field to ``val``.
    """
    def transform(doc):
        doc[field] = val
        return doc
    return transform

def increment(field):
    """
    Increment a given field in the document by 1.
    """
    def transform(doc):
        if field not in doc:
            doc[field] = 1
        else:
            doc[field] += 1
        return doc
    return transform

def decrement(field):
    """
    Decrement a given field in the document by 1.
    """
    def transform(doc):
        if field not in doc:
            doc[field] = -1
        else:
            doc[field] -= 1
        return doc
    return transform