from bson.objectid import ObjectId

def serialize(document: dict) -> dict:
    """
    Serialize a MongoDB document by converting ObjectId to string recursively
    and translating `_id` to `id`.
    """
    if not document:
        return document
    serialized = {}
    for key, value in document.items():
        if key == "_id":
            serialized["id"] = str(value)  # Translate `_id` to `id`
        elif isinstance(value, ObjectId):
            serialized[key] = str(value)
        elif isinstance(value, dict):
            serialized[key] = serialize(value)  # Recursive call for nested documents
        elif isinstance(value, list):
            serialized[key] = [serialize(item) if isinstance(item, dict) else item for item in value]
        else:
            serialized[key] = value
    return serialized

def deserialize(data: dict) -> dict:
    """
    Deserialize a dictionary by converting string `id` to ObjectId recursively
    and translating `id` to `_id`.
    """
    if not data:
        return data
    deserialized = {}
    for key, value in data.items():
        if key == "id":
            deserialized["_id"] = ObjectId(value)
        elif isinstance(value, dict):
            deserialized[key] = deserialize(value)
        elif isinstance(value, list):
            deserialized[key] = [deserialize(item) if isinstance(item, dict) else item for item in value]
        else:
            deserialized[key] = value
    return deserialized
