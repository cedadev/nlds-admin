from uuid import UUID, uuid4
from hashlib import md5


def create_sub_id(filelist: list[dict]) -> str:
    """Sub id is now created by hashing the paths from the filelist"""
    if filelist != []:
        filenames = [f for f in filelist]
        # sort the filenames to ensure hashes match!
        filenames.sort()
        filelist_hash = md5("".join(filenames).encode()).hexdigest()
        sub_id = UUID(filelist_hash)
    else:
        sub_id = uuid4()
    return str(sub_id)
