"""Define the ResourceShed class."""

from clotho.resource import Resource
from clotho.clothodb import ClothoDB


class ResourceShed:
    """Instantiates and reuses Resource objects."""

    def __init__(self, db) -> None:
        if isinstance(db, ClothoDB):
            self._db = db
        else:
            self._db = ClothoDB(db)

        self.resources = {}

    def get(self, name):
        resource = self.resources.get(name)
        if not resource:
            resource = Resource(self._db, {'Name': name}, resource_shed=self)
            self.resources[name] = resource
        return resource
