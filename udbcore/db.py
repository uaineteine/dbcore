class DB:
    """A superset of a path and name."""
    def __init__(self, path:str, name:str, enforce_name:bool=True):
        self.path = path
        self.name = name
        
        if enforce_name:
            DB.acceptable_name(name)
        
        self.loaded = False

    @staticmethod
    def acceptable_name(name: str) -> bool:
        """
        Raises ValueError with a specific message if the name is not acceptable for a DB.
        Returns True if the name is valid.
        An acceptable name:
        - does not contain any slashes or backslashes
        - contains a .duckdb file extension
        - is not empty
        - contains no spaces
        - is less than 20 characters long
        """
        if len(name) == 0:
            raise ValueError("Name is empty.")
        if len(name) > 20:
            raise ValueError("Name is longer than 20 characters.")
        if ' ' in name:
            raise ValueError("Name contains spaces.")
        if '/' in name or '\\' in name:
            raise ValueError("Name contains slashes or backslashes.")
        if not name.lower().endswith('.duckdb'):
            raise ValueError("Name must have a .duckdb extension.")
        return True

    def set_loaded(self, loaded:bool) -> None:
        """Sets the loaded state of the DB."""
        self.loaded = loaded
