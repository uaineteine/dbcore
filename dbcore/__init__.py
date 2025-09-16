from .db import *
from .ddb import *
from .connection_pool import *
from .transactions import *
from .query_builder import *
from .schema import *
from .monitoring import *

module_version = "2.0.0"
module_name = "dbcore"
module_description = "The `dbcore` submodule provides core abstractions and utilities for working with database files in this project."

def check_module_version(version_expecting:str) -> bool:
    """
    Check if the current module version matches the expected version.

    :param version_expecting: The version string to check against (e.g., "2.0.0").
    :type version_expecting: str
    :return: True if the versions match, False otherwise.
    :rtype: bool
    """
    return module_version == version_expecting

def get_module_info() -> dict:
    """
    Get information about the module.

    :return: A dictionary containing the module's name, version, and description.
    :rtype: dict
    """
    return {
        "name": module_name,
        "version": module_version,
        "description": module_description
    }
