# DEPRECATED: Use app.storage.memory.InMemoryStorage instead
# This module is kept for backward compatibility only

import warnings
from app.storage.memory import InMemoryStorage

warnings.warn(
    "app.storage.db.KeyValueStore is deprecated, use app.storage.memory.InMemoryStorage instead",
    DeprecationWarning,
    stacklevel=2
)

# Alias for backward compatibility
KeyValueStore = InMemoryStorage


        

            
        
        

        
        