from relict_core.config import events
from relict_core.config.events import BaseEvent


EVENT_MAPPING: dict[str, type[BaseEvent]] = {
    cls.__name__: cls
    for cls in BaseEvent.__subclasses__()
}
