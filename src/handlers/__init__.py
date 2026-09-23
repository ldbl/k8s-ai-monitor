"""Handler imports for kopf discovery.

kopf needs all decorated handlers to be imported when the module is loaded.
"""
from src.handlers.events import on_warning_event  # noqa: F401
from src.handlers.flux import on_kustomization_event, on_helmrelease_event  # noqa: F401
from src.handlers.startup import on_startup, on_cleanup  # noqa: F401
