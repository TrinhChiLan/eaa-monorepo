"""control_pipeline.fsm — FSM sub-package for the EAA conveyor control_pipeline.

Public surface
--------------
``FSMEngine``
    The table-driven state machine that enforces deterministic conveyor
    state transitions.  Import it directly from this package:

    >>> from control_pipeline.fsm import FSMEngine
"""

from control_pipeline.fsm.engine import FSMEngine

__all__: list[str] = ["FSMEngine"]
