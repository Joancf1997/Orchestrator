from Core.Orchestrator import Orchestrator
from Steps import Steps

steps = Steps()
# Steps 
orchestator = Orchestrator(
    [
        steps.Extract,
        steps.Transform, 
        steps.Load,
        steps.work,
        steps.Drop
    ]
)
# Only needed when working with datasets from DB in python
steps.helper = orchestator.get_hepler()

# Start execution
orchestator.execute()