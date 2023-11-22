from Orchestrator import Orchestrator
from Steps import Steps


# Steps 
orchestator = Orchestrator(
    [
        Steps.Extract,
        Steps.Transform, 
        Steps.Load
    ]
)

# Start execution
orchestator.execute()