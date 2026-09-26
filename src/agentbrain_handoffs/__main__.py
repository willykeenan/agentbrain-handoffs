"""``python -m agentbrain_handoffs`` runs the same command line as ``handoffs``."""
from .cli import main

if __name__ == '__main__':
    raise SystemExit(main())
