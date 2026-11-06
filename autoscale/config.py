import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BALANCER_STRATEGY = os.getenv("BALANCER_STRATEGY", "round_robin")
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "1200"))
WORKER_PORT = int(os.getenv("WORKER_PORT", "8001"))
WORKER_URL = os.getenv("WORKER_URL", "")
MIN_SIM_WORKERS = int(os.getenv("MIN_SIM_WORKERS", "2"))
MAX_SIM_WORKERS = int(os.getenv("MAX_SIM_WORKERS", "20"))
SIM_WORKER_CAPACITY = float(os.getenv("SIM_WORKER_CAPACITY", "5"))
AUTOSCALE_INTERVAL = float(os.getenv("AUTOSCALE_INTERVAL", "2"))

QUEUE_KEY = "tasks:queue"
WORKERS_KEY = "workers"
SIM_WORKERS_KEY = "autoscaler:sim_workers"
EVENTS_KEY = "autoscaler:events"
STRATEGY_KEY = "gateway:strategy"
