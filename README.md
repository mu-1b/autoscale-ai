# AutoScale AI

A distributed API system built with Python, FastAPI, Redis, and Docker. It uses load balancing to distribute requests across workers, a Redis queue for background tasks, and an autoscaling simulation that adjusts worker capacity based on traffic.

The project also includes a dashboard to monitor system performance and test how it responds to traffic spikes.

## Features

- Round-robin and least-connections load balancing
- Redis task queue with asynchronous workers
- Per-client API rate limiting
- Autoscaling simulation based on traffic, latency, and queue size
- Dashboard showing request rate, latency, queue depth, and worker activity
- Load generator for simulating traffic spikes
- Worker health checks and error handling

## Getting Started

Make sure Docker is installed, then run:

```bash
cp .env.example .env
docker compose up --build
```

Open http://localhost:8000 to view the dashboard. Use the **Trigger demo spike** button to test the system under increased traffic.

API documentation is available at http://localhost:8000/docs.

## Notes

Autoscaling is simulated rather than creating or removing Docker containers. It adjusts the number of active task consumers within existing workers to demonstrate how scaling affects queue processing.
