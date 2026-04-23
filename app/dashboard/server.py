from __future__ import annotations

import logging
from pathlib import Path

from aiohttp import web

from app.dashboard.generator import DASHBOARD_PATH, generate_dashboard
from app.clients.mongo_client import MongoService

logger = logging.getLogger(__name__)

_NOT_YET = b"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Databot Dashboard</title>
<style>
  body{background:#0f172a;color:#94a3b8;font-family:system-ui,sans-serif;
       display:flex;align-items:center;justify-content:center;min-height:100vh;text-align:center}
  h2{color:#e2e8f0;margin-bottom:8px}
</style></head>
<body>
  <div>
    <h2>Dashboard not yet generated</h2>
    <p>The daily pipeline hasn't run yet. Check back after the next scheduled run.</p>
  </div>
</body>
</html>"""


async def _handle_root(request: web.Request) -> web.Response:
    if DASHBOARD_PATH.exists():
        return web.FileResponse(DASHBOARD_PATH)
    return web.Response(body=_NOT_YET, content_type="text/html")


async def start_dashboard_server(port: int = 8080) -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", _handle_root)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Dashboard server listening on port %d", port)
    return runner
