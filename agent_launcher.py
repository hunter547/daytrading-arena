#!/usr/bin/env python3
"""Launch all ChatNode + Agent Router processes defined in agents.yml.

Reads agents.yml at startup and spawns:
- One ChatNode process per unique model
- One Agent Router process per agent

All processes run as async subprocesses. If any process exits, it is
automatically restarted after a brief delay.

Usage:
    python agent_launcher.py --bootstrap-servers localhost:9092
"""

import argparse
import asyncio
import logging
import os
import signal
import sys

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] agent_launcher: %(message)s",
)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "agents.yml")


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


async def run_process(name: str, cmd: list[str], env: dict):
    """Run a subprocess, restarting on exit."""
    while True:
        logger.info(f"[{name}] Starting: {' '.join(cmd)}")
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env={**os.environ, **env},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        # Stream output with prefix
        async def stream_output():
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                print(f"[{name}] {line.decode().rstrip()}")

        await stream_output()
        code = await proc.wait()
        logger.warning(f"[{name}] Exited with code {code}, restarting in 5s...")
        await asyncio.sleep(5)


def build_chatnode_cmd(model_name: str, model_cfg: dict, bootstrap: str) -> tuple[list[str], dict]:
    """Build command + env for a ChatNode process."""
    api_key_env = model_cfg.get("api_key_env", "OPENAI_API_KEY")

    cmd = [
        sys.executable, "deploy_chat_node.py",
        "--name", model_name,
        "--model-id", model_cfg["model_id"],
        "--bootstrap-servers", bootstrap,
    ]
    if model_cfg.get("base_url"):
        cmd += ["--base-url", model_cfg["base_url"]]
        cmd += ["--api-key", os.environ.get(api_key_env, "")]
    if model_cfg.get("reasoning_effort"):
        cmd += ["--reasoning-effort", str(model_cfg["reasoning_effort"])]
    if model_cfg.get("max_workers"):
        cmd += ["--max-workers", str(model_cfg["max_workers"])]

    env = {"SERVICE_NAME": f"chatnode_{model_name}"}
    return cmd, env


def build_agent_cmd(agent_name: str, agent_cfg: dict, bootstrap: str) -> tuple[list[str], dict]:
    """Build command + env for an Agent Router process."""
    cmd = [
        sys.executable, "deploy_router_node.py",
        "--name", agent_name,
        "--chat-node-name", agent_cfg["model"],
        "--strategy", agent_cfg["strategy"],
        "--bootstrap-servers", bootstrap,
    ]
    env = {"SERVICE_NAME": f"agent_{agent_name}"}
    return cmd, env


async def main():
    parser = argparse.ArgumentParser(description="Launch agents from agents.yml")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--config", default=CONFIG_PATH)
    args = parser.parse_args()

    config = load_config(args.config)
    models = config.get("models", {})
    agents = config.get("agents", {})

    if not agents:
        logger.error("No agents defined in agents.yml")
        sys.exit(1)

    # Determine which models are actually used
    used_models = {a["model"] for a in agents.values()}
    for m in used_models:
        if m not in models:
            logger.error(f"Agent references model '{m}' not defined in models section")
            sys.exit(1)

    # Publish agent names so trading-tools can pre-create accounts
    agent_names = ",".join(sorted(agents.keys()))
    logger.info(f"Agent names: {agent_names}")

    tasks = []

    # Launch ChatNodes (one per model)
    for model_name in sorted(used_models):
        cmd, env = build_chatnode_cmd(model_name, models[model_name], args.bootstrap_servers)
        tasks.append(run_process(f"chatnode:{model_name}", cmd, env))

    # Small delay so chatnodes start listening before routers connect
    async def launch_agents_after_delay():
        await asyncio.sleep(3)
        agent_tasks = []
        for agent_name in sorted(agents.keys()):
            cmd, env = build_agent_cmd(agent_name, agents[agent_name], args.bootstrap_servers)
            agent_tasks.append(run_process(f"agent:{agent_name}", cmd, env))
        await asyncio.gather(*agent_tasks)

    tasks.append(launch_agents_after_delay())

    logger.info(f"Launching {len(used_models)} model(s), {len(agents)} agent(s)")
    for name, a in agents.items():
        logger.info(f"  {name}: model={a['model']}, strategy={a['strategy']}")

    # Handle shutdown gracefully
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: [t.cancel() for t in asyncio.all_tasks()])

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Shutting down all agents...")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAgent launcher stopped.")
