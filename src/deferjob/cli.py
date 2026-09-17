from __future__ import annotations

import argparse
import importlib
import logging
import sys
from collections.abc import Sequence

from deferjob.client import Defer


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="deferjob",
        description="Durable delayed jobs in Postgres.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    install = sub.add_parser("install", help="create the job table")
    install.add_argument("--dsn", required=True, help="Postgres connection string")
    install.add_argument("--table", default="defer_jobs")

    worker = sub.add_parser("worker", help="run due jobs until interrupted")
    worker.add_argument(
        "--app",
        required=True,
        help="import path to a Defer instance, e.g. myapp.jobs:jobs",
    )
    worker.add_argument(
        "--poll",
        type=float,
        default=None,
        help="seconds between polls",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.command == "install":
        Defer(args.dsn, table=args.table).install()
        print(f"installed table {args.table}")
        return 0

    jobs = _load_app(args.app)
    jobs.run(poll_interval=args.poll)
    return 0


def _load_app(path: str) -> Defer:
    if ":" not in path:
        raise SystemExit(" --app must look like module.path:attribute")
    module_name, attr = path.split(":", 1)
    module = importlib.import_module(module_name)
    obj = module
    for part in attr.split("."):
        obj = getattr(obj, part)
    if not isinstance(obj, Defer):
        raise SystemExit(f"{path} is {type(obj).__name__}, expected Defer")
    return obj


if __name__ == "__main__":
    sys.exit(main())
