"""
Прогоняет collect_leads_customers на синтетической выгрузке и считает запросы и память.

    DATABASE_URL=postgresql+psycopg2://... python scripts/benchmark.py --reset
    DATABASE_URL=postgresql+psycopg2://... python scripts/benchmark.py

С --reset схема пересоздаётся (DROP ВСЕХ таблиц модели) — запускать только на отдельной базе.
"""
import argparse
import os
import resource
import sys
import time
from pathlib import Path

from flask import Flask
from sqlalchemy import event, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main  # noqa: E402
from amocrm import Customer, Lead  # noqa: E402
from models import db  # noqa: E402

API_VALUE_PADDING = "x" * 3000


class BenchCustomer(Customer):
    def as_dict(self):
        return {"id": self.id, "name": self.name, "padding": API_VALUE_PADDING}


class BenchLead(Lead):
    def as_dict(self):
        return {"id": self.id, "name": self.name, "padding": API_VALUE_PADDING}


def fields(login):
    return {"Логин": {"id": 1, "value": login}}


def customers(count):
    for i in range(1, count + 1):
        yield BenchCustomer(
            id=i, name=f"Покупатель {i}", next_price=0, next_date=0, responsible_user_id=1, status_id=1,
            periodicity=0, created_by=1, updated_by=1, created_at=0, updated_at=0, closest_task_at=0,
            is_deleted=False, ltv=0, purchases_count=0, average_check=0, account_id=1,
            custom_fields_values=fields(f"login-{i}"),
            _embedded={"contacts": [{"id": i}], "companies": [{"id": i}]},
        )


def leads(count, customer_count):
    for i in range(1, count + 1):
        yield BenchLead(id=1_000_000 + i, name=f"Сделка {i}", status_id=1,
                        custom_fields_values=fields(f"login-{(i % customer_count) + 1}"),
                        _embedded={"contacts": [{"id": (i % customer_count) + 1}], "companies": []})


def peak_rss_mb():
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 1024 / 1024 if sys.platform == "darwin" else peak / 1024


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--customers", type=int, default=20_000)
    parser.add_argument("--leads", type=int, default=50_000)
    parser.add_argument("--seed-contacts", action="store_true", help="заполнить amo_contacts перед прогоном")
    args = parser.parse_args()

    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
    db.init_app(app)
    with app.app_context():
        if args.reset:
            db.drop_all()
            db.create_all()
        if args.seed_contacts:
            db.session.execute(text(
                "UPDATE abstract_customer SET amo_contacts = jsonb_build_array("
                "jsonb_build_object('general', jsonb_build_object('id', id, 'name', 'контакт')))"
            ))
            db.session.commit()

        main.amocrm_get = lambda cls, **kwargs: (
            customers(args.customers) if cls is Customer else leads(args.leads, args.customers)
        )
        counters = {"statements": 0, "commits": 0}

        def on_statement(*_):
            counters["statements"] += 1

        def on_commit(*_):
            counters["commits"] += 1

        event.listen(db.engine, "before_cursor_execute", on_statement)
        event.listen(db.engine, "commit", on_commit)
        baseline = peak_rss_mb()
        started = time.monotonic()
        main.collect_leads_customers(is_full=True)
        print(f"statements={counters['statements']} commits={counters['commits']} "
              f"seconds={time.monotonic() - started:.1f} "
              f"rss_before_mb={baseline:.0f} rss_peak_mb={peak_rss_mb():.0f}")


if __name__ == "__main__":
    run()
