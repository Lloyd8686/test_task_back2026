import logging
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple, Type, Union

from sqlalchemy import delete, exists, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from abstract_customer import dict_compare_merge, get_customer_json, get_lead_json, merge_send_methods
from amocrm import BaseEntity, Customer, COMPANIES, CONTACTS, Lead, amocrm_get
from models import db, AbstractCustomer, AmoCustomer, AmoLead

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
INDEX_FETCH_SIZE = 5000
DELETE_MARKER = 'удалить'
ARCHIVED_CUSTOMER_STATUS = 111349
LOST_LEAD_STATUS = 143
UPSERT_COLUMNS = ('abstract_customer_id', 'login', 'name', 'contact_ids', 'company_ids', 'data')

AmoModel = Union[Type[AmoCustomer], Type[AmoLead]]
LoginIndex = Dict[str, Tuple[int, ...]]


def has_delete_marker(name: Optional[str]) -> bool:
    return DELETE_MARKER in (name or '').lower()


def load_entity_records(model: AmoModel) -> Dict[int, int]:
    """Возвращает {id сущности в CRM: id записи клиента}."""
    stmt = select(model.id, model.abstract_customer_id).execution_options(yield_per=INDEX_FETCH_SIZE)
    records = {entity_id: record_id for entity_id, record_id in db.session.execute(stmt)}
    db.session.commit()
    return records


def load_login_index(model: AmoModel) -> LoginIndex:
    """Возвращает {логин: id разных записей клиента, где он встречается}."""
    stmt = (select(model.login, model.abstract_customer_id)
            .where(model.login.is_not(None))
            .distinct()
            .execution_options(yield_per=INDEX_FETCH_SIZE))
    index: LoginIndex = {}
    for login, record_id in db.session.execute(stmt):
        index[login] = index.get(login, ()) + (record_id,)
    db.session.commit()
    return index


def _nested_ids(entity: BaseEntity) -> Dict[str, List[int]]:
    return {
        'contact_ids': sorted({contact['id'] for contact in entity.contacts}),
        'company_ids': sorted({company['id'] for company in entity.companies}),
    }


def _entity_row(entity: BaseEntity, payload: dict) -> dict:
    return {'id': entity.id, 'login': payload['general']['login'], 'name': entity.name,
            'data': payload, **_nested_ids(entity)}


def _chunks(items: List[int]) -> Iterable[List[int]]:
    for start in range(0, len(items), BATCH_SIZE):
        yield items[start:start + BATCH_SIZE]


class SyncRun:
    """
    Состояние одного прогона.

    Поиск идёт по компактным индексам в памяти, изменения копятся в буферах и уходят
    в Postgres пачками. Между пачками транзакция не держится открытой на время запросов к CRM.
    """

    def __init__(self, is_full: bool):
        self.is_full = is_full
        self.has_errors = False
        self._redirects: Dict[int, int] = {}  # слитая запись -> запись, в которую её влили
        self._free_record_ids: List[int] = []
        self._new_records: List[int] = []
        self._customers: Dict[int, dict] = {}
        self._leads: Dict[int, dict] = {}
        self._skipped_customers: Dict[int, dict] = {}

    def collect_customers(self) -> Tuple[List[int], int]:
        known = load_entity_records(AmoCustomer)
        leads_by_login = load_login_index(AmoLead)
        seen: Set[int] = set()
        exported = 0
        for customer in amocrm_get(Customer, _with=f"{COMPANIES},{CONTACTS}"):
            exported += 1
            if self.is_full isа р False and customer.status_id == ARCHIVED_CUSTOMER_STATUS:
                continue
            try:
                self._process_customer(customer, known, leads_by_login, seen)
            except Exception as e:
                self.has_errors = True
                logger.exception(f'{customer} {e}')
            self._flush_if_full()
        self.flush()
        return [entity_id for entity_id in known if entity_id not in seen], exported

    def collect_leads(self) -> Tuple[List[int], int]:
        known = load_entity_records(AmoLead)
        customers_by_login = load_login_index(AmoCustomer)
        seen: Set[int] = set()
        exported = 0
        for lead in amocrm_get(Lead, _with=f"{CONTACTS}"):
            exported += 1
            if self.is_full is False and lead.status_id == LOST_LEAD_STATUS:
                continue
            try:
                self._process_lead(lead, known, customers_by_login, seen)
            except Exception as e:
                self.has_errors = True
                logger.exception(f'{lead} {e}')
            self._flush_if_full()
        self.flush()
        return [entity_id for entity_id in known if entity_id not in seen], exported

    def _process_customer(self, customer: Customer, known: Dict[int, int],
                          leads_by_login: LoginIndex, seen: Set[int]) -> None:
        login = customer.get_field('Логин')
        if not login:
            if customer.id in known:
                # Покупатель есть в выгрузке, поэтому не обновляем его, но и не удаляем.
                self._skipped_customers[customer.id] = {'id': customer.id, **_nested_ids(customer)}
                self._mark_seen(customer, seen)
            return

        row = _entity_row(customer, get_customer_json(customer))
        record_id = known.get(customer.id)
        if record_id is None:
            record_id = self._unique_record(leads_by_login.get(login), login)  # почти всегда логины совпадают
        if record_id is None:
            record_id = self._new_record()
        known[customer.id] = record_id
        self._customers[customer.id] = {**row, 'abstract_customer_id': record_id}
        self._mark_seen(customer, seen)

    def _process_lead(self, lead: Lead, known: Dict[int, int],
                      customers_by_login: LoginIndex, seen: Set[int]) -> None:
        row = _entity_row(lead, get_lead_json(lead))
        record_id = known.get(lead.id)
        if record_id is not None:
            record_id = self.resolve(record_id)

        login = lead.get_field('Логин')
        if login:
            login_record_id = self._unique_record(customers_by_login.get(login), login)
            if record_id is None:
                record_id = login_record_id
            elif login_record_id is not None and login_record_id != record_id:
                self._merge(loser_id=login_record_id, winner_id=record_id)
        if record_id is None:
            record_id = self._new_record()
        known[lead.id] = record_id
        self._leads[lead.id] = {**row, 'abstract_customer_id': record_id}
        self._mark_seen(lead, seen)

    @staticmethod
    def _mark_seen(entity: BaseEntity, seen: Set[int]) -> None:
        if not has_delete_marker(entity.name):
            seen.add(entity.id)

    def resolve(self, record_id: int) -> int:
        """Возвращает итоговую запись с учётом слияний."""
        while record_id in self._redirects:
            record_id = self._redirects[record_id]
        return record_id

    def _unique_record(self, record_ids: Optional[Tuple[int, ...]], login: str) -> Optional[int]:
        roots = {self.resolve(record_id) for record_id in record_ids or ()}
        if len(roots) > 1:
            logger.warning(f"Login {login} has more than one abccustomer")
            return None
        return roots.pop() if roots else None

    def _new_record(self) -> int:
        if not self._free_record_ids:
            self._free_record_ids = list(db.session.scalars(
                text("SELECT nextval(pg_get_serial_sequence('abstract_customer', 'id')) "
                     "FROM generate_series(1, :count)"),
                {'count': BATCH_SIZE},
            ))
            db.session.commit()
        record_id = self._free_record_ids.pop()
        self._new_records.append(record_id)
        return record_id

    def _flush_if_full(self) -> None:
        if len(self._customers) + len(self._leads) + len(self._skipped_customers) >= BATCH_SIZE:
            self.flush()

    def flush(self) -> None:
        """Пишет накопленное одной транзакцией: сначала записи, потом сущности."""
        session = db.session
        if self._new_records:
            session.execute(pg_insert(AbstractCustomer), [{'id': record_id} for record_id in self._new_records])
        self._upsert(AmoCustomer, self._customers)
        self._upsert(AmoLead, self._leads)
        if self._skipped_customers:
            session.execute(update(AmoCustomer), list(self._skipped_customers.values()))
        session.commit()
        self._new_records.clear()
        self._customers.clear()
        self._leads.clear()
        self._skipped_customers.clear()

    def _upsert(self, model: AmoModel, rows: Dict[int, dict]) -> None:
        if not rows:
            return
        stmt = pg_insert(model).values(list(rows.values()))
        table = model.__table__
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.id],
            set_={column: stmt.excluded[column] for column in UPSERT_COLUMNS},
            # Неизменившиеся строки не переписываем: нет лишнего WAL и мёртвых версий строк.
            where=or_(*[table.c[column].is_distinct_from(stmt.excluded[column]) for column in UPSERT_COLUMNS]),
        )
        db.session.execute(stmt)

    def _merge(self, loser_id: int, winner_id: int) -> None:
        """Сливает запись loser_id в winner_id. Слияния редкие, поэтому выполняются сразу."""
        self.flush()
        session = db.session
        winner = session.get(AbstractCustomer, winner_id, with_for_update=True)
        loser = session.get(AbstractCustomer, loser_id, with_for_update=True)
        winner.amo_companies = dict_compare_merge(winner.amo_companies or [], loser.amo_companies or [])
        winner.amo_contacts = dict_compare_merge(winner.amo_contacts or [], loser.amo_contacts or [])
        winner.send_methods = merge_send_methods(winner.send_methods, loser.send_methods)
        for model in (AmoCustomer, AmoLead):
            session.execute(update(model)
                            .where(model.abstract_customer_id == loser_id)
                            .values(abstract_customer_id=winner_id)
                            .execution_options(synchronize_session=False))
        session.delete(loser)
        session.commit()
        self._redirects[loser_id] = winner_id


def _load_allowed_nested_ids(record_ids: List[int]) -> Dict[int, Dict[str, Set[int]]]:
    allowed: Dict[int, Dict[str, Set[int]]] = defaultdict(lambda: {'amo_contacts': set(), 'amo_companies': set()})
    for model in (AmoCustomer, AmoLead):
        rows = db.session.execute(select(model.abstract_customer_id, model.contact_ids, model.company_ids)
                                  .where(model.abstract_customer_id.in_(record_ids)))
        for record_id, contact_ids, company_ids in rows:
            allowed[record_id]['amo_contacts'].update(contact_ids)
            allowed[record_id]['amo_companies'].update(company_ids)
    return allowed


def _filter_nested(items: Optional[List[dict]], allowed_ids: Set[int]) -> List[dict]:
    return [item for item in items or []
            if item['general'].get('id') in allowed_ids and not has_delete_marker(item['general'].get('name'))]


def _clean_nested_entities() -> None:
    session = db.session
    last_id = 0
    while True:
        rows = session.execute(
            select(AbstractCustomer.id, AbstractCustomer.amo_contacts, AbstractCustomer.amo_companies)
            .where(AbstractCustomer.id > last_id,
                   or_(AbstractCustomer.amo_contacts != [], AbstractCustomer.amo_companies != []))
            .order_by(AbstractCustomer.id)
            .limit(BATCH_SIZE)
        ).all()
        if not rows:
            return
        last_id = rows[-1].id
        allowed = _load_allowed_nested_ids([row.id for row in rows])
        updates = []
        for row in rows:
            contacts = _filter_nested(row.amo_contacts, allowed[row.id]['amo_contacts'])
            companies = _filter_nested(row.amo_companies, allowed[row.id]['amo_companies'])
            if contacts != (row.amo_contacts or []) or companies != (row.amo_companies or []):
                updates.append({'id': row.id, 'amo_contacts': contacts, 'amo_companies': companies})
        if updates:
            session.execute(update(AbstractCustomer), updates)


def cleanup(stale_customers: List[int], stale_leads: List[int]) -> None:
    """Удаляет всё, чего не было в выгрузке, одной транзакцией."""
    session = db.session
    for model, stale_ids in ((AmoCustomer, stale_customers), (AmoLead, stale_leads)):
        for chunk in _chunks(stale_ids):
            session.execute(delete(model).where(model.id.in_(chunk)).execution_options(synchronize_session=False))
    session.execute(
        delete(AbstractCustomer)
        .where(~exists().where(AmoCustomer.abstract_customer_id == AbstractCustomer.id),
               ~exists().where(AmoLead.abstract_customer_id == AbstractCustomer.id))
        .execution_options(synchronize_session=False)
    )
    _clean_nested_entities()
    session.commit()


def collect_leads_customers(is_full=True):
    """
    Сначала синхронизирует покупателей, затем сделки. Правила описаны в README.

    Ошибка выгрузки поднимается наружу. Ошибка отдельной сущности логируется, остальные
    сущности обрабатываются, но удаление устаревших данных в таком прогоне не запускается.
    """
    run = SyncRun(is_full)
    try:
        logger.info(f"Collect customers {is_full=}")
        stale_customers, customers_exported = run.collect_customers()
        logger.info(f"Collect leads {is_full=}")
        stale_leads, leads_exported = run.collect_leads()

        if is_full is False:
            return
        if run.has_errors:
            logger.error("Чистка пропущена: часть сущностей не обработана")
            return
        if not customers_exported or not leads_exported:
            # 204 от CRM неотличим от пустой выгрузки, а цена ошибки — потеря всей таблицы.
            logger.error(f"Чистка пропущена: пустая выгрузка {customers_exported=} {leads_exported=}")
            return
        logger.info("Чистка")
        cleanup(stale_customers, stale_leads)
    except Exception:
        db.session.rollback()
        raise
