import json
from collections import defaultdict
from typing import Dict, List, Optional

from sqlalchemy import text

from amocrm import Customer, Lead
from models import db, AbcCustomerEntities


def update_jsonb_field(entity_id: int,
                       attribute: AbcCustomerEntities,
                       index: int, new_value: Dict,
                       table_name='abstract_customer'):
    """Обновление jsonb поля в указанной таблице.
    :param table_name: Название таблицы (должно быть прописано).
    :param entity_id: ид абстрактного покупателя.
    :param attribute: Поле таблицы, которое надо обновить в формате Enum.
    :param index: Индекс изменения - цифра если массив, массив строк если словарь. Цифра должна быть точной.
    :param new_value: Новые данные для изменения
    """
    if table_name != 'abstract_customer':
        raise Exception("Неизвестная таблица")
    query = text(f"""
        UPDATE {table_name}
        SET {attribute.value} = jsonb_set({attribute.value}, :path, :value)
        WHERE id = :entity_id
    """)
    db.session.execute(query, {'path': f'{{{index}}}', 'value': json.dumps(new_value), 'entity_id': entity_id})


def dict_compare_merge(l_dict1, l_dict2):
    d = defaultdict(dict)
    for i in l_dict1 + l_dict2:
        d[i['general']['id']].update(i)

    return list(d.values())


def merge_send_methods(first: Optional[List[Dict]], second: Optional[List[Dict]]) -> List[Dict]:
    """Объединяет способы отправки без дублей; при совпадении остаётся запись из first."""
    merged = {}
    for method in (first or []) + (second or []):
        merged.setdefault((method.get('type'), method.get('value')), method)
    return list(merged.values())


def get_customer_json(customer: Customer) -> dict:
    custom_fields = customer.custom_fields_values or {}
    return {
        'general': {
            'id': customer.id,
            'login': customer.get_field('Логин'),
            'billing_type': customer.get_field('Вид оплаты'),
            'is_robot': bool(custom_fields.get('Робот')),
            'is_nds': bool(custom_fields.get('Оплата с НДС')),
            'system': customer.get_field('Система'),
            'host': customer.get_field('Хост'),
            'name': customer.name,
        },
        'api_value': customer.as_dict()
    }


def get_lead_json(lead: Lead) -> dict:
    return {
        'general': {
            'id': lead.id,
            'login': lead.get_field('Логин'),
            'name': lead.name,
        },
        'api_value': lead.as_dict()
    }
