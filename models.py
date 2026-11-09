from enum import Enum

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import BigInteger, Column, DDL, ForeignKey, Index, MetaData, Text, event, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import declared_attr, relationship

naming_convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(column_0_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
db = SQLAlchemy(metadata=MetaData(naming_convention=naming_convention))

# Индекс по названию строится на триграммах (ILIKE '%...%').
event.listen(db.metadata, "before_create", DDL("CREATE EXTENSION IF NOT EXISTS pg_trgm"))


class AbcCustomerEntities(Enum):
    AmoCompany = 'amo_companies'
    AmoContact = 'amo_contacts'
    SendMethods = 'send_methods'


class AbstractCustomer(db.Model):
    """
    Клиент: связывает покупателей и сделки CRM (таблицы amo_customer и amo_lead).

    "amo_companies": [
        {
            "general": {
                "id": int,
                "inn": Optional[str],
                "name": str
            }
            "api_value": {}
        }
    ]

    "amo_contacts": [
        {
            "general": {
                "id": int,
                "phone": Optional[str],
                "name": str
            }
            "api_value": {}
        }
    ]

    "send_methods": [
        {
            "type": str (mail,sms,wtsp),
            "value": str,
            "desc": str,
        }
    ]
    """
    __tablename__ = "abstract_customer"
    id = Column(db.BigInteger(), nullable=False, primary_key=True)
    amo_companies = Column(JSONB)
    amo_contacts = Column(JSONB)
    send_methods = Column(JSONB)
    created_at = Column(db.DateTime, server_default=func.now())
    updated_at = Column(db.DateTime, onupdate=func.now())

    customers = relationship("AmoCustomer", back_populates="abstract_customer", order_by="AmoCustomer.id",
                             cascade="all, delete-orphan", passive_deletes=True)
    leads = relationship("AmoLead", back_populates="abstract_customer", order_by="AmoLead.id",
                         cascade="all, delete-orphan", passive_deletes=True)

    @property
    def amo_customers(self):
        """Прежний формат для кода, читающего JSON-массив."""
        return [customer.data for customer in self.customers]

    @property
    def amo_leads(self):
        """Прежний формат для кода, читающего JSON-массив."""
        return [lead.data for lead in self.leads]


class AmoEntityMixin:
    """
    Сущность CRM. id — идентификатор в CRM.

    data хранит прежний элемент JSON-массива: {"general": {...}, "api_value": {...}}.
    contact_ids и company_ids — вложенные сущности из последней выгрузки.
    Связь сделка-покупатель идёт через общий abstract_customer_id.
    """
    id = Column(BigInteger, primary_key=True, autoincrement=False)
    login = Column(Text, index=True)
    name = Column(Text)
    contact_ids = Column(ARRAY(BigInteger), nullable=False, server_default="{}")
    company_ids = Column(ARRAY(BigInteger), nullable=False, server_default="{}")
    data = Column(JSONB, nullable=False)

    @declared_attr
    def abstract_customer_id(cls):
        return Column(BigInteger, ForeignKey("abstract_customer.id", ondelete="CASCADE"), nullable=False, index=True)


def _name_trgm_index(table_name: str) -> Index:
    return Index(f"ix_{table_name}_name_trgm", "name",
                 postgresql_using="gin", postgresql_ops={"name": "gin_trgm_ops"})


class AmoCustomer(AmoEntityMixin, db.Model):
    __tablename__ = "amo_customer"
    __table_args__ = (_name_trgm_index(__tablename__),)

    abstract_customer = relationship("AbstractCustomer", back_populates="customers")


class AmoLead(AmoEntityMixin, db.Model):
    __tablename__ = "amo_lead"
    __table_args__ = (_name_trgm_index(__tablename__),)

    abstract_customer = relationship("AbstractCustomer", back_populates="leads")
