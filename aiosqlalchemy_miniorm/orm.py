# -*- coding: utf-8 -*-
import collections
import logging

from sqlalchemy.ext.declarative import DeclarativeMeta


logger = logging.getLogger('aiosqlalchemy_miniorm')


class classproperty(object):
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, instance_cls=None):
        return self.func(instance_cls)


OrderBy = collections.namedtuple('OrderBy', ['field', 'order'])


class BaseModelManager:
    FETCH_ALL = 'fetchall'
    FETCH_ONE = 'fetchone'
    FETCH_ROW_COUNT = 'rowcount'
    FETCH_SCALAR = 'scalar'

    SORT_UP = 'asc'
    SORT_DOWN = 'desc'

    SORT_ORDERS = (SORT_UP, SORT_DOWN)

    table = None
    row_class = None

    def __init__(self, table, row_class):
        self.row_class = row_class
        self.sql = None
        self.table = table
        self.transaction_connection = None

    @property
    def engine(self):
        return self.table.bind

    @staticmethod
    async def fetch_from_result_proxy(result_proxy, fetch):
        if not hasattr(result_proxy, fetch):
            raise AttributeError('ResultProxy has no attribute {}'.format(fetch))

        result = getattr(result_proxy, fetch)

        if callable(result):
            result = await result()

        return result

    def transaction(self):
        """
        Usage:
            async with SomeModel.objects.transaction() as some_model_objects:
                await some_model_objects.do_some_stuff()
                await some_model_objects.do_another_stuff()

        Note: Transactions are not cross-models.
        """
        return _TransactionContextManager(self.new_instance())

    async def run_query(self, sql=None, fetch=FETCH_ALL):
        if sql is None:
            sql = self.get_sql()

        if self.transaction_connection:
            return await self.run_query_with_connection(self.transaction_connection, sql, fetch)
        else:
            async with self.engine.acquire() as connection:
                return await self.run_query_with_connection(connection, sql, fetch)

    async def run_query_with_connection(self, connection, sql=None, fetch=FETCH_ALL):
        try:
            result_proxy = await connection.execute(sql)
            self.set_sql(None)

            return await self.fetch_from_result_proxy(result_proxy, fetch)
        except Exception as e:
            logger.error('Execution of "%s" sql fails with "%s".', sql, e)
            raise

    async def fetchall(self, sql=None):
        return await self.run_query(sql=sql, fetch=self.FETCH_ALL)

    async def fetchone(self, sql=None):
        return await self.run_query(sql=sql, fetch=self.FETCH_ONE)

    async def scalar(self, sql=None):
        return await self.run_query(sql=sql, fetch=self.FETCH_SCALAR)

    async def rowcount(self, sql=None):
        return await self.run_query(sql=sql, fetch=self.FETCH_ROW_COUNT)

    @property
    def _pk_column(self):
        return self.table.primary_key.columns.values()[0]

    def get_sql(self):
        assert self.sql is not None, 'sql attribute is not defined'

        return self.sql

    def set_sql(self, sql):
        self.sql = sql

        return self

    def where(self, where_list: list=None):
        if where_list:
            for where in where_list:
                self.sql = self.get_sql().where(where)

        return self

    def order_by(self, order_by: list=None):
        if order_by:
            for item in order_by:
                assert isinstance(item, OrderBy), 'Order items should be instances of OrderBy class'
                assert item.order in self.SORT_ORDERS, 'Unknown sort order `{}`'.format(item.order)
                order_column = self.table.columns[item.field]

                if item.order == self.SORT_DOWN:
                    order_column = order_column.desc()
                else:
                    order_column = order_column.asc()

                self.sql = self.get_sql().order_by(order_column)

        return self

    def offset(self, offset: int=0):
        if offset > 0:
            self.sql = self.get_sql().offset(offset)

        return self

    def limit(self, limit: int=None):
        self.sql = self.get_sql().limit(limit)

        return self

    def values(self, *args, **kwargs):
        self.set_sql(self.get_sql().values(*args, **kwargs))
        return self

    def returning(self, *cols):
        self.set_sql(self.get_sql().returning(*cols))
        return self

    async def get_item(self, where_list: list=None):
        self.set_sql(self.table.select())\
            .where(where_list)

        return await self.fetchone()

    async def get_instance(self, where_list: list=None):
        row_proxy = await self.get_item(where_list)

        if row_proxy:
            return self.row_class(**dict(row_proxy))

        return None

    async def bulk_insert(self, values: list, fetch=True):
        self.set_sql(self.table.insert())\
            .values(values)

        if fetch:
            self.returning(*self.table.columns)

        if not fetch:
            return await self.rowcount()
        else:
            rows = await self.fetchall()

            return [self.row_class(**dict(row)) for row in rows]

    async def insert(self, fetch=True, **values):
        self.set_sql(self.table.insert()) \
            .values(**values)

        if fetch:
            self.returning(*self.table.columns)
            row = await self.fetchone()

            return self.row_class(**dict(row))
        else:
            return await self.scalar()

    async def update(self, where_list: list=None, fetch=False, **values):
        self.set_sql(self.table.update()) \
            .where(where_list) \
            .values(**values)

        if fetch:
            self.returning(*self.table.columns)
            rows = await self.fetchall()

            return [self.row_class(**dict(row)) for row in rows] if rows else []
        else:
            return await self.rowcount()

    async def delete(self, where_list: list=None):
        self.set_sql(self.table.delete())\
            .where(where_list)

        return await self.rowcount()

    async def get_items(self, query=None, where_list: list=None, limit: int=None, offset: int=0, order_by: list=None):
        base_query = query if query is not None else self.table.select()
        self.set_sql(base_query) \
            .where(where_list) \
            .order_by(order_by) \
            .offset(offset) \
            .limit(limit)

        return await self.fetchall()

    async def get_instances(self, where_list: list=None, limit: int=None, offset: int=0, order_by: list=None):
        result = []
        rows = await self.get_items(where_list=where_list, limit=limit, offset=offset, order_by=order_by)

        for row in rows:
            result.append(self.row_class(**dict(row)))

        return result

    async def count(self, query=None, where_list: list=None):
        base_query = query if query is not None else self.table.count()

        self.set_sql(base_query)\
            .where(where_list)

        return await self.scalar()

    def new_instance(self):
        return type(self)(table=self.table, row_class=self.row_class)


class RowModelDeclarativeMeta(DeclarativeMeta):
    def __getattr__(cls, item):
        if item == 'c':
            return cls.__table__.c
        if item == 'objects':
            if cls.model_manager is None:
                cls.model_manager = cls.__model_manager_class__(table=cls.__table__, row_class=cls)
            return cls.model_manager
        return super().__getattr__(item)


class RowModel:
    __model_manager_class__ = BaseModelManager
    model_manager = None

    def __new__(cls, *args, **kwargs):
        if cls.model_manager is None:
            cls.model_manager = cls.__model_manager_class__(table=cls.__table__, row_class=cls)
        return super().__new__(cls)

    def __iter__(self):
        for col in self.columns:
            yield col.key, getattr(self, col.key)

    def __repr__(self):
        return '{}{}'.format(self.__class__.__name__, {self.pk_column.key: self._pk_value})

    @classproperty
    def table(cls):
        return cls.__table__

    @classproperty
    def columns(cls):
        return cls.table.columns

    @classproperty
    def autoincrement_column(cls):
        return cls.table._autoincrement_column

    @classproperty
    def pk_column(cls):
        return list(cls.table.primary_key)[0]

    @property
    def _pk_value(self):
        return getattr(self, self.pk_column.key)

    def _get_values(self):
        values = {}
        for col in self.columns:
            value = getattr(self, col.key)
            # let init with default values
            if col is not self.autoincrement_column and value is not None:
                values[col.key] = value
        return values

    def _get_value(self, key):
        return getattr(self, key)

    def _set_values(self, values: dict):
        for col in self.columns:
            if col.key in values and values[col.key] != self._get_value(col.key):
                setattr(self, col.key, values[col.key])

    def check(self):
        if self._sa_instance_state._deleted:
            raise Exception("You can't save or update deleted row")

    async def insert(self):
        res = await self.model_manager.insert(**self._get_values())
        self._set_values(dict(res))

        return self

    async def update(self, **kwargs):
        self.check()
        where = (self.pk_column == self._pk_value)
        row_count = await self.model_manager.update(where_list=[where], fetch=False, **kwargs)

        if row_count:
            self._set_values(kwargs)

        return self

    async def delete(self):
        self.check()
        where = (self.pk_column == self._pk_value)
        rowcount = await self.model_manager.delete([where])
        self._sa_instance_state._deleted = True

        return rowcount


class _TransactionContextManager:
    def __init__(self, model_mgr):
        self._model_mgr = model_mgr
        self._engine_acquire_cm = None
        self._transaction_cm = None

    async def __aenter__(self):
        self._engine_acquire_cm = self._model_mgr.engine.acquire()
        self._model_mgr.transaction_connection = await self._engine_acquire_cm.__aenter__()
        self._transaction_cm = self._model_mgr.transaction_connection.begin()
        await self._transaction_cm.__aenter__()

        return self._model_mgr

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._transaction_cm.__aexit__(exc_type, exc_val, exc_tb)
        await self._engine_acquire_cm.__aexit__(exc_type, exc_val, exc_tb)
        self._model_mgr.transaction_connection = None
