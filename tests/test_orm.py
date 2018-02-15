# -*- coding: utf-8 -*-

import pytest
from asynctest import CoroutineMock
from pytest_mock import MockFixture

from aiosqlalchemy_miniorm.orm import (
    BaseModelManager,
    RowModel,
    RowModelDeclarativeMeta,
    _TransactionContextManager,
    OrderBy,
)


def async_context_mock(return_value):
    class AsyncContextMock:
        async def __aenter__(self):
            return return_value

        async def __aexit__(self, *args):
            pass

    return AsyncContextMock


class AsyncContextManager:
    def __init__(self, mock_obj):
        self.mock_obj = mock_obj

    async def __aenter__(self):
        return self.mock_obj

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def async_context_manager():
    return AsyncContextManager


@pytest.fixture
def model_manager(mocker: MockFixture):
    fake_table = mocker.Mock()
    fake_row_class = mocker.Mock()

    return BaseModelManager(fake_table, fake_row_class)


class TestBaseModelManagerInit:
    def test_ok(self, mocker: MockFixture):
        fake_table = mocker.Mock()
        fake_row_class = mocker.Mock()

        model_manager = BaseModelManager(fake_table, fake_row_class)

        assert model_manager.table == fake_table
        assert model_manager.row_class == fake_row_class


class TestBaseModelManagerEngine:
    def test_ok(self, model_manager: BaseModelManager):
        compared_engine = model_manager.engine
        expected_engine = model_manager.table.bind

        assert compared_engine == expected_engine


class TestBaseModelFetchFromResultProxy:
    @pytest.mark.asyncio
    async def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_fetch = CoroutineMock()
        fake_proxy_result = mocker.Mock(fake_fetch=fake_fetch)

        compared_result = await model_manager.fetch_from_result_proxy(fake_proxy_result, 'fake_fetch')
        expected_result = fake_fetch.return_value

        assert compared_result == expected_result

        fake_fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_ok_not_callable(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_fetch = 'fake property result'
        fake_proxy_result = mocker.Mock(fake_fetch=fake_fetch)

        compared_result = await model_manager.fetch_from_result_proxy(fake_proxy_result, 'fake_fetch')
        expected_result = fake_fetch

        assert compared_result == expected_result

    @pytest.mark.asyncio
    async def test_error(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_proxy_result = None

        with pytest.raises(AttributeError):
            await model_manager.fetch_from_result_proxy(fake_proxy_result, 'fake_fetch')


class TestBaseModelFetch:
    @pytest.mark.parametrize("test_method,test_constant", [
        ('fetchall', 'FETCH_ALL'),
        ('fetchone', 'FETCH_ONE'),
        ('scalar', 'FETCH_SCALAR'),
        ('rowcount', 'FETCH_ROW_COUNT'),
    ])
    @pytest.mark.asyncio
    async def test_ok(self, test_method, test_constant, model_manager: BaseModelManager, mocker: MockFixture):
        fake_sql = mocker.Mock()

        mocked_const = mocker.patch.object(model_manager, test_constant)
        mocked_run_query = mocker.patch.object(model_manager, 'run_query', CoroutineMock())

        compared_result = await getattr(model_manager, test_method)(fake_sql)
        expected_result = mocked_run_query.return_value

        assert compared_result == expected_result

        mocked_run_query.assert_called_once_with(sql=fake_sql, fetch=mocked_const)


class TestBaseModelManagerRunQuery:
    @pytest.mark.asyncio
    async def test_transaction_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_fetch = mocker.Mock()
        fake_sql = mocker.Mock()
        mocked_run_query_with_connection = mocker.patch.object(
            model_manager, 'run_query_with_connection', CoroutineMock()
        )
        fake_transaction_connection = mocker.Mock()
        mocker.patch.object(model_manager, 'transaction_connection', fake_transaction_connection)
        compared_return = await model_manager.run_query(fake_sql, fetch=fake_fetch)
        expected_return = mocked_run_query_with_connection.return_value

        assert compared_return == expected_return
        mocked_run_query_with_connection.assert_called_once_with(fake_transaction_connection, fake_sql, fake_fetch)

    @pytest.mark.asyncio
    async def test_acquire_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_sql = mocker.Mock()
        fake_fetch = mocker.Mock()
        mocked_run_query_with_connection = mocker.patch.object(
            model_manager, 'run_query_with_connection', CoroutineMock()
        )
        mocked_connection = CoroutineMock()
        mocker.patch.object(
            BaseModelManager,
            'engine',
            mocker.PropertyMock(
                return_value=mocker.Mock(
                    acquire=async_context_mock(return_value=mocked_connection)
                )
            )
        )

        await model_manager.run_query(fake_sql, fake_fetch)

        mocked_run_query_with_connection.assert_called_once_with(mocked_connection, fake_sql, fake_fetch)

    @pytest.mark.asyncio
    async def test_ok_wo_sql(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_fetch = mocker.Mock()
        mocked_get_sql = mocker.patch.object(model_manager, 'get_sql')
        mocked_connection = CoroutineMock()
        mocker.patch.object(model_manager, 'sql')
        mocker.patch.object(
            BaseModelManager,
            'engine',
            mocker.PropertyMock(
                return_value=mocker.Mock(
                    acquire=async_context_mock(return_value=mocked_connection)
                )
            )
        )
        mocked_run_query_with_connection = mocker.patch.object(
            model_manager, 'run_query_with_connection', CoroutineMock()
        )

        compared_return = await model_manager.run_query(fetch=fake_fetch)

        mocked_get_sql.assert_called_once_with()
        assert compared_return == mocked_run_query_with_connection.return_value
        mocked_run_query_with_connection.assert_called_once_with(
            mocked_connection, mocked_get_sql.return_value, fake_fetch
        )


class TestBaseModelManagerRunQueryWithConnection:
    @pytest.mark.asyncio
    async def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_fetch = mocker.Mock()
        fake_sql = mocker.Mock()
        fake_connection = CoroutineMock(execute=CoroutineMock())
        mocked_fetch = mocker.patch.object(model_manager, 'fetch_from_result_proxy', CoroutineMock())

        compared_return = await model_manager.run_query_with_connection(fake_connection, fake_sql, fetch=fake_fetch)

        expacted_return = mocked_fetch.return_value

        assert compared_return == expacted_return

        fake_connection.execute.assert_called_once_with(fake_sql)
        mocked_fetch.assert_called_once_with(fake_connection.execute.return_value, fake_fetch)

    @pytest.mark.asyncio
    async def test_error(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_sql = mocker.Mock()
        fake_connection = CoroutineMock(execute=CoroutineMock(side_effect=Exception))
        mocked_logger = mocker.patch('aiosqlalchemy_miniorm.orm.logger')

        with pytest.raises(Exception):
            await model_manager.run_query_with_connection(fake_connection, fake_sql)

        mocked_logger.error.assert_called_once_with(mocker.ANY, fake_sql, mocker.ANY)
        fake_connection.execute.assert_called_once_with(fake_sql)


class TestBaseModelManagerPkColumn:
    def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_pk = mocker.Mock()

        mocker.patch.object(model_manager, 'table', **{'primary_key.columns.values.return_value': [fake_pk]})
        compared_pk_column = model_manager._pk_column

        assert compared_pk_column == fake_pk


class TestBaseModelManagerGetSql:
    def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_sql = mocker.patch.object(model_manager, 'sql')

        compared_sql = model_manager.get_sql()

        assert compared_sql == fake_sql

    def test_error(self, model_manager: BaseModelManager, mocker: MockFixture):
        mocker.patch.object(model_manager, 'sql', None)

        with pytest.raises(AssertionError):
            model_manager.get_sql()


class TestBaseModelManagerSetSql:
    def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_sql = mocker.Mock()

        model_manager.set_sql(fake_sql)

        assert model_manager.sql == fake_sql


class TestBaseModelManagerWhere:
    def test_ok_with_where_list(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = ['foo', 'bar']
        mocked_get_sql = mocker.Mock()

        mocker.patch.object(model_manager, 'get_sql', mocked_get_sql)
        mocker.patch.object(model_manager, 'sql')

        model_manager.where(fake_where_list)

        compared_sql = model_manager.sql
        expected_sql = mocked_get_sql.return_value.where.return_value

        calls = [mocker.call(where) for where in fake_where_list]
        mocked_get_sql.return_value.where.assert_has_calls(calls)

        assert compared_sql == expected_sql

    def test_ok_wo_where_list(self, model_manager: BaseModelManager, mocker: MockFixture):
        mocked_get_sql = mocker.Mock()

        mocker.patch.object(model_manager, 'get_sql', mocked_get_sql)

        model_manager.where()

        mocked_get_sql.return_value.where.assert_not_called()


class TestBaseModelManagerOrderBy:
    def test_ok_with_order_by_asc(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_column_name = 'foo'
        fake_order_by = [OrderBy(field=fake_column_name, order='asc')]
        fake_column = mocker.Mock(asc=mocker.Mock())
        mocked_get_sql = mocker.patch.object(model_manager, 'get_sql')

        mocker.patch.object(model_manager, 'table', columns={fake_column_name: fake_column})

        model_manager.order_by(fake_order_by)

        mocked_get_sql.assert_called_once_with()
        assert model_manager.sql == mocked_get_sql.return_value.order_by.return_value
        mocked_get_sql.return_value.order_by.assert_called_once_with(fake_column.asc.return_value)

    def test_ok_with_order_by_desc(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_column_name = 'foo'
        fake_order_by = [OrderBy(field=fake_column_name, order='desc')]
        fake_column = mocker.Mock(desc=mocker.Mock())
        mocked_get_sql = mocker.patch.object(model_manager, 'get_sql')

        mocker.patch.object(model_manager, 'table', columns={fake_column_name: fake_column})

        model_manager.order_by(fake_order_by)

        mocked_get_sql.assert_called_once_with()
        assert model_manager.sql == mocked_get_sql.return_value.order_by.return_value
        mocked_get_sql.return_value.order_by.assert_called_once_with(fake_column.desc.return_value)

    def test_ok_wo_order_by(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_sql = mocker.patch.object(model_manager, 'sql')
        mocked_get_sql = mocker.patch.object(model_manager, 'get_sql')

        model_manager.order_by()

        mocked_get_sql.assert_not_called()

        compared_sql = model_manager.sql

        assert compared_sql == fake_sql


class TestBaseModelManagerOffset:
    def test_ok_with_offset(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_offset = 10
        mocked_get_sql = mocker.patch.object(model_manager, 'get_sql')

        model_manager.offset(fake_offset)

        mocked_get_sql.return_value.offset.assert_called_once_with(fake_offset)

        compared_sql = model_manager.sql
        expected_sql = mocked_get_sql.return_value.offset.return_value

        assert compared_sql == expected_sql

    def test_ok_wo_offset(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_sql = mocker.patch.object(model_manager, 'sql')
        mocked_get_sql = mocker.patch.object(model_manager, 'get_sql')

        model_manager.offset()

        mocked_get_sql.assert_not_called()

        compared_sql = model_manager.sql

        assert compared_sql == fake_sql


class TestBaseModelManagerLimit:
    def test_ok_with_limit(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_limit = 30
        mocked_get_sql = mocker.patch.object(model_manager, 'get_sql')

        model_manager.limit(fake_limit)

        mocked_get_sql.return_value.limit.assert_called_once_with(fake_limit)

        compared_sql = model_manager.sql
        expected_sql = mocked_get_sql.return_value.limit.return_value

        assert compared_sql == expected_sql

    def test_ok_wo_offset(self, model_manager: BaseModelManager, mocker: MockFixture):
        mocked_get_sql = mocker.patch.object(model_manager, 'get_sql')

        model_manager.limit()

        mocked_get_sql.return_value.limit.assert_called_once_with(None)

        compared_sql = model_manager.sql
        expected_sql = mocked_get_sql.return_value.limit.return_value

        assert compared_sql == expected_sql


class TestBaseModelManagerValues:
    def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_values = {'foo': 'bar'}
        mocked_get_sql = mocker.patch.object(model_manager, 'get_sql')

        model_manager.values(**fake_values)

        mocked_get_sql.return_value.values.assert_called_once_with(**fake_values)

        compared_sql = model_manager.sql
        expected_sql = mocked_get_sql.return_value.values.return_value

        assert compared_sql == expected_sql


class TestBaseModelManagerGetItem:
    @pytest.mark.asyncio
    async def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_where = mocker.patch.object(model_manager, 'where', return_value=model_manager)
        mocked_fetchone = mocker.patch.object(model_manager, 'fetchone', CoroutineMock())
        mocked_table = mocker.patch.object(model_manager, 'table')

        compared_result = await model_manager.get_item(fake_where_list)
        expected_result = mocked_fetchone.return_value

        mocked_set_sql.assert_called_once_with(mocked_table.select.return_value)
        mocked_table.select.assert_called_once_with()
        mocked_where.assert_called_once_with(fake_where_list)
        mocked_fetchone.assert_called_once_with()

        assert compared_result == expected_result


class TestBaseModelManagerGetInstance:
    @pytest.mark.asyncio
    async def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        mocked_get_item = mocker.patch.object(
            model_manager,
            'get_item',
            CoroutineMock(return_value={'foo': 'bar'})
        )
        mocked_row_class = mocker.patch.object(model_manager, 'row_class')

        compared_result = await model_manager.get_instance(fake_where_list)
        expected_result = mocked_row_class.return_value

        mocked_get_item.assert_called_once_with(fake_where_list)
        mocked_row_class.assert_called_once_with(**dict(mocked_get_item.return_value))

        assert compared_result == expected_result

    @pytest.mark.asyncio
    async def test_ok_no_results(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        mocked_get_item = mocker.patch.object(model_manager, 'get_item', CoroutineMock(return_value=None))
        mocked_row_class = mocker.patch.object(model_manager, 'row_class')

        compared_result = await model_manager.get_instance(fake_where_list)
        expected_result = None

        mocked_get_item.assert_called_once_with(fake_where_list)
        mocked_row_class.assert_not_called()

        assert compared_result == expected_result


class TestBaseModelManagerInsert:
    @pytest.mark.asyncio
    async def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_values = {'foo': 'bar'}
        fake_fetchedone = {'foo': 'bar'}
        mocked_table = mocker.patch.object(model_manager, 'table', mocker.Mock(columns=[mocker.Mock(), mocker.Mock()]))
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_row_class = mocker.patch.object(model_manager, 'row_class')
        mocked_values = mocker.patch.object(model_manager, 'values', return_value=model_manager)
        mocked_returning = mocker.patch.object(model_manager, 'returning', return_value=model_manager)
        mocked_fetchone = mocker.patch.object(
            model_manager,
            'fetchone',
            CoroutineMock(return_value=fake_fetchedone)
        )

        compared_result = await model_manager.insert(**fake_values)
        expected_result = mocked_row_class.return_value

        mocked_set_sql.assert_called_once_with(mocked_table.insert.return_value)
        mocked_table.insert.assert_called_once_with()
        mocked_values.assert_called_once_with(**fake_values)
        mocked_returning.assert_called_once_with(*mocked_table.columns)
        mocked_fetchone.assert_called_once_with()
        mocked_row_class.assert_called_once_with(**fake_fetchedone)

        assert compared_result == expected_result

    @pytest.mark.asyncio
    async def test_ok_fetch_false(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_values = {'foo': 'bar'}
        mocked_table = mocker.patch.object(model_manager, 'table')
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_values = mocker.patch.object(model_manager, 'values', return_value=model_manager)
        mocked_scalar = mocker.patch.object(model_manager, 'scalar', CoroutineMock())

        compared_result = await model_manager.insert(fetch=False, **fake_values)
        expected_result = mocked_scalar.return_value

        mocked_set_sql.assert_called_once_with(mocked_table.insert.return_value)
        mocked_table.insert.assert_called_once_with()
        mocked_values.assert_called_once_with(**fake_values)
        mocked_scalar.assert_called_once_with()

        assert compared_result == expected_result


class TestBaseModelManagerBulkInsert:
    @pytest.mark.asyncio
    async def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_values = [{'foo': 'bar'}]
        fake_fetchedall = [{'foo': 'bar'}]
        mocked_table = mocker.patch.object(model_manager, 'table', mocker.Mock(columns=[mocker.Mock(), mocker.Mock()]))
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_row_class = mocker.patch.object(model_manager, 'row_class')
        mocked_values = mocker.patch.object(model_manager, 'values', return_value=model_manager)
        mocked_returning = mocker.patch.object(model_manager, 'returning', return_value=model_manager)
        mocked_fetchall = mocker.patch.object(
            model_manager,
            'fetchall',
            CoroutineMock(return_value=fake_fetchedall)
        )

        compared_result = await model_manager.bulk_insert(fake_values)
        expected_result = [mocked_row_class.return_value for _ in fake_fetchedall]

        mocked_set_sql.assert_called_once_with(mocked_table.insert.return_value)
        mocked_table.insert.assert_called_once_with()
        mocked_values.assert_called_once_with(fake_values)
        mocked_returning.assert_called_once_with(*mocked_table.columns)
        mocked_fetchall.assert_called_once_with()

        row_class_calls = [mocker.call(**row) for row in fake_fetchedall]
        mocked_row_class.assert_has_calls(row_class_calls)

        assert compared_result == expected_result

    @pytest.mark.asyncio
    async def test_ok_fetch_false(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_values = [{'foo': 'bar'}]
        mocked_table = mocker.patch.object(model_manager, 'table', mocker.Mock(columns=[mocker.Mock(), mocker.Mock()]))
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_values = mocker.patch.object(model_manager, 'values', return_value=model_manager)
        mocked_rowcount = mocker.patch.object(model_manager, 'rowcount', CoroutineMock())
        compared_result = await model_manager.bulk_insert(fake_values, fetch=False)
        expected_result = mocked_rowcount.return_value

        mocked_set_sql.assert_called_once_with(mocked_table.insert.return_value)
        mocked_table.insert.assert_called_once_with()
        mocked_values.assert_called_once_with(fake_values)
        mocked_rowcount.assert_called_once_with()

        assert compared_result == expected_result


class TestBaseModelManagerUpdate:
    @pytest.mark.asyncio
    async def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        fake_values = {'foo': 'bar'}
        mocked_table = mocker.patch.object(model_manager, 'table', **{
            'update': mocker.Mock(),
            'columns': [mocker.Mock()]
        })
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_values = mocker.patch.object(model_manager, 'values', return_value=model_manager)
        mocked_where = mocker.patch.object(model_manager, 'where', return_value=model_manager)
        mocked_returning = mocker.patch.object(model_manager, 'returning', return_value=model_manager)
        mocked_row_class = mocker.patch.object(model_manager, 'row_class', return_value=model_manager)
        mocked_fetchall = mocker.patch.object(
            model_manager,
            'fetchall',
            CoroutineMock(return_value=[dict(test='test')])
        )

        compared_result = await model_manager.update(fake_where_list, fetch=True, **fake_values)
        expected_result = [mocked_row_class.return_value]

        mocked_set_sql.assert_called_once_with(mocked_table.update.return_value)
        mocked_values.assert_called_once_with(**fake_values)
        mocked_table.update.assert_called_once_with()
        mocked_where.assert_called_once_with(fake_where_list)
        mocked_returning.assert_called_once_with(*mocked_table.columns)
        mocked_values.assert_called_once_with(**fake_values)
        mocked_fetchall.assert_called_once_with()
        mocked_row_class.assert_called_once_with(**dict(mocked_fetchall.return_value.pop()))

        assert compared_result == expected_result

    @pytest.mark.asyncio
    async def test_ok_no_results(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        fake_values = {'foo': 'bar'}
        mocked_table = mocker.patch.object(model_manager, 'table', **{
            'update': mocker.Mock(),
            'columns': [mocker.Mock()]
        })
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_values = mocker.patch.object(model_manager, 'values', return_value=model_manager)
        mocked_where = mocker.patch.object(model_manager, 'where', return_value=model_manager)
        mocked_returning = mocker.patch.object(model_manager, 'returning', return_value=model_manager)
        mocked_row_class = mocker.patch.object(model_manager, 'row_class', return_value=model_manager)
        mocked_fetchall = mocker.patch.object(model_manager, 'fetchall', CoroutineMock(return_value=None))

        compared_result = await model_manager.update(fake_where_list, fetch=True, **fake_values)
        expected_result = []

        mocked_set_sql.assert_called_once_with(mocked_table.update.return_value)
        mocked_values.assert_called_once_with(**fake_values)
        mocked_table.update.assert_called_once_with()
        mocked_where.assert_called_once_with(fake_where_list)
        mocked_returning.assert_called_once_with(*mocked_table.columns)
        mocked_values.assert_called_once_with(**fake_values)
        mocked_fetchall.assert_called_once_with()
        mocked_row_class.assert_not_called()

        assert compared_result == expected_result

    @pytest.mark.asyncio
    async def test_ok_wo_fetch(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        fake_values = {'foo': 'bar'}
        mocked_table = mocker.patch.object(model_manager, 'table')
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_values = mocker.patch.object(model_manager, 'values', return_value=model_manager)
        mocked_where = mocker.patch.object(model_manager, 'where', return_value=model_manager)
        mocked_rowcount = mocker.patch.object(model_manager, 'rowcount', CoroutineMock())

        compared_result = await model_manager.update(fake_where_list, fetch=False, **fake_values)
        expected_result = mocked_rowcount.return_value

        mocked_set_sql.assert_called_once_with(mocked_table.update.return_value)
        mocked_values.assert_called_once_with(**fake_values)
        mocked_table.update.assert_called_once_with()
        mocked_where.assert_called_once_with(fake_where_list)
        mocked_values.assert_called_once_with(**fake_values)
        mocked_rowcount.assert_called_once_with()

        assert compared_result == expected_result


class TestBaseModelManagerDelete:
    @pytest.mark.asyncio
    async def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        mocked_table = mocker.patch.object(model_manager, 'table')
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_where = mocker.patch.object(model_manager, 'where', return_value=model_manager)
        mocked_rowcount = mocker.patch.object(model_manager, 'rowcount', CoroutineMock())

        compared_result = await model_manager.delete(fake_where_list)
        expected_result = mocked_rowcount.return_value

        mocked_table.delete.assert_called_once_with()
        mocked_set_sql.assert_called_once_with(mocked_table.delete.return_value)
        mocked_where.assert_called_once_with(fake_where_list)
        mocked_rowcount.assert_called_once_with()

        assert compared_result == expected_result


class TestBaseModelGetInstances:
    @staticmethod
    @pytest.fixture
    def fake_rows():
        return [{'foo': 'bar'}, {'baz': 'qux'}]

    @pytest.mark.asyncio
    async def test_ok(self, fake_rows, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        fake_order_by = mocker.Mock()
        fake_offset = mocker.Mock()
        fake_limit = mocker.Mock()
        mocked_row_class = mocker.patch.object(model_manager, 'row_class')
        mocked_get_items = mocker.patch.object(
            model_manager,
            'get_items',
            CoroutineMock(return_value=fake_rows)
        )

        actual_result = await model_manager.get_instances(
            where_list=fake_where_list,
            limit=fake_limit,
            offset=fake_offset,
            order_by=fake_order_by
        )

        expected_result = [mocked_row_class.return_value for _ in fake_rows]
        row_class_calls = [mocker.call(**row) for row in fake_rows]
        mocked_row_class.assert_has_calls(row_class_calls)
        mocked_get_items.assert_called_once_with(
            where_list=fake_where_list,
            limit=fake_limit,
            offset=fake_offset,
            order_by=fake_order_by
        )

        assert actual_result == expected_result


class TestBaseModelGetItems:
    @pytest.mark.asyncio
    async def test_ok(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        fake_order_by = mocker.Mock()
        fake_offset = mocker.Mock()
        fake_limit = mocker.Mock()
        mocked_table = mocker.patch.object(model_manager, 'table')
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_where = mocker.patch.object(model_manager, 'where', return_value=model_manager)
        mocked_order_by = mocker.patch.object(model_manager, 'order_by', return_value=model_manager)
        mocked_offset = mocker.patch.object(model_manager, 'offset', return_value=model_manager)
        mocked_limit = mocker.patch.object(model_manager, 'limit', return_value=model_manager)
        mocked_fetchall = mocker.patch.object(model_manager, 'fetchall', CoroutineMock())

        compared_result = await model_manager.get_items(
            where_list=fake_where_list,
            limit=fake_limit,
            offset=fake_offset,
            order_by=fake_order_by
        )
        expected_result = mocked_fetchall.return_value

        mocked_set_sql.assert_called_once_with(mocked_table.select.return_value)
        mocked_table.select.assert_called_once_with()
        mocked_where.assert_called_once_with(fake_where_list)
        mocked_order_by.assert_called_once_with(fake_order_by)
        mocked_offset.assert_called_once_with(fake_offset)
        mocked_limit.assert_called_once_with(fake_limit)
        mocked_fetchall.assert_called_once_with()

        assert compared_result == expected_result


class TestBaseModelTransaction:
    def test_ok(self, mocker: MockFixture, model_manager: BaseModelManager):
        mocked_transaction_cm_cls = mocker.patch('aiosqlalchemy_miniorm.orm._TransactionContextManager')
        mocked_new_instance = mocker.patch.object(model_manager, 'new_instance')

        compared_transaction_cm = model_manager.transaction()
        expected_transaction_cm = mocked_transaction_cm_cls.return_value

        assert compared_transaction_cm == expected_transaction_cm
        mocked_transaction_cm_cls.assert_called_once_with(mocked_new_instance.return_value)


class TestBaseModelNewInstance:
    def test_ok(self, mocker: MockFixture, model_manager: BaseModelManager):
        mocked_table = mocker.patch.object(model_manager, 'table')
        mocked_row_class = mocker.patch.object(model_manager, 'row_class')
        mocked_init = mocker.patch.object(BaseModelManager, '__init__', return_value=None)

        compared_instance = model_manager.new_instance()

        mocked_init.assert_called_once_with(table=mocked_table, row_class=mocked_row_class)
        assert isinstance(compared_instance, BaseModelManager)


class TestBaseModelManagerCount:
    @pytest.mark.asyncio
    async def test_ok_wo_query(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        mocked_table = mocker.patch.object(model_manager, 'table')
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_where = mocker.patch.object(model_manager, 'where', return_value=model_manager)
        mocked_scalar = mocker.patch.object(model_manager, 'scalar', CoroutineMock())

        compared_result = await model_manager.count(where_list=fake_where_list)
        expected_result = mocked_scalar.return_value

        mocked_set_sql.assert_called_once_with(mocked_table.count.return_value)
        mocked_table.count.assert_called_once_with()
        mocked_where.assert_called_once_with(fake_where_list)
        mocked_scalar.assert_called_once_with()

        assert compared_result == expected_result

    @pytest.mark.asyncio
    async def test_ok_with_query(self, model_manager: BaseModelManager, mocker: MockFixture):
        fake_where_list = mocker.Mock()
        fake_query = mocker.Mock()
        mocked_set_sql = mocker.patch.object(model_manager, 'set_sql', return_value=model_manager)
        mocked_where = mocker.patch.object(model_manager, 'where', return_value=model_manager)
        mocked_scalar = mocker.patch.object(model_manager, 'scalar', CoroutineMock())

        compared_result = await model_manager.count(query=fake_query, where_list=fake_where_list)
        expected_result = mocked_scalar.return_value

        mocked_set_sql.assert_called_once_with(fake_query)
        mocked_where.assert_called_once_with(fake_where_list)
        mocked_scalar.assert_called_once_with()

        assert compared_result == expected_result


class TestRowModelDeclarativeMetaGetAttr:
    @staticmethod
    @pytest.fixture
    def fixture_data(mocker: MockFixture):
        class FakeSuperGetAttr:
            __getattr__ = mocker.Mock()

        class FakeRowModel:
            __model_manager_class__ = mocker.MagicMock()
            __table__ = mocker.MagicMock()

            model_manager = None

        RowModelDeclarativeMeta.__bases__ = (type, FakeSuperGetAttr,)

        return RowModelDeclarativeMeta('test', (FakeRowModel,), dict()), FakeSuperGetAttr

    def test_ok_c_attr(self, fixture_data):
        fake_row_model, _ = fixture_data

        compared_attr = fake_row_model.c
        expacted_attr = fake_row_model.__table__.c

        assert compared_attr == expacted_attr

    def test_ok_objects_attr(self, fixture_data):
        fake_row_model, _ = fixture_data

        compared_attr = fake_row_model.objects
        expacted_attr = fake_row_model.model_manager

        fake_row_model.__model_manager_class__.assert_called_once_with(
            table=fake_row_model.__table__,
            row_class=fake_row_model
        )
        assert compared_attr == expacted_attr

    def test_ok_unknown_attr(self, fixture_data):
        fake_row_model, fake_super = fixture_data

        compared_attr = fake_row_model.unknown
        expected_attr = fake_super.__getattr__.return_value

        assert compared_attr == expected_attr


@pytest.fixture
def fake_row_model(mocker: MockFixture):
    class FakeRowModel(RowModel):
        __table__ = mocker.Mock()
        model_manager = mocker.Mock()

    return FakeRowModel()


@pytest.fixture
def fake_columns(mocker: MockFixture):
    return [
        mocker.Mock(key='foo'),
        mocker.Mock(key='bar'),
        mocker.Mock(key='baz'),
        mocker.Mock(key='qux')
    ]


class TestRowModelNew:
    @staticmethod
    @pytest.fixture
    def fixture_data(mocker: MockFixture):
        class FakeSuper:
            __new__ = mocker.Mock()

        class FakeRowModel(RowModel, FakeSuper):
            __table__ = mocker.Mock()
            __model_manager_class__ = mocker.Mock()

        return FakeSuper, FakeRowModel

    def test_ok(self, fixture_data):
        fake_super, fake_row_model = fixture_data

        fake_row_model()

        fake_row_model.__model_manager_class__.assert_called_once_with(
            table=fake_row_model.__table__,
            row_class=fake_row_model
        )
        fake_super.__new__.assert_called_once_with(fake_row_model)


class TestRowModelIter:
    def test_ok(self, fake_columns, fake_row_model, mocker: MockFixture):
        fake_columns_keys = [col.key for col in fake_columns]
        fake_values = {key: mocker.Mock() for key in fake_columns_keys}
        fake_row_model.__table__.columns = fake_columns

        for key in fake_columns_keys:
            setattr(fake_row_model, key, fake_values[key])

        for key, value in fake_row_model:
            assert key in fake_columns_keys
            assert value == fake_values[key]


class TestRowModelTable:
    def test_ok(self, fake_row_model):
        compared_table = fake_row_model.table
        expected_table = fake_row_model.__table__

        assert compared_table == expected_table


class TestRowModelColumns:
    def test_ok(self, fake_columns, fake_row_model):
        fake_row_model.__table__.columns = fake_columns

        compared_columns = fake_row_model.columns

        assert compared_columns == fake_columns


class TestRowModelCheck:
    def test_ok(self, fake_row_model, mocker: MockFixture):
        fake_row_model._sa_instance_state = mocker.Mock(_deleted=False)

        fake_row_model.check()

    def test_error(self, fake_row_model, mocker: MockFixture):
        fake_row_model._sa_instance_state = mocker.Mock(_deleted=True)

        with pytest.raises(Exception):
            fake_row_model.check()


class TestRowModelPkColumn:
    def test_ok(self, fake_row_model, mocker: MockFixture):
        fake_pk_column = mocker.Mock()
        fake_row_model.__table__.primary_key = [fake_pk_column]
        mocker.patch.object(fake_row_model, '__table__', primary_key=[fake_pk_column])

        compared_pk = fake_row_model.pk_column
        expected_pk = fake_pk_column

        assert compared_pk == expected_pk


class TestRowModelAutoincrementColumn:
    def test_ok(self, fake_row_model):
        compared_pk = fake_row_model.autoincrement_column
        expected_pk = fake_row_model.__table__._autoincrement_column

        assert compared_pk == expected_pk


class TestRowModelPkValue:
    def test_ok(self, fake_row_model, mocker: MockFixture):
        fake_pk_key = 'foo'
        fake_row_model.__table__.primary_key = [mocker.Mock(key=fake_pk_key)]
        setattr(fake_row_model, fake_pk_key, mocker.Mock())

        compared_pk_value = fake_row_model._pk_value
        expected_pk_value = fake_row_model.foo

        assert compared_pk_value == expected_pk_value


class TestRowModelGetValues:
    def test_ok(self, fake_columns, fake_row_model, mocker: MockFixture):
        fake_values = {col.key: mocker.Mock() for col in fake_columns}
        fake_row_model.__table__._autoincrement_column = fake_columns[0]
        fake_row_model.__table__.columns = fake_columns
        expected_values = {}

        for col in fake_columns:
            setattr(fake_row_model, col.key, fake_values[col.key])
            if col != fake_columns[0]:
                expected_values.update({
                    col.key: fake_values[col.key]
                })

        compared_values = fake_row_model._get_values()

        assert compared_values == expected_values


class TestRowModelGetValue:
    def test_ok(self, fake_columns, fake_row_model, mocker: MockFixture):
        fake_values = {col.key: mocker.Mock() for col in fake_columns}

        for col in fake_columns:
            setattr(fake_row_model, col.key, fake_values[col.key])

        for col in fake_columns:
            compared_value = fake_row_model._get_value(col.key)
            assert compared_value == fake_values[col.key]


class TestRowModelSetValues:
        def test_ok(self, fake_columns, fake_row_model, mocker: MockFixture):
            fake_values = {col.key: mocker.Mock() for col in fake_columns}
            fake_row_model.__table__.columns = fake_columns
            mocked_get_value = mocker.patch.object(fake_row_model, '_get_value')
            expected_calls = []

            for col in fake_columns:
                setattr(fake_row_model, col.key, None)
                expected_calls.append(mocker.call(col.key))

            fake_row_model._set_values(fake_values)
            mocked_get_value.assert_has_calls(expected_calls, any_order=True)

            for key, value in fake_values.items():
                assert value == getattr(fake_row_model, key)


class TestRowModelInsert:
    @pytest.mark.asyncio
    async def test_ok(self, fake_row_model, mocker: MockFixture):
        returning_data = {'id': 10}
        mocked_model_manager_insert = mocker.patch.object(
            fake_row_model.model_manager,
            'insert',
            new=CoroutineMock(return_value=returning_data)
        )

        mocked_get_values = mocker.patch.object(fake_row_model, '_get_values', return_value={})
        mocked_set_values = mocker.patch.object(fake_row_model, '_set_values')

        expected_result = await fake_row_model.insert()

        mocked_get_values.assert_called_once_with()
        mocked_set_values.assert_called_once_with(returning_data)
        mocked_model_manager_insert.assert_called_once_with(
            **mocked_get_values.return_value
        )

        assert expected_result == fake_row_model


class TestRowModelUpdate:
    @pytest.mark.asyncio
    async def test_ok(self, fake_row_model, mocker: MockFixture):
        fake_pk_key = 'foo'
        fake_kwargs = {'bar': 'baz'}
        fake_row_model.__table__.primary_key = [mocker.Mock(key=fake_pk_key)]
        mocked_check = mocker.patch.object(fake_row_model, 'check')
        mocked_set_values = mocker.patch.object(fake_row_model, '_set_values')
        mocked_model_manager = mocker.patch.object(fake_row_model, 'model_manager', update=CoroutineMock())
        setattr(fake_row_model, fake_pk_key, mocker.Mock())

        await fake_row_model.update(**fake_kwargs)

        mocked_check.assert_called_once_with()
        mocked_model_manager.update.assert_called_once_with(
            where_list=[(fake_row_model.pk_column == fake_row_model._pk_value)],
            fetch=False,
            **fake_kwargs
        )
        mocked_set_values.assert_called_once_with(fake_kwargs)

    @pytest.mark.asyncio
    async def test_ok_none_result(self, fake_row_model, mocker: MockFixture):
        fake_pk_key = 'foo'
        fake_kwargs = {'bar': 'baz'}
        fake_row_model.__table__.primary_key = [mocker.Mock(key=fake_pk_key)]
        mocked_check = mocker.patch.object(fake_row_model, 'check')
        mocked_set_values = mocker.patch.object(fake_row_model, '_set_values')
        mocked_model_manager = mocker.patch.object(
            fake_row_model, 'model_manager', update=CoroutineMock(return_value=None)
        )
        setattr(fake_row_model, fake_pk_key, mocker.Mock())

        await fake_row_model.update(**fake_kwargs)

        mocked_check.assert_called_once_with()
        mocked_model_manager.update.assert_called_once_with(
            where_list=[(fake_row_model.pk_column == fake_row_model._pk_value)],
            fetch=False,
            **fake_kwargs
        )
        mocked_set_values.assert_not_called()


class TestRowModelDelete:
    @pytest.mark.asyncio
    async def test_ok(self, fake_row_model, mocker: MockFixture):
        fake_pk_key = 'foo'
        fake_row_model._sa_instance_state = mocker.Mock(_deleted=False)
        fake_row_model.__table__.primary_key = [mocker.Mock(key=fake_pk_key)]
        mocked_check = mocker.patch.object(fake_row_model, 'check')
        mocked_model_manager = mocker.patch.object(fake_row_model, 'model_manager', delete=CoroutineMock())
        setattr(fake_row_model, fake_pk_key, mocker.Mock())

        await fake_row_model.delete()

        mocked_check.assert_called_once_with()
        mocked_model_manager.delete.assert_called_once_with(
            [(fake_row_model.pk_column == fake_row_model._pk_value)]
        )

        compared_deleted = fake_row_model._sa_instance_state._deleted
        expected_deleted = True
        assert compared_deleted == expected_deleted


class TestTransactionContextManager:
    @pytest.mark.asyncio
    async def test_transaction_cm(self, async_context_manager, mocker: MockFixture):
        exc_type, exc_val, exc_tb = None, None, None
        fake_transaction_cm = async_context_manager(mocker.Mock())
        fake_connection = mocker.Mock(begin=mocker.Mock(return_value=fake_transaction_cm))
        fake_conn_cm = async_context_manager(fake_connection)
        fake_model_mgr = mocker.Mock(
            transaction_connection=None,
            engine=mocker.Mock(acquire=mocker.Mock(return_value=fake_conn_cm))
        )
        mocker.spy(fake_conn_cm, '__aenter__')
        mocker.spy(fake_conn_cm, '__aexit__')

        mocker.spy(fake_transaction_cm, '__aenter__')
        mocker.spy(fake_transaction_cm, '__aexit__')

        transaction = _TransactionContextManager(fake_model_mgr)

        async with transaction as model_objects:
            assert model_objects == fake_model_mgr
            fake_model_mgr.engine.acquire.assert_called_once_with()
            fake_conn_cm.__aenter__.assert_called_once_with()
            fake_transaction_cm.__aenter__.assert_called_once_with()
            fake_connection.begin.assert_called_once_with()
            assert fake_model_mgr.transaction_connection == fake_connection

        fake_transaction_cm.__aexit__.assert_called_once_with(exc_type, exc_val, exc_tb)
        fake_conn_cm.__aexit__.assert_called_once_with(exc_type, exc_val, exc_tb)
        assert fake_model_mgr.transaction_connection is None
