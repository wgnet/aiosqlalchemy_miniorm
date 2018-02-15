# Asynchronous SQLAlchemy Object Relational Mapper.

[![PyPI](https://img.shields.io/pypi/v/aiosqlalchemy-miniorm.svg?maxAge=3600)](https://pypi.python.org/pypi/aiosqlalchemy-miniorm)
[![Python Versions](https://img.shields.io/pypi/pyversions/aiosqlalchemy-miniorm.svg?maxAge=3600)](https://pypi.python.org/pypi/aiosqlalchemy-miniorm)

This is an ORM for accessing SQLAlchemy using asyncio. Working on top of SQLAlchemy Core.

It presents a method of associating user-defined Python classes with database tables, and instances of those classes (objects) with rows in their corresponding tables.

## Usage

Initialization:
    
    from sqlalchemy import MetaData, Integer, String, DateTime
    from aiopg.sa import create_engine
    from aiosqlalchemy_miniorm import RowModel, RowModelDeclarativeMeta, BaseModelManager
    
    metadata = MetaData()
    BaseModel = declarative_base(metadata=metadata, cls=RowModel, metaclass=RowModelDeclarativeMeta)    
    
    async def setup():
        metadata.bind = await create_engine(**database_settings)
    
    class MyEntityManager(BaseModelManager):
        async def get_with_products(self):
            return await self.get_items(where_list=[(MyEntity.c.num_products > 0)])
        
    class MyEntity(BaseModel):
        __tablename__ = 'my_entity'
        __model_manager_class__ = MyEntityManager
    
        id = Column(Integer, primary_key=True)
        name = Column(String(100), nullable=False)
        num_products = Column(Integer)
        created_at = Column(DateTime(), server_default=text('now()'), nullable=False)
    
Query:
    
    objects = await MyEntity.objects.get_instances(
        where_list=[(MyEntity.c.name == 'foo')],
        order_by=['name', '-num_products']
    )
    
    num_objects = await MyEntity.objects.count(
        where_list=[(MyEntity.c.name == 'foo'), (MyEntity.c.num_products > 3)]
    )

or (low-level):
    
    objects = await MyEntity.objects \
        .set_sql(MyEntity.table.select()) \
        .where([(MyEntity.c.name == 'foo')]) \
        .limit(10) \
        .fetchall()
    
Management:
    
    record = await MyEntity.objects.insert(
        name='bar',
        num_products=0,
    )
    
    await record.update(name='baz')
    await record.delete()


Transactions:

    async with MyEntity.objects.transaction() as my_entity_objects:
        await my_entity_objects.insert(name='bar', num_products=0)
        await my_entity_objects.delete([(MyEntity.c.name == 'foo')])
