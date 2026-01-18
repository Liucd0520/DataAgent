

"""
from langchain_community.utilities import SQLDatabase
from sqlalchemy import create_engine, text
from typing import Any, Dict, Optional, Union, Literal, Sequence
from sqlalchemy.engine import Connection
from sqlalchemy.sql import Executable
from sqlalchemy.engine import Result


# 1. 定义你的达梦专用类（保持不变）
class DamengSQLDatabase(SQLDatabase):
    def _execute(
        self,
        command: Union[str, Executable],
        fetch: Literal["all", "one", "cursor"] = "all",
        *,
        parameters: Optional[Dict[str, Any]] = None,
        execution_options: Optional[Dict[str, Any]] = None,
    ) -> Union[Sequence[Dict[str, Any]], Result]:
        parameters = parameters or {}
        execution_options = execution_options or {}
        with self._engine.begin() as connection:
            if self._schema is not None and self.dialect == "dm":
                # 达梦设置 schema
                connection.exec_driver_sql(
                    f"ALTER SESSION SET CURRENT_SCHEMA = `{self._schema}`",
                    execution_options=execution_options,
                )

            if isinstance(command, str):
                command = text(command)
            cursor = connection.execute(command, parameters, execution_options=execution_options)

            if cursor.returns_rows:
                if fetch == "all":
                    return [x._asdict() for x in cursor.fetchall()]
                elif fetch == "one":
                    first = cursor.fetchone()
                    return [] if first is None else [first._asdict()]
                elif fetch == "cursor":
                    return cursor
                else:
                    raise ValueError("Fetch must be 'one', 'all', or 'cursor'")
            return []


# 2. 保存原始 from_uri
_original_from_uri = SQLDatabase.from_uri


# 3. 重写 from_uri，增加对 dm 的支持
@classmethod
def patched_from_uri(
    cls,
    database_uri: str,
    *,
    engine_args: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> SQLDatabase:
    # 创建临时引擎以获取 dialect
    temp_engine = create_engine(database_uri, **(engine_args or {}))
    dialect_name = temp_engine.dialect.name
    temp_engine.dispose()

    # 如果是达梦，返回自定义类；否则走原逻辑
    if dialect_name == "dm":
        # 注意：这里直接调用父类构造，但实际使用 DamengSQLDatabase
        instance = DamengSQLDatabase.__new__(DamengSQLDatabase)
        DamengSQLDatabase.__init__(instance, create_engine(database_uri, **(engine_args or {})), **kwargs)
        return instance
    else:
        return _original_from_uri(database_uri, engine_args=engine_args, **kwargs)


# 4. 替换原方法（monkey patch）
SQLDatabase.from_uri = patched_from_uri


if __name__ == '__main__':
    from langchain_community.utilities import SQLDatabase

    # 自动识别 dm://... 并使用你的 DamengSQLDatabase
    db = SQLDatabase.from_uri("dm://SYSDBA:SYSDBA_dm001@172.31.24.111:5236/12345")
    # db = SQLDatabase.from_uri("dm+dmPython://SYSDBA:SYSDBA_dm001@172.31.24.111:5236/12345")
    # 'dm': f'dm+dmPython://{username}:{password}@{host}:{port}/{database}',

    # 后续调用和原来一样
    print(db.get_table_info())
    db.run("SELECT * FROM table WHERE id = :id", parameters={"id": 1})

"""