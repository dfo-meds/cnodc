import pathlib
import typing as t

import markupsafe
from sqlparse.tokens import DDL, Keyword, Newline, Whitespace, Comment, Punctuation
from sqlparse.sql import TokenList, Token, Parenthesis, Values, Where



class DatabaseSchema:

    def __init__(self):
        self.tables: dict[str, DatabaseTable] = {}

    def create_table(self, table_name: str, version: tuple[int, int]):
        self.tables[table_name] = DatabaseTable(table_name, version)

    def set_table_comment(self, table_name: str, comment: str | None):
        self.tables[table_name].comment = comment

    def set_column_comment(self, table_name: str, column_name: str, comment: str | None):
        self.tables[table_name].columns[column_name].comment = comment

    def create_column(self, table_name: str, column_def: ColumnDef, version: tuple[int, int]):
        self.tables[table_name].columns[column_def.column_name] = DatabaseColumn(column_def.column_name, version)
        self.tables[table_name].columns[column_def.column_name].set_from_def(column_def)

    def create_index(self,
                     index_name: str,
                     table_name: str,
                     column_names: list[str],
                     index_type: str | None,
                     where_clause: str | None,
                     is_unique: bool,
                     version: tuple[int, int]):
        ...

    def add_constraint(self, table_name: str, constraint: PrimaryKey | ForeignKey, version: tuple[int, int]):
        ...

    def add_table_partition(self, table_name: str, partition_of: str, lower_bound: str, upper_bound: str, version: tuple[int, int]):
        ...

    def drop_index(self, index_name: str, version: tuple[int, int]):
        ...

    def drop_constraint(self, table_name: str, constraint_name: str, version: tuple[int, int]):
        ...

    def drop_table(self, table_name: str, version: tuple[int, int]):
        self.tables[table_name].deprecate(version)

    def drop_column(self, table_name: str, column_name: str, version: tuple[int, int]):
        self.tables[table_name].columns[column_name].deprecate(version)

    def show(self):
        for table_name in sorted(self.tables):
            self.tables[table_name].show()

    def build_html(self,
                   output_file: pathlib.Path,
                   template: pathlib.Path | None = None):
        if template is None:
            template_full = pathlib.Path(__file__).absolute().parent / "_template.html"
        else:
            template_full = template
        with open(output_file, "w") as h:
            with open(template_full, "r") as tf:
                h.write(tf.read().replace("{{ CONTENT }}", self.build_html_content()))

    def build_html_content(self) -> str:
        content = ""
        for table_name in sorted(self.tables.keys()):
            content += self.tables[table_name].build_html_content()
        return content



class DatabaseObject:

    def __init__(self, name: str, created: tuple[int, int], comment: str | None = None):
        self.name = name
        self.created: tuple[int, int] = created
        self.deprecated: tuple[int, int] | None = None
        self.comment = comment

    def set_comment(self, comment: str | None):
        self.comment = comment

    def deprecate(self, version: tuple[int, int]):
        self.deprecated = version


class DatabaseTable(DatabaseObject):

    def __init__(self, name: str, created: tuple[int, int]):
        super().__init__(name, created)
        self.columns: dict[str, DatabaseColumn] = {}

    def show(self):
        s = f"{self.name} [since {'.'.join(str(x) for x in self.created)}]"
        if self.deprecated:
            s += "[deprecated {'.'.join(str(x) for x in self.deprecated)}]"
        print(s)
        if self.comment:
            print(f"# {self.comment}")
        print("=====")
        for col_name in sorted(self.columns.keys()):
            self.columns[col_name].show()
        print("=====\n")

    def build_html_content(self) -> str:
        content = f'<div class="table" id="table_{self.name}">'
        content += '<h2>' + str(markupsafe.escape(self.name)) + ' <span class="version">'
        content += f"{'.'.join(str(x) for x in self.created)}"
        content += '</h2>'
        if self.deprecated:
            content += f'<p class="removed">Removed {'.'.join(str(x) for x in self.deprecated)}</p>'
        if self.comment:
            content += f'<p class="comment">' + str(markupsafe.escape(self.comment)) + '</p>'
        content += '<table cellspacing="0"><thead><tr><th>Column</th><th>Data Type</th><th>Properties</th><th>Comment</th></tr></thead><tbody>'
        for column_name in sorted(self.columns.keys()):
            content += self.columns[column_name].build_html_content()
        content += '</tbody></table></div>'
        return content


class DatabaseColumn(DatabaseObject):

    def __init__(self, name: str, created: tuple[int, int]):
        super().__init__(name, created)
        self.data_type = None
        self.data_type_size = None

    def set_from_def(self, cdef: ColumnDef):
        if cdef.data_type is not None:
            self.data_type = cdef.data_type
            self.data_type_size = cdef.data_type_size

    def build_html_content(self) -> str:
        content = '<tr>'
        content += '<th class="column_name">' + str(markupsafe.escape(self.name))
        content += ' <span class="version">' + '.'.join(str(x) for x in self.created) + '</span>'
        if self.deprecated:
            content += '<span class="removed"> - ' + '.'.join(str(x) for x in self.deprecated) + '</span>'
        content += '</th>'
        content += '<td class="data_type">' + (self.data_type or 'UNKNOWN').upper()
        if self.data_type_size:
            content += ' (' + str(self.data_type_size) + ')'
        content += '</td>'
        content += '<td class="properties"> </td>'
        content += '<td class="comments">' + str(markupsafe.escape(self.comment or "")) + '</td>'
        content += '</tr>'
        return content

    def show(self):
        s = f"- {self.name}"
        print(s)
        if self.comment:
            print(f"  # {self.comment}")




class SqlDatabaseChange:


    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        raise NotImplemented


class TableComment(SqlDatabaseChange):

    def __init__(self, table_name: str, comment: str):
        self.table_name = table_name
        self.comment = comment

    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        db.set_table_comment(self.table_name, self.comment.strip('"') if self.comment else None)


class ColumnComment(SqlDatabaseChange):

    def __init__(self, column_name: str, comment: str):
        self.table_name, self.column_name = column_name.split(".", maxsplit=1)
        self.comment = comment

    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        db.set_column_comment(self.table_name, self.column_name, self.comment.strip('"' if self.comment else None))


class CreateTable(SqlDatabaseChange):

    def __init__(self, table_name: str, columns: list[ColumnDef | ForeignKey | PrimaryKey]):
        self.table_name = table_name
        self.columns = columns

    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        version = (major_version, minor_version)
        db.create_table(self.table_name, version)
        for column in self.columns:
            if isinstance(column, ColumnDef):
                db.create_column(self.table_name, column, version)
            else:
                db.add_constraint(self.table_name, column, version)



class PartitionTable(SqlDatabaseChange):

    def __init__(self, table_name: str, partition_of: str, start: str, end: str):
        self.table_name = table_name
        self.partition_of = partition_of
        self.start = start
        self.end = end

    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        db.add_table_partition(self.table_name, self.partition_of, self.start, self.end, (major_version, minor_version))


class Index(SqlDatabaseChange):

    def __init__(self,
                 index_name: str,
                 table_name: str,
                 column_names: list[str],
                 index_type: str | None = None,
                 where_clause: str | None = None,
                 is_unique: bool = False):
        self.index_name = index_name
        self.index_type = index_type
        self.column_names = column_names
        self.table_name = table_name
        self.where_clause = where_clause
        self.is_unique = is_unique

    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        db.create_index(self.index_name, self.table_name, self.column_names, self.index_type, self.where_clause, self.is_unique, (major_version, minor_version))


class DropTable(SqlDatabaseChange):

    def __init__(self, table_name: str):
        self.table_name = table_name

    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        db.drop_table(self.table_name, (major_version, minor_version))


class AddColumn(SqlDatabaseChange):

    def __init__(self,
                 table_name: str,
                 column_def: ColumnDef):
        self.table_name = table_name
        self.column_def = column_def

    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        db.create_column(self.table_name, self.column_def, (major_version, minor_version))

class DropColumn(SqlDatabaseChange):

    def __init__(self, table_name: str, column_name: str):
        self.table_name = table_name
        self.column_name = column_name

    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        db.drop_column(self.table_name, self.column_name, (major_version, minor_version))



class DropConstraint(SqlDatabaseChange):

    def __init__(self, table_name: str, constraint_name: str):
        self.table_name = table_name
        self.constraint_name = constraint_name

    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        db.drop_constraint(self.table_name, self.constraint_name, (major_version, minor_version))


class DropIndex(SqlDatabaseChange):

    def __init__(self, index_name: str):
        self.index_name = index_name

    def apply(self, db: DatabaseSchema, major_version: int, minor_version: int):
        db.drop_index(self.index_name, (major_version, minor_version))


class ForeignKey:

    def __init__(self, columns: list[str], foreign_table: str, foreign_columns: list[str]):
        self.columns = columns
        self.foreign_table = foreign_table
        self.foreign_columns = foreign_columns


class PrimaryKey:

    def __init__(self, columns: list[str]):
        self.columns = columns

class ColumnDef:

    def __init__(self,
                 column_name: str,
                 data_type: str,
                 data_type_size: str | None = None,
                 default: t.Any = None,
                 not_null: bool = False,
                 constraints: list[str] | None = None):
        self.column_name = column_name
        self.data_type = data_type
        self.data_type_size = data_type_size
        self.default = default
        self.not_null = not_null
        self.constraints = constraints



class SqlStatement:

    def __init__(self, statement: TokenList):
        self.statement: TokenList = statement

    def _relevant_tokens(self) -> list[Token]:
        return self._clean_tokens(self.statement.tokens)

    @staticmethod
    def _clean_tokens(tokens: list[Token]) -> list[Token]:
        return [
            token for token in tokens
            if not (token.ttype in (Newline, Whitespace, Comment) or (token.ttype is None and token.value.startswith("--")))
        ]

    @staticmethod
    def flatten(tokens: list[Token]) -> list[Token]:
        new_tokens = []
        for x in tokens:
            new_tokens.extend(SqlStatement._clean_tokens(x.flatten()))
        return new_tokens

    def _assemble_table_def(self, tokens: list[Token]):
        if tokens[1].ttype is Keyword and tokens[1].value == "PARTITION":
            return self._assemble_partition_table_def(tokens[0], tokens[3], tokens[5])
        elif isinstance(tokens[1], Parenthesis):
            return self._assemble_new_table_def(tokens[0], self.flatten(tokens[1].tokens))

    def _assemble_new_table_def(self, table_name: Token, tokens: list[Token]):
        buffer = []
        columns = []
        depth = -1
        for token in tokens:
            if token.ttype is Punctuation:
                if token.value == "(":
                    depth += 1
                    if depth == 0:
                        continue
                elif token.value == ")":
                    depth -= 1
                    if depth == -1:
                        continue
                elif token.value == "," and depth == 0:
                    columns.append(self._assemble_column(buffer))
                    buffer.clear()
                    continue
            buffer.append(token)
        if buffer:
            columns.append(self._assemble_column(buffer))
        return CreateTable(table_name.value, columns)

    def _assemble_column(self, tokens: list[Token]):
        if tokens[0].ttype is Keyword:
            if tokens[0].value == "FOREIGN":
                return self._assemble_foreign_key(tokens[2:])
            elif tokens[0].value == "PRIMARY KEY":
                return self._assemble_primary_key(tokens[2:-1])

        kwargs = {
            "column_name": tokens[0].value,
            "data_type": tokens[1].value,
        }

        in_data_type_size = False
        data_type_size = []


        in_default = False
        default = []

        in_ref = False
        ref_buffer = []
        refs = []

        idx = 2
        while idx < len(tokens):
            if tokens[idx].value == "(" and not (in_default or data_type_size or in_ref):
                in_data_type_size = True
            elif tokens[idx].value == ")" and in_data_type_size:
                in_data_type_size = False
            elif tokens[idx].value == "NOT NULL":
                kwargs["not_null"] = False
                in_default = False
                in_ref = False
            elif tokens[idx].value == "DEFAULT":
                in_default = True
                in_ref = False
            elif tokens[idx].value == "REFERENCES":
                in_default = False
                in_ref = True
            elif tokens[idx].value == "PRIMARY KEY":
                in_default = False
                in_ref = False
                refs.append(tokens[idx])
            elif in_data_type_size:
                data_type_size.append(tokens[idx])
            elif in_default:
                default.append(tokens[idx])
            elif in_ref:
                ref_buffer.append(tokens[idx])
            idx += 1
        if default:
            kwargs["default"] = " ".join(str(x) for x in default)
        if data_type_size:
            kwargs["data_type_size"] = " ".join(str(x) for x in data_type_size)
        if ref_buffer:
            refs.append(" ".join(str(x) for x in ref_buffer))
        if refs:
            kwargs["constraints"] = refs
        return ColumnDef(**kwargs)

    def _assemble_foreign_key(self, identifiers: list[Token]):
        for idx, element in enumerate(identifiers):
            if element.value == "REFERENCES":
                return ForeignKey(
                    [x.value for x in identifiers[1:idx-1:2]],
                    identifiers[idx+1].value,
                    [x.value for x in identifiers[idx+3:-1:2]]
                )
        raise ValueError("NO REFERENCES")

    def _assemble_primary_key(self, identifiers: list[Token]):
        return PrimaryKey([x.value for x in identifiers[::2]])

    def _assemble_partition_table_def(self, table_name: Token, partition_of_table: Token, values: Values):
        tokens = self._clean_tokens(values.tokens)
        return PartitionTable(
            table_name.value,
            partition_of_table.value,
            tokens[2].value.strip("() \r\t\n"),
            tokens[4].value.strip("() \r\t\n")
        )

    def _assemble_index_def(self, tokens: list[Token], is_unique: bool = False):
        kwargs = {
            "index_name": tokens[0].value,
            "is_unique": is_unique
        }
        if len(tokens) >= 4 and tokens[3].value == "USING":
            kwargs["table_name"] = tokens[2].value
            method_info = self._clean_tokens(tokens[4].flatten())
            kwargs["index_type"] = method_info[0].value
            kwargs["column_names"] = [str(x) for x in method_info[2::2]]
            start_at = 5

        else:
            table_cols = self._clean_tokens([x for x in tokens[2].flatten()])
            kwargs["table_name"] = table_cols[0].value
            kwargs["column_names"] = [x.value for x in table_cols[2::2]]
            start_at = 3
        if isinstance(tokens[start_at], Where):
            kwargs["where_clause"] = " ".join(str(x) for x in self._clean_tokens(tokens[start_at].tokens)[1:])
        return Index(**kwargs)

    def _assemble_function(self, tokens: list[Token]):
        return None

    def _assemble_procedure(self, tokens: list[Token]):
        return None

    def _assemble_trigger(self, tokens: list[Token]):
        return None

    def _skip_if_not_exists(self, tokens: list[Token]) -> list[Token]:
        if tokens[0].ttype is Keyword and tokens[0].value == "IF NOT EXISTS":
            return tokens[1:]
        else:
            return tokens

    def get_change(self) -> SqlDatabaseChange | None:
        tokens = self._relevant_tokens()
        token = tokens[0]
        if token.ttype is DDL and token.value in ("CREATE", "CREATE OR REPLACE"):

            if tokens[1].ttype is Keyword:
                if tokens[1].value == "TABLE":
                    return self._assemble_table_def(self._skip_if_not_exists(tokens[2:]))
                elif tokens[1].value == "INDEX":
                    return self._assemble_index_def(self._skip_if_not_exists(tokens[2:]))
                elif tokens[1].value == "UNIQUE" and tokens[2].ttype is Keyword and tokens[2].value == "INDEX":
                    return self._assemble_index_def(self._skip_if_not_exists(tokens[3:]), True)
                elif tokens[1].value == "FUNCTION":
                    return self._assemble_function(self._skip_if_not_exists(tokens[2:]))
                elif tokens[1].value == "PROCEDURE":
                    return self._assemble_procedure(self._skip_if_not_exists(tokens[3:]))
                elif tokens[1].value == "TRIGGER":
                    return self._assemble_trigger(self._skip_if_not_exists(tokens[2:]))
        elif token.ttype is Keyword and token.value == "COMMENT":
            if tokens[2].ttype is Keyword and tokens[2].value == "TABLE":
                return TableComment(tokens[3].value, tokens[5].value)
            if tokens[2].ttype is Keyword and tokens[2].value == "COLUMN":
                return ColumnComment(tokens[3].value, tokens[5].value)
        elif token.ttype is Keyword and token.value == "DO":
            return None
        elif token.ttype is DDL and token.value == "ALTER":
            if tokens[3].value == "DROP" and tokens[4].value == "COLUMN":
                return DropColumn(tokens[2].value, tokens[5].value)
            elif tokens[3].value == "ADD" and tokens[4].value == "COLUMN":
                return AddColumn(tokens[2].value, t.cast(ColumnDef, self._assemble_column(tokens[5:])))
            elif tokens[3].value == "DROP" and tokens[4].value == "CONSTRAINT":
                return DropConstraint(tokens[2].value, tokens[5].value)
        elif token.ttype is DDL and token.value == "DROP":
            if tokens[1].value == "INDEX":
                return DropIndex(tokens[-2].value)
            elif tokens[1].value == "TABLE":
                return DropTable(tokens[-2].value)
        print("unrecognized statement")
        print(tokens)
        return None

    @staticmethod
    def parse(s: str) -> t.Generator[SqlStatement, None, None]:
        import sqlparse
        for statement in sqlparse.parse(s):
            yield SqlStatement(statement)


class DatabaseDocumenter:

    def __init__(self):
        self._db_path = pathlib.Path(__file__).parent.absolute()
        self._changes: dict[int, dict[int, list[SqlDatabaseChange]]] = {}
        self.schema = DatabaseSchema()


    def build(self) -> DatabaseSchema:
        for file in self._db_path.glob("v*_*.sql"):
            self.load_file(file)
        for major_version in sorted(self._changes.keys()):
            for minor_version in sorted(self._changes[major_version].keys()):
                for change in self._changes[major_version][minor_version]:
                    change.apply(self.schema, major_version, minor_version)
        return self.schema


    def load_file(self, file_path: pathlib.Path):
        pieces = file_path.name[1:-4].split("_", maxsplit=1)
        if len(pieces) == 2 and pieces[0].isdigit() and pieces[1].isdigit():
            with open(file_path, "r") as h:
                for statement in SqlStatement.parse(h.read()):
                    change = statement.get_change()
                    if change is not None:
                        self.add_change(change, int(pieces[0]), int(pieces[1]))

    def add_change(self, change: SqlDatabaseChange, major_version: int, minor_version: int):
        if major_version not in self._changes:
            self._changes[major_version] = {}
        if minor_version not in self._changes[major_version]:
            self._changes[major_version][minor_version] = []
        self._changes[major_version][minor_version].append(change)
