"""
    Handles database upgrades for NODB.

    Use with care!
"""
import pathlib
import typing as t

import zrlog

from nodb.controller import PostgresController, _PGCursor

DB_ROOT = pathlib.Path(__file__).resolve().absolute().parent / 'db'

class Upgrader:

    def __init__(self, db: PostgresController):
        self.db = db
        self._log = zrlog.get_logger('nodb.upgrade')

    def upgrade(self):
        if not self.version_table_exists():
            self._log.info("No nodb version detected")
            upgrade_start = (0,0)
        else:
            upgrade_start = self.get_last_version()
            self._log.info("Last nodb version detected as (%s, %s)", *upgrade_start)
        for file in self.find_upgrade_files(*upgrade_start):
            self.apply_upgrade_file(*file)
        self._log.info("Upgrade complete")

    def find_upgrade_files(self, last_major_version: int, last_minor_version: int) -> t.Iterable[tuple[pathlib.Path, int, int]]:
        files: list[tuple[pathlib.Path, int, int]] = []
        self._log.debug("Searching for upgrades in %s", DB_ROOT)
        for file in DB_ROOT.rglob("v*_*.sql"):
            pieces = file.name[1:-4].split('_', maxsplit=1)
            if not (pieces[0].isdigit() and pieces[1].isdigit()):
                self._log.debug("Skipping file %s, version components are not numbers", file)
                continue
            major_version = int(pieces[0])
            minor_version = int(pieces[1])
            if major_version > last_major_version or (major_version == last_major_version and minor_version > last_minor_version):
                files.append((file, major_version, minor_version))
            else:
                self._log.debug("Skipping file %s, already applied", file)
        files.sort(key=lambda x: (x[1], x[2]))
        return files

    def apply_upgrade_file(self, file_path: pathlib.Path, major_version: int, minor_version: int):
        with self.db.cursor() as cursor:
            try:
                self._log.info("Applying database file %s", file_path)
                self._apply_upgrade_file(cursor, file_path)
                self._log.trace("Updating major/minor version to %s,%s", major_version, minor_version)
                cursor.execute("INSERT INTO nodb_version (lookup, version_major, version_minor) VALUES (1, %s, %s) ON CONFLICT (lookup) DO UPDATE SET version_major = EXCLUDED.version_major, version_minor = EXCLUDED.version_minor", [major_version, minor_version])
                self._log.trace("Committing changes")
                cursor.commit()
                self._log.debug("Success")
            except Exception:
                self._log.exception("Error applying %s", file_path)
                cursor.rollback()
                self._log.trace("Rollback complete")
                raise

    def _apply_upgrade_file(self, cursor: _PGCursor, file_path: pathlib.Path):

        # A precaution just in case someone tries to call this with a bad database file
        file_path = file_path.resolve().absolute()
        if not str(file_path).startswith(str(DB_ROOT)):
            raise ValueError('Database files must be in the appropriate directory')

        with open(file_path, 'r', encoding='utf-8') as h:
            cursor.execute(h.read())

    def get_last_version(self) -> tuple[int, int]:
        with self.db.cursor() as cursor:
            cursor.execute('SELECT version_major, version_minor FROM nodb_version WHERE lookup = 1')
            result = cursor.fetchone()
            if result:
                return result[0], result[1]
            else:
                return 0, 0

    def version_table_exists(self) -> bool:
        with self.db.cursor() as cursor:
            cursor.execute("SELECT to_regclass('nodb_version') IS NOT NULL AS table_exists")
            result = cursor.fetchone()
            return bool(result[0])
