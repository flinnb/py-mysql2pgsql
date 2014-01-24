from __future__ import with_statement, absolute_import
from contextlib import closing


class PostgresFkHelper(object):
    FIND_ALL_FOREIGN_KEYS_SQL = """
    SELECT tc.constraint_name,
        tc.constraint_type,
        tc.table_name,
        kcu.column_name,
        tc.is_deferrable,
        tc.initially_deferred,
        rc.match_option AS match_type,

        rc.update_rule AS on_update,
        rc.delete_rule AS on_delete,
        ccu.table_name AS references_table,
        ccu.column_name AS references_field
    FROM information_schema.table_constraints tc

    LEFT JOIN information_schema.key_column_usage kcu
        ON tc.constraint_catalog = kcu.constraint_catalog
        AND tc.constraint_schema = kcu.constraint_schema
        AND tc.constraint_name = kcu.constraint_name

    LEFT JOIN information_schema.referential_constraints rc
        ON tc.constraint_catalog = rc.constraint_catalog
        AND tc.constraint_schema = rc.constraint_schema
        AND tc.constraint_name = rc.constraint_name

    LEFT JOIN information_schema.constraint_column_usage ccu
        ON rc.unique_constraint_catalog = ccu.constraint_catalog
        AND rc.unique_constraint_schema = ccu.constraint_schema
        AND rc.unique_constraint_name = ccu.constraint_name

    WHERE lower(tc.constraint_type) in ('foreign key')
    """

    DROP_FOREIGN_KEY_TEMPLATE = "ALTER TABLE {0} DROP CONSTRAINT {1};"
    ADD_FOREIGN_KEY_TEMPLATE = 'ALTER TABLE "{0}" ADD FOREIGN KEY ("{1}") REFERENCES "{2}"({3});'

    COLUMN_CONSTRAINT_NAME = 0
    COLUMN_CONSTRAINT_TYPE = 1
    COLUMN_TABLE_NAME = 2
    COLUMN_COLUMN_NAME = 3
    COLUMN_IS_DEFERRABLE = 4
    COLUMN_INITIALLY_DEFERRED = 5
    COLUMN_MATCH_TYPE = 6
    COLUMN_ON_UPDATE = 7
    COLUMN_ON_DELETE = 8
    COLUMN_REFERENCES_TABLE = 9
    COLUMN_REFERENCES_FIELD = 10

    def __init__(self, conn):
        self.conn = conn
        self.foreign_keys = []

    def find_all_keys(self):
        with closing(self.conn.cursor()) as cur:
            cur.execute(PostgresFkHelper.FIND_ALL_FOREIGN_KEYS_SQL)
            self.foreign_keys = cur.fetchall()

    def remove_foreign_keys(self):
        for foreign_key in self.foreign_keys:
            with closing(self.conn.cursor()) as cur:
                cur.execute(
                    PostgresFkHelper.DROP_FOREIGN_KEY_TEMPLATE.format(
                        foreign_key[PostgresFkHelper.COLUMN_TABLE_NAME],
                        foreign_key[PostgresFkHelper.COLUMN_CONSTRAINT_NAME]
                    )
                )
                self.conn.commit()

    def restore_foreign_keys(self):
        for foreign_key in self.foreign_keys:
            with closing(self.conn.cursor()) as cur:
                cur.execute(
                    PostgresFkHelper.ADD_FOREIGN_KEY_TEMPLATE.format(
                        foreign_key[PostgresFkHelper.COLUMN_TABLE_NAME],
                        foreign_key[PostgresFkHelper.COLUMN_COLUMN_NAME],
                        foreign_key[PostgresFkHelper.COLUMN_REFERENCES_TABLE],
                        foreign_key[PostgresFkHelper.COLUMN_REFERENCES_FIELD]
                    )
                )
                self.conn.commit()
