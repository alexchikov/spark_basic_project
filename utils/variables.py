from dataclasses import dataclass, fields


@dataclass
class Tables:
    test_table: str = "test_db.basic_table"

    def __iter__(self):
        for f in fields(self):
            yield getattr(self, f.name)


@dataclass
class OutputTables:
    test_table: str = "test_db.test_output_table"

    def __iter__(self):
        for f in fields(self):
            yield getattr(self, f.name)
