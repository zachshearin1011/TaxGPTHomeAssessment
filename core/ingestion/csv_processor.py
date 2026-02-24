from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class Document:

    text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    source: str = ""
    doc_type: str = ""


@dataclass
class GraphTriple:

    subject: str
    subject_type: str
    predicate: str
    object: str
    object_type: str
    properties: dict[str, Any] = field(default_factory=dict)


class CSVProcessor:

    def __init__(self, path: Path):
        self.path = path
        self.df: pd.DataFrame | None = None

    def load(self) -> pd.DataFrame:
        self.df = pd.read_csv(self.path)
        logger.info("Loaded CSV with %d rows and columns: %s", len(self.df), list(self.df.columns))
        return self.df

    def to_documents(self) -> list[Document]:
        if self.df is None:
            self.load()
        assert self.df is not None

        docs: list[Document] = []

        for _, row in self.df.iterrows():
            text = self._row_to_text(row)
            meta = row.to_dict()
            meta["source_file"] = self.path.name
            docs.append(Document(text=text, metadata=meta, source=self.path.name, doc_type="csv_row"))

        docs.extend(self._aggregate_summaries())

        logger.info("Produced %d documents from CSV", len(docs))
        return docs

    def to_graph_triples(self) -> list[GraphTriple]:
        if self.df is None:
            self.load()
        assert self.df is not None

        triples: list[GraphTriple] = []
        seen: set[tuple[str, str, str]] = set()

        for _, row in self.df.iterrows():
            taxpayer = str(row["Taxpayer Type"])
            state = str(row["State"])
            income_src = str(row["Income Source"])
            deduction = str(row["Deduction Type"])
            year = str(row["Tax Year"])

            pairs = [
                (taxpayer, "TaxpayerType", "files_in", state, "State"),
                (taxpayer, "TaxpayerType", "earns_from", income_src, "IncomeSource"),
                (taxpayer, "TaxpayerType", "claims", deduction, "DeductionType"),
                (taxpayer, "TaxpayerType", "filed_in_year", year, "TaxYear"),
                (state, "State", "has_income_source", income_src, "IncomeSource"),
                (income_src, "IncomeSource", "paired_with_deduction", deduction, "DeductionType"),
            ]
            for subj, st, pred, obj, ot in pairs:
                key = (subj, pred, obj)
                if key not in seen:
                    seen.add(key)
                    triples.append(GraphTriple(
                        subject=subj, subject_type=st,
                        predicate=pred,
                        object=obj, object_type=ot,
                    ))

        for (tp, st), grp in self.df.groupby(["Taxpayer Type", "State"]):
            triples.append(GraphTriple(
                subject=str(tp), subject_type="TaxpayerType",
                predicate="stats_in_state",
                object=str(st), object_type="State",
                properties={
                    "avg_income": round(float(grp["Income"].mean()), 2),
                    "avg_tax": round(float(grp["Tax Owed"].mean()), 2),
                    "count": int(len(grp)),
                },
            ))

        logger.info("Produced %d graph triples from CSV", len(triples))
        return triples

    @staticmethod
    def _row_to_text(row: pd.Series) -> str:
        return (
            f"A {row['Taxpayer Type']} taxpayer in {row['State']} for tax year {row['Tax Year']}. "
            f"Transaction date: {row['Transaction Date']}. "
            f"Income source: {row['Income Source']}, amount ${row['Income']:,.2f}. "
            f"Deduction type: {row['Deduction Type']}, amount ${row['Deductions']:,.2f}. "
            f"Taxable income: ${row['Taxable Income']:,.2f}. "
            f"Tax rate: {row['Tax Rate']:.2%}. Tax owed: ${row['Tax Owed']:,.2f}."
        )

    def _aggregate_summaries(self) -> list[Document]:
        assert self.df is not None
        docs: list[Document] = []

        for col in ["Taxpayer Type", "State", "Income Source", "Tax Year"]:
            for val, grp in self.df.groupby(col):
                text = (
                    f"Aggregate statistics for {col}={val}: "
                    f"{len(grp)} records. "
                    f"Total income: ${grp['Income'].sum():,.2f}, "
                    f"average income: ${grp['Income'].mean():,.2f}. "
                    f"Total deductions: ${grp['Deductions'].sum():,.2f}, "
                    f"average deductions: ${grp['Deductions'].mean():,.2f}. "
                    f"Total taxable income: ${grp['Taxable Income'].sum():,.2f}. "
                    f"Average tax rate: {grp['Tax Rate'].mean():.2%}. "
                    f"Total tax owed: ${grp['Tax Owed'].sum():,.2f}, "
                    f"average tax owed: ${grp['Tax Owed'].mean():,.2f}."
                )
                docs.append(Document(
                    text=text,
                    metadata={"group_by": col, "group_value": str(val), "record_count": len(grp), "source_file": self.path.name},
                    source=self.path.name,
                    doc_type="csv_aggregate",
                ))

        return docs