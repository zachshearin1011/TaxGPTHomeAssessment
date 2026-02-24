from __future__ import annotations

import os
import tempfile
from pathlib import Path

import django
import pandas as pd
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from core.ingestion.csv_processor import CSVProcessor
from core.storage.graph_store import KnowledgeGraph


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    csv_path = tmp_path / "test_tax_data.csv"
    df = pd.DataFrame({
        "Taxpayer Type": ["Individual", "Corporation", "Partnership", "Trust", "Non-Profit"],
        "Tax Year": [2023, 2023, 2022, 2021, 2023],
        "Transaction Date": ["2023-01-15", "2023-03-20", "2022-06-10", "2021-11-05", "2023-07-22"],
        "Income Source": ["Salary", "Business Income", "Capital Gains", "Rental", "Investment"],
        "Deduction Type": [
            "Mortgage Interest", "Business Expenses", "Charitable Contributions",
            "Medical Expenses", "Education Expenses",
        ],
        "State": ["CA", "NY", "TX", "FL", "IL"],
        "Income": [150000.00, 500000.00, 250000.00, 80000.00, 300000.00],
        "Deductions": [30000.00, 100000.00, 50000.00, 15000.00, 60000.00],
        "Taxable Income": [120000.00, 400000.00, 200000.00, 65000.00, 240000.00],
        "Tax Rate": [0.22, 0.32, 0.24, 0.18, 0.25],
        "Tax Owed": [26400.00, 128000.00, 48000.00, 11700.00, 60000.00],
    })
    df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def knowledge_graph(tmp_path: Path) -> KnowledgeGraph:
    return KnowledgeGraph(persist_dir=tmp_path / "graph_test")


@pytest.fixture
def csv_processor(sample_csv: Path) -> CSVProcessor:
    proc = CSVProcessor(sample_csv)
    proc.load()
    return proc