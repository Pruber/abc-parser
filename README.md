

import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Dict, Optional, Any
from pathlib import Path

# ==========================================
# PART 1: Database & Parsing Logic
# ==========================================

class TuneDatabase:
    """Handles all SQL interactions using SQLite."""

    def __init__(self, db_name: str = "tunes.db"):
        """Initialize database connection and schema."""
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self) -> None:
        """Creates the tunes table if it doesn't exist."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS tunes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                book_id INTEGER,
                reference_number TEXT,
                title TEXT,
                rhythm TEXT,
                key_sig TEXT,
                content TEXT
            )
        """)
        self.conn.commit()

    def insert_tune(self, tune_data: Dict[str, Any]) -> None:
        """
        Inserts a single tune dictionary into the database.
        
        Args:
            tune_data: Dictionary containing tune details.
        """
        self.cursor.execute("""
            INSERT INTO tunes (book_id, reference_number, title, rhythm, key_sig, content)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            tune_data.get('book_id'),
            tune_data.get('X'),
            tune_data.get('T', 'Unknown'),
            tune_data.get('R', 'Unknown'),
            tune_data.get('K', 'Unknown'),
            tune_data.get('content', '')
        ))
        self.conn.commit()

    def close(self) -> None:
        """Closes the database connection."""
        self.conn.close()
