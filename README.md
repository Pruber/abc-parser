# abc-parser
for my assignment
import os
import re
import sqlite3
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Tuple

# --- Configuration ---
ABC_DIR = 'abc_books'
DB_NAME = 'abc_tunes.db'

# --- Part 1: File Loading & Parsing (30%) ---

def parse_abc_file(file_path: Path, book_number: int) -> List[Dict[str, Any]]:
    """
