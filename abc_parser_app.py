#!/usr/bin/env python3
"""
ABC File Parser and Analysis application.

- Recursively scans an `abc_books/` directory for .abc files.
- Parses tunes (multiple tunes per file supported).
- Stores parsed tunes into a SQLite DB with book number.
- Loads tunes into pandas for analysis.
- Provides a simple text menu for user queries.

Author: <Your Name>
Date: YYYY-MM-DD
"""

import os
import re
import sqlite3
from typing import Dict, List, Optional, Any
from datetime import datetime

import pandas as pd  # make sure pandas is installed

# ---------------------------
# Parsing utilities
# ---------------------------

TUNE_BLOCK_REGEX = re.compile(r'(?m)^(X:.*?)(?=(?:\nX:)|\Z)', re.DOTALL)


def find_abc_files(base_dir: str) -> List[str]:
    """
    Recursively find all .abc files in base_dir.
    """
    abc_files = []
    for root, _, files in os.walk(base_dir):
        for f in files:
            if f.lower().endswith('.abc'):
                abc_files.append(os.path.join(root, f))
    return sorted(abc_files)


def get_book_number_from_path(file_path: str, base_dir: str) -> Optional[int]:
    """
    Derive the book number from the subfolder of base_dir.
    Example: base_dir/2/tune200.abc -> returns 2
    """
    rel = os.path.relpath(file_path, base_dir)
    parts = rel.split(os.sep)
    if len(parts) >= 2:
        # parent folder is first element
        parent = parts[0]
        if parent.isdigit():
            return int(parent)
    return None


def parse_tune_block(block: str) -> Dict[str, Any]:
    """
    Parse a single tune block (starting with X:) and extract fields.
    Returns a dictionary with keys: x_number, titles (list), composer, rhythm, meter, unit_length, key, tune_text.
    """
    lines = block.strip().splitlines()
    fields = {
        'x_number': None,
        'titles': [],
        'composer': None,
        'rhythm': None,
        'meter': None,
        'unit_length': None,
        'key': None,
        'tune_text': block.strip()
    }
    for line in lines:
        if len(line) >= 2 and line[1] == ':':
            tag = line[0].upper()
            value = line[2:].strip()
            if tag == 'X':
                fields['x_number'] = value
            elif tag == 'T':
                fields['titles'].append(value)
            elif tag == 'C':
                fields['composer'] = value
            elif tag == 'R':
                fields['rhythm'] = value
            elif tag == 'M':
                fields['meter'] = value
            elif tag == 'L':
                fields['unit_length'] = value
            elif tag == 'K':
                fields['key'] = value
            # Add handling for other tags if needed
    # Normalize title to a single string (join multiple T: lines)
    fields['title'] = ' / '.join(fields['titles']) if fields['titles'] else None
    return fields


def parse_abc_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Parse all tunes from a given .abc file and return a list of tune dicts.
    """
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    blocks = TUNE_BLOCK_REGEX.findall(content)
    tunes = []
    for block in blocks:
        tune = parse_tune_block(block)
        tunes.append(tune)
    return tunes


# ---------------------------
# Database utilities
# ---------------------------

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS tunes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    book INTEGER,
    file_path TEXT,
    x_number TEXT,
    title TEXT,
    composer TEXT,
    rhythm TEXT,
    meter TEXT,
    unit_length TEXT,
    key TEXT,
    tune_text TEXT,
    date_added TEXT
);
"""


def init_db(db_path: str) -> sqlite3.Connection:
    """
    Initialize SQLite database and return connection.
    """
    conn = sqlite3.connect(db_path)
    conn.execute(DB_SCHEMA)
    conn.commit()
    return conn


def insert_tune(conn: sqlite3.Connection, tune: Dict[str, Any], book: Optional[int], file_path: str):
    """
    Insert a parsed tune dict into the tunes table.
    """
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO tunes (book, file_path, x_number, title, composer, rhythm, meter, unit_length, key, tune_text, date_added)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            book,
            file_path,
            tune.get('x_number'),
            tune.get('title'),
            tune.get('composer'),
            tune.get('rhythm'),
            tune.get('meter'),
            tune.get('unit_length'),
            tune.get('key'),
            tune.get('tune_text'),
            now
        )
    )
    conn.commit()


# ---------------------------
# High-level Import routine
# ---------------------------

def import_abc_books(base_dir: str, db_path: str) -> int:
    """
    Walk base_dir, parse .abc files and insert tunes into the DB.
    Returns total number of tunes inserted.
    """
    conn = init_db(db_path)
    abc_files = find_abc_files(base_dir)
    total = 0
    for fpath in abc_files:
        book = get_book_number_from_path(fpath, base_dir)
        tunes = parse_abc_file(fpath)
        for tune in tunes:
            insert_tune(conn, tune, book, fpath)
            total += 1
    conn.close()
    return total


# ---------------------------
# Pandas loading & analysis
# ---------------------------

def load_tunes_df(db_path: str) -> pd.DataFrame:
    """
    Load the entire tunes table into a pandas DataFrame.
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM tunes", conn)
    conn.close()
    return df


def get_tunes_by_book(df: pd.DataFrame, book_number: int) -> pd.DataFrame:
    """Get all tunes from a specific book."""
    return df[df['book'] == book_number]


def get_tunes_by_type(df: pd.DataFrame, tune_type: str) -> pd.DataFrame:
    """Get all tunes of a specific rhythm/type (case-insensitive)."""
    mask = df['rhythm'].fillna('').str.lower().str.contains(tune_type.lower())
    return df[mask]


def search_tunes(df: pd.DataFrame, search_term: str) -> pd.DataFrame:
    """Search tunes by title (case insensitive)."""
    mask = df['title'].fillna('').str.lower().str.contains(search_term.lower())
    return df[mask]


def top_composers(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Return top composers by count (non-null)."""
    comp = df['composer'].fillna('Unknown')
    return comp.value_counts().head(top_n).reset_index().rename(columns={'index': 'composer', 'composer': 'count'})


# ---------------------------
# Simple Command-Line Interface
# ---------------------------

def main_menu(db_path: str, base_dir: str):
    """
    Simple interactive menu for the user.
    """
    while True:
        print("\nABC Parser & Analysis")
        print("1) Import ABC books into DB (scan folders and insert)")
        print("2) Load tunes into pandas DataFrame")
        print("3) Show tunes by book")
        print("4) Show tunes by type/rhythm (e.g., jig, reel)")
        print("5) Search tunes by title")
        print("6) Top composers")
        print("7) Exit")
        choice = input("Enter choice: ").strip()
        if choice == '1':
            n = import_abc_books(base_dir, db_path)
            print(f"Imported {n} tunes into {db_path}")
        elif choice == '2':
            df = load_tunes_df(db_path)
            print(f"Loaded {len(df)} tunes.")
            print(df[['id', 'book', 'title', 'rhythm']].head(20).to_string(index=False))
        elif choice == '3':
            book = input("Enter book number: ").strip()
            try:
                bnum = int(book)
            except ValueError:
                print("Invalid number")
                continue
            df = load_tunes_df(db_path)
            res = get_tunes_by_book(df, bnum)
            print(res[['id', 'title', 'rhythm', 'composer']].to_string(index=False))
        elif choice == '4':
            ttype = input("Enter rhythm/type (e.g., reel, jig, hornpipe): ").strip()
            df = load_tunes_df(db_path)
            res = get_tunes_by_type(df, ttype)
            print(res[['id', 'title', 'book', 'composer']].to_string(index=False))
        elif choice == '5':
            term = input("Enter search term for title: ").strip()
            df = load_tunes_df(db_path)
            res = search_tunes(df, term)
            print(res[['id', 'title', 'book', 'rhythm', 'composer']].to_string(index=False))
        elif choice == '6':
            df = load_tunes_df(db_path)
            print(top_composers(df, top_n=15).to_string(index=False))
        elif choice == '7':
            print("Goodbye.")
            break
        else:
            print("Invalid option. Try again.")


# ---------------------------
# If run as script
# ---------------------------

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="ABC Parser & Analysis")
    parser.add_argument('--base_dir', type=str, default='abc_books', help='Base directory containing abc_books/')
    parser.add_argument('--db', type=str, default='tunes.db', help='SQLite DB file path')
    args = parser.parse_args()
    main_menu(db_path=args.db, base_dir=args.base_dir)
