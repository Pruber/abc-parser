

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

def parse_abc_file(filepath: Path, book_id: int) -> List[Dict[str, Any]]:
    """
    Parses a single ABC file and returns a list of tunes.
    
    Args:
        filepath: Path object pointing to the file.
        book_id: Integer representing the book folder.
        
    Returns:
        List of dictionaries, where each dict is a tune.
    """
    tunes = []
    current_tune = {}
    in_tune = False
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check if a new tune is starting (X: indicates start)
            if line.startswith("X:"):
                # If we were already parsing a tune, save the previous one
                if current_tune:
                    tunes.append(current_tune)
                
                # Start new tune
                current_tune = {'book_id': book_id, 'content': line + '\n'}
                current_tune['X'] = line.split(':')[1].strip()
                in_tune = True
                
            elif in_tune:
                # Append line to raw content
                current_tune['content'] += line + '\n'
                
                # Parse Headers
                if line.startswith("T:"):
                    # Only take the first title if multiple exist
                    if 'T' not in current_tune: 
                        current_tune['T'] = line.split(':')[1].strip()
                elif line.startswith("R:"):
                    current_tune['R'] = line.split(':')[1].strip()
                elif line.startswith("K:"):
                    current_tune['K'] = line.split(':')[1].strip()
                    
        # Don't forget the very last tune in the file
        if current_tune:
            tunes.append(current_tune)
            
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        
    return tunes


def process_directory(base_path: str, db: TuneDatabase) -> int:
    """
    Recursively walks directory, parses files, and saves to DB.
    
    Returns:
        Total number of tunes processed.
    """
    total_tunes = 0
    path_obj = Path(base_path)
    
    # Walk through directory
    for file_path in path_obj.rglob('*.abc'):
        # Extract book number from parent folder name
        try:
            parent_folder = file_path.parent.name
            book_id = int(parent_folder)
        except ValueError:
            print(f"Skipping {file_path}: Parent folder '{file_path.parent.name}' is not a valid number.")
            continue
            
        # Parse
        tunes_found = parse_abc_file(file_path, book_id)
        
        # Insert
        for tune in tunes_found:
            db.insert_tune(tune)
            
        total_tunes += len(tunes_found)
        print(f"Processed Book {book_id}: {file_path.name} ({len(tunes_found)} tunes)")
        
    return total_tunes
||Part 2||
