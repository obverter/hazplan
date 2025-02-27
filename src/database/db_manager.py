"""
Database management module for storing and retrieving chemical data.
"""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from sqlalchemy import Column, Float, Integer, String, Text, create_engine, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Rest of the file remains the same...

# Create base class for SQLAlchemy models
Base = declarative_base()


class Chemical(Base):
    """SQLAlchemy model for the chemicals table."""

    __tablename__ = "chemicals"

    # Ensure a primary key is defined
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Existing columns
    cas_number = Column(String(20), unique=True, index=True, nullable=True)
    name = Column(String(255), nullable=False, index=True)
    formula = Column(String(255))
    molecular_weight = Column(Float)
    canonical_smiles = Column(Text)
    isomeric_smiles = Column(Text)
    inchi = Column(Text)
    inchikey = Column(String(27))
    xlogp = Column(Float)
    exact_mass = Column(Float)
    monoisotopic_mass = Column(Float)
    tpsa = Column(Float)
    complexity = Column(Float)
    charge = Column(Integer)
    h_bond_donor_count = Column(Integer)
    h_bond_acceptor_count = Column(Integer)
    rotatable_bond_count = Column(Integer)
    heavy_atom_count = Column(Integer)
    physical_state = Column(String(50))
    color = Column(String(50))
    density = Column(String(100))
    melting_point = Column(String(100))
    boiling_point = Column(String(100))
    flash_point = Column(String(100))
    solubility = Column(Text)
    vapor_pressure = Column(String(100))
    hazard_statements = Column(Text)
    precautionary_statements = Column(Text)
    ghs_pictograms = Column(Text)
    signal_word = Column(String(20))
    source_url = Column(Text)
    source_name = Column(String(255))

    # New columns for parsed property values
    density_value = Column(Float, nullable=True)
    density_unit = Column(String(50), nullable=True)
    melting_point_value = Column(Float, nullable=True)
    melting_point_unit = Column(String(50), nullable=True)
    boiling_point_value = Column(Float, nullable=True)
    boiling_point_unit = Column(String(50), nullable=True)
    flash_point_value = Column(Float, nullable=True)
    flash_point_unit = Column(String(50), nullable=True)
    vapor_pressure_value = Column(Float, nullable=True)
    vapor_pressure_unit = Column(String(50), nullable=True)

    # New columns for toxicity data
    lc50 = Column(String(255), nullable=True)
    ld50 = Column(String(255), nullable=True)
    acute_toxicity_notes = Column(Text, nullable=True)

    def to_dict(self) -> Dict[str, any]:
        """Convert the model to a dictionary."""
        base_dict = {
            "id": self.id,
            "cas_number": self.cas_number,
            "name": self.name,
            "formula": self.formula,
            "molecular_weight": self.molecular_weight,
            "canonical_smiles": self.canonical_smiles,
            "isomeric_smiles": self.isomeric_smiles,
            "inchi": self.inchi,
            "inchikey": self.inchikey,
            "xlogp": self.xlogp,
            "exact_mass": self.exact_mass,
            "monoisotopic_mass": self.monoisotopic_mass,
            "tpsa": self.tpsa,
            "complexity": self.complexity,
            "charge": self.charge,
            "h_bond_donor_count": self.h_bond_donor_count,
            "h_bond_acceptor_count": self.h_bond_acceptor_count,
            "rotatable_bond_count": self.rotatable_bond_count,
            "heavy_atom_count": self.heavy_atom_count,
            "physical_state": self.physical_state,
            "color": self.color,
            "density": self.density,
            "melting_point": self.melting_point,
            "boiling_point": self.boiling_point,
            "flash_point": self.flash_point,
            "solubility": self.solubility,
            "vapor_pressure": self.vapor_pressure,
            "hazard_statements": self.hazard_statements,
            "precautionary_statements": self.precautionary_statements,
            "ghs_pictograms": self.ghs_pictograms,
            "signal_word": self.signal_word,
            "source_url": self.source_url,
            "source_name": self.source_name,
        }

        # Add new parsed value columns to the dictionary
        additional_fields = [
            "density_value",
            "density_unit",
            "melting_point_value",
            "melting_point_unit",
            "boiling_point_value",
            "boiling_point_unit",
            "flash_point_value",
            "flash_point_unit",
            "vapor_pressure_value",
            "vapor_pressure_unit",
        ]

        for field in additional_fields:
            base_dict[field] = getattr(self, field, None)

        return base_dict


class DatabaseManager:
    """
    Database manager for storing and retrieving chemical data.
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the database manager.

        Args:
            db_path: Path to the SQLite database file. If None, a default path
                     will be used in the data directory.
        """
        if db_path is None:
            # Get the project root directory (assuming this file is in src/database/)
            project_root = Path(__file__).parent.parent.parent
            data_dir = project_root / "data"
            os.makedirs(data_dir, exist_ok=True)
            db_path = str(data_dir / "chemical_safety.db")

        # Create the SQLite engine
        self.engine = create_engine(f"sqlite:///{db_path}")

        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)

        logger.info(f"Database initialized at {db_path}")

    def add_chemical(self, chemical_data: Dict[str, any]) -> Optional[int]:
        """
        Add a chemical to the database.

        Args:
            chemical_data: Dictionary containing chemical data

        Returns:
            ID of the inserted chemical, or None if insertion failed
        """
        try:
            with Session(self.engine) as session:
                # Check if chemical with the same CAS number already exists
                cas_number = chemical_data.get("cas_number")
                if cas_number:
                    existing = session.execute(
                        select(Chemical).where(Chemical.cas_number == cas_number)
                    ).scalar_one_or_none()

                    if existing:
                        logger.info(
                            f"Chemical with CAS {cas_number} already exists, updating"
                        )
                        # Update the existing record
                        for key, value in chemical_data.items():
                            if hasattr(existing, key):
                                setattr(existing, key, value)
                        session.commit()
                        return existing.id

                # If no CAS number or chemical not found by CAS, check by name and formula
                name = chemical_data.get("name")
                formula = chemical_data.get("formula")
                if name and formula:
                    existing = session.execute(
                        select(Chemical).where(
                            (Chemical.name == name) & (Chemical.formula == formula)
                        )
                    ).scalar_one_or_none()

                    if existing:
                        logger.info(
                            f"Chemical with name '{name}' and formula '{formula}' already exists, updating"
                        )
                        # Update the existing record
                        for key, value in chemical_data.items():
                            if hasattr(existing, key):
                                setattr(existing, key, value)
                        session.commit()
                        return existing.id

                # Create a new chemical record
                chemical = Chemical(**chemical_data)
                session.add(chemical)
                session.commit()
                logger.info(
                    f"Added chemical: {chemical_data.get('name')} (CAS: {cas_number})"
                )
                return chemical.id
        except Exception as e:
            logger.error(f"Error adding chemical to database: {str(e)}")
            return None

    def get_chemical_by_cas(self, cas_number: str) -> Optional[Dict[str, any]]:
        """
        Get a chemical by its CAS number.

        Args:
            cas_number: CAS registry number

        Returns:
            Dictionary containing chemical data, or None if not found
        """
        try:
            with Session(self.engine) as session:
                chemical = session.execute(
                    select(Chemical).where(Chemical.cas_number == cas_number)
                ).scalar_one_or_none()

                if chemical:
                    return chemical.to_dict()
                return None
        except Exception as e:
            logger.error(f"Error retrieving chemical with CAS {cas_number}: {str(e)}")
            return None

    def search_chemicals(self, query: str) -> List[Dict[str, any]]:
        """
        Search for chemicals by name or CAS number.

        Args:
            query: Search query (partial name or CAS number)

        Returns:
            List of dictionaries containing matching chemical data
        """
        try:
            with Session(self.engine) as session:
                # Print out the full query for debugging
                logger.info(f"Searching for chemical query: {query}")

                # Search by name (case-insensitive LIKE query)
                name_matches = (
                    session.execute(
                        select(Chemical).where(Chemical.name.ilike(f"%{query}%"))
                    )
                    .scalars()
                    .all()
                )

                # Search by CAS number (case-insensitive LIKE query)
                cas_matches = (
                    session.execute(
                        select(Chemical).where(Chemical.cas_number.ilike(f"%{query}%"))
                    )
                    .scalars()
                    .all()
                )

                # Search by formula (case-sensitive)
                formula_matches = (
                    session.execute(
                        select(Chemical).where(Chemical.formula.like(f"%{query}%"))
                    )
                    .scalars()
                    .all()
                )

                # Combine results, removing duplicates
                all_matches = {
                    c.id: c for c in name_matches + cas_matches + formula_matches
                }

                # Log the number of matches found
                logger.info(f"Found {len(all_matches)} matching chemicals")

                # If no matches, log the queries tried
                if not all_matches:
                    logger.warning(
                        f"No matches found for queries: name like '%{query}%', cas_number like '%{query}%', formula like '%{query}%'"
                    )

                return [c.to_dict() for c in all_matches.values()]
        except Exception as e:
            logger.error(f"Error searching chemicals with query '{query}': {str(e)}")
            return []

    def export_to_csv(self, output_path: Optional[str] = None) -> Optional[str]:
        """
        Export the chemicals database to a CSV file.

        Args:
            output_path: Path to save the CSV file. If None, a default path
                         will be used in the data directory.

        Returns:
            Path to the saved CSV file, or None if export failed
        """
        try:
            with Session(self.engine) as session:
                chemicals = session.execute(select(Chemical)).scalars().all()

                if not chemicals:
                    logger.warning("No chemicals to export")
                    return None

                # Convert to dataframe
                data = [c.to_dict() for c in chemicals]
                df = pd.DataFrame(data)

                # Set default output path if not provided
                if output_path is None:
                    project_root = Path(__file__).parent.parent.parent
                    data_dir = project_root / "data" / "processed"
                    os.makedirs(data_dir, exist_ok=True)
                    output_path = str(data_dir / "chemicals_export.csv")

                # Save to CSV
                df.to_csv(output_path, index=False)
                logger.info(f"Exported {len(data)} chemicals to {output_path}")
                return output_path
        except Exception as e:
            logger.error(f"Error exporting chemicals to CSV: {str(e)}")
            return None

    def get_all_chemicals(self) -> List[Dict[str, any]]:
        """
        Get all chemicals in the database.

        Returns:
            List of dictionaries containing all chemical data
        """
        try:
            with Session(self.engine) as session:
                chemicals = session.execute(select(Chemical)).scalars().all()
                return [c.to_dict() for c in chemicals]
        except Exception as e:
            logger.error(f"Error retrieving all chemicals: {str(e)}")
            return []

    def count_chemicals(self) -> int:
        """
        Count the number of chemicals in the database.

        Returns:
            Number of chemicals in the database
        """
        try:
            with Session(self.engine) as session:
                count = session.query(Chemical).count()
                return count
        except Exception as e:
            logger.error(f"Error counting chemicals: {str(e)}")
            return 0
