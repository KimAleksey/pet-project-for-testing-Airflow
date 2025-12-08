import logging
import duckdb
from dotenv import load_dotenv
from os import getenv

class WorkingWithPostgres:
    """
    Utils to work with Postgres via DuckDB
    """
    logger = logging.getLogger(__name__)


    @staticmethod
    def connect_to_postgres_via_duckdb(dbname, host, port, user, password, alias="db"):
        """
        Create connection to DB
        """
        # Check required params
        required = {"dbname": dbname, "host": host, "port": port, "user": user, "password": password}

        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ValueError(f"Missing required parameters: {', '.join(missing)}")

        try:
            conn_str = (
                f"dbname={dbname} "
                f"host={host} "
                f"port={port} "
                f"user={user} "
                f"password={password}"
            )

            duckdb.sql(
                f"""
                ATTACH '{conn_str}' AS {alias} (TYPE postgres);  
                """
            )

            WorkingWithPostgres.logger.info(
                f"Successfully connected to PostgreSQL '{dbname}' via DuckDB as alias '{alias}'"
            )

            return alias

        except Exception as e:
            WorkingWithPostgres.logger.error(
                f"Failed to connect to PostgreSQL '{dbname}' via DuckDB'. {e}"
            )
            raise RuntimeError("Unable to connect") from e


    @staticmethod
    def get_db_credentials() -> dict:
        try:
            load_dotenv()
        except Exception:
            raise RuntimeError("Cannot load DB credentials from .env file")

        return {
            "POSTGRES_DB": getenv("POSTGRES_DB"),
            "POSTGRES_HOST": getenv("POSTGRES_HOST"),
            "POSTGRES_PORT": getenv("POSTGRES_PORT"),
            "POSTGRES_USER_NAME": getenv("POSTGRES_USER_NAME"),
            "POSTGRES_PASSWORD": getenv("POSTGRES_PASSWORD"),
        }


    @staticmethod
    def connect_to_postgres() -> str:
        credentials = WorkingWithPostgres.get_db_credentials()

        alias = WorkingWithPostgres.connect_to_postgres_via_duckdb(
            dbname=credentials["POSTGRES_DB"],
            host=credentials["POSTGRES_HOST"],
            port=credentials["POSTGRES_PORT"],
            user=credentials["POSTGRES_USER_NAME"],
            password=credentials["POSTGRES_PASSWORD"],
            alias="db"
        )

        if not alias:
            raise RuntimeError("Failed to connect to PostgreSQL")
        return alias