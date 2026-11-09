-- The container creates the "eventstore" database from POSTGRES_DB.
-- The read side gets its own database so the two sides don't share any tables.
CREATE DATABASE readmodel OWNER ledger;
