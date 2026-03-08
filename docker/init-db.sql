-- Enable pg_trgm extension for GIN trigram search (fund name fuzzy search)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
