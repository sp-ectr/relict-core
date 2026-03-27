-- Stores base configuration and persona settings for the LLM.
CREATE TABLE IF NOT EXISTS bot_configs (
    id                  SERIAL PRIMARY KEY,
    chat_id             BIGINT NOT NULL UNIQUE,
    admin_id            BIGINT NOT NULL,
    timezone            TEXT NOT NULL,
    shard_id            INTEGER NOT NULL DEFAULT 0,
    llm_client_name     TEXT NOT NULL DEFAULT 'gemini',
    created_at          TIMESTAMPTZ DEFAULT (now() at time zone 'utc'),
    updated_at          TIMESTAMPTZ DEFAULT (now() at time zone 'utc')
);
-- Participants
CREATE TABLE IF NOT EXISTS participants (
    id                      SERIAL PRIMARY KEY,
    config_id               INTEGER NOT NULL REFERENCES bot_configs(id) ON DELETE CASCADE,
    user_id                 BIGINT NOT NULL,
    custom_name             TEXT NOT NULL,
    relationship_score      INTEGER NOT NULL
        CHECK (relationship_score >= 0 AND relationship_score <= 100),
    memories                TEXT[] DEFAULT '{}',
    is_ignored              BOOLEAN NOT NULL DEFAULT false,
    last_interaction_at     TIMESTAMPTZ,
    created_at              TIMESTAMPTZ DEFAULT (now() at time zone 'utc'),
    updated_at              TIMESTAMPTZ DEFAULT (now() at time zone 'utc')
);

-- INDEXES AND TRIGGERS
CREATE INDEX IF NOT EXISTS idx_bot_configs_chat_id ON bot_configs(chat_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_participant ON participants(config_id, user_id);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = (now() at time zone 'utc');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER update_bot_configs_updated_at
BEFORE UPDATE ON bot_configs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE TRIGGER update_participants_updated_at
BEFORE UPDATE ON participants
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();