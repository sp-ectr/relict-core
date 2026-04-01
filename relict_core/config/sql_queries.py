"""
Queries for interacting with PostgreSQL databases.
"""
UPSERT_BOT_CONFIG = """
INSERT INTO bot_configs (chat_id, admin_id, timezone, shard_id, llm_client_name)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (chat_id) DO UPDATE SET
    admin_id = EXCLUDED.admin_id,
    timezone = EXCLUDED.timezone,
    llm_client_name = EXCLUDED.llm_client_name
RETURNING id;
"""

GET_BOT_CONFIG = """
SELECT id, chat_id, admin_id, timezone, shard_id,  llm_client_name
FROM bot_configs
WHERE chat_id = $1;
"""

GET_BOT_CONFIG_BY_ID = """
SELECT id, chat_id, admin_id, timezone, shard_id,  llm_client_name
FROM bot_configs
WHERE id = $1;
"""

DELETE_BOT_CONFIG = """
DELETE FROM bot_configs WHERE chat_id = $1;
"""

DELETE_BOT_CONFIG_BY_ID = """
DELETE FROM bot_configs WHERE id = $1;
"""

INSERT_PARTICIPANT = """
INSERT INTO participants (config_id, user_id, user_name, relationship_score, memories)
VALUES ($1, $2, $3, $4, '{}')
RETURNING id;
"""


GET_PARTICIPANT = """
SELECT config_id, user_id, user_name, relationship_score, is_ignored, last_interaction_at
FROM participants
WHERE config_id = $1 AND user_id = $2;
"""

GET_PARTICIPANTS_WITH_MEMORIES = """
SELECT config_id, user_id, user_name, relationship_score, memories
FROM participants
WHERE config_id = $1
  AND user_id = ANY($2)
"""

UPDATE_PARTICIPANT_MEMORY = """
UPDATE participants
SET memories = (
    SELECT ARRAY(
        SELECT unnest(array_prepend($2::TEXT, memories))
        LIMIT 10
    )
)
WHERE id = $1;
"""


UPDATE_RELATIONSHIP_SCORE = """
UPDATE participants
SET 
    relationship_score = GREATEST(0, LEAST(100, relationship_score + $1)),
    last_interaction_at = now() at time zone 'utc'
WHERE id = $2;
"""

SET_IGNORED_STATUS = """
UPDATE participants
SET 
    is_ignored = $1,
    relationship_score = CASE WHEN $1 THEN 0 ELSE relationship_score END
WHERE id = $2;
"""




