"""
Queries for interacting with PostgreSQL databases.
"""
UPSERT_BOT_CONFIG = """
INSERT INTO bot_configs (chat_id, bot_name, admin_id, timezone, personality_prompt)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (chat_id) DO UPDATE SET
    bot_name = EXCLUDED.bot_name,
    admin_id = EXCLUDED.admin_id,
    timezone = EXCLUDED.timezone,
    personality_prompt = EXCLUDED.personality_prompt
RETURNING id;
"""

GET_BOT_CONFIG = """
SELECT id, chat_id, bot_name, admin_id, timezone, personality_prompt
FROM bot_configs
WHERE chat_id = $1;
"""

GET_BOT_CONFIG_BY_ID = """
SELECT id, chat_id, bot_name, admin_id, timezone, personality_prompt
FROM bot_configs
WHERE id = $1;
"""

GET_TIMEZONE_BY_CONFIG_ID = """
SELECT timezone
FROM bot_configs
WHERE id = $1;
"""

GET_ALL_BOT_CONFIGS = """
SELECT id, chat_id, bot_name, admin_id, timezone, personality_prompt 
FROM bot_configs;
"""

DELETE_BOT_CONFIG = """
DELETE FROM bot_configs WHERE chat_id = $1;
"""

INSERT_PARTICIPANT = """
INSERT INTO participants (config_id, user_id, custom_name, gender)
VALUES ($1, $2, $3, $4)
RETURNING id, custom_name;
"""

UPDATE_PERSONALITY_PROMPT = (
    "UPDATE bot_configs SET personality_prompt = $1 WHERE id = $2;"
)

GET_PARTICIPANT = """
SELECT id, custom_name, gender, relationship_score, is_ignored, last_interaction_at
FROM participants
WHERE config_id = $1 AND user_id = $2;
"""

GET_PARTICIPANTS_WITH_MEMORIES = """
SELECT
    p.id,
    p.user_id,
    p.custom_name,
    p.gender,
    p.relationship_score,
    COALESCE(
        (
            SELECT ARRAY_AGG(ltm.memory_summary ORDER BY ltm.created_at DESC)
            FROM long_term_memory ltm
            WHERE ltm.participant_id = p.id
        ),
        '{}'
    ) AS memories
FROM
    participants p
WHERE
    p.config_id = $1 AND p.is_ignored = false;
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

INSERT_LONG_TERM_MEMORY = """
WITH new_memory AS (
    INSERT INTO long_term_memory (participant_id, memory_summary)
    VALUES ($1, $2)
    RETURNING *
),
ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY created_at DESC) AS rn
    FROM long_term_memory
)
DELETE FROM long_term_memory
WHERE id IN (SELECT id FROM ranked WHERE rn > 10);
"""

DELETE_BOT_CONFIG_BY_ID = """
DELETE FROM bot_configs WHERE id = $1;
"""

