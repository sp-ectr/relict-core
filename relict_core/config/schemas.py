from pydantic import BaseModel


class PersonalityManifest(BaseModel):
    bot_name: str
    persona_description: str
    core_goal: str


class Participant(BaseModel):
    custom_name: str
    gender: str
    relationship_score: int
    memories: list[str] | None = None

