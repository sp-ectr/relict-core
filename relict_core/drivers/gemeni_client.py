"""
Gemini LLM client implementation.

Provides an adapter between the internal LLM interface and
Google Gemini chat sessions. Manages session lifecycle and
handles JSON request/response serialization.
"""
from typing import Any

from google import genai
from google.genai.types import GenerateContentConfig, Schema, Type

from relict_core.config.llm_interface import BaseLLMClient
from relict_core.config.relict_settings import LLMSettings
from relict_core.config.schemas import PersonalityManifest, LLMRequest, LLMResponse

RESPONSE_SCHEMA = Schema(
    type=Type.OBJECT,
    properties={
        "text_reply": Schema(type=Type.STRING, nullable=True),
        "new_memories": Schema(
            type=Type.OBJECT,
            nullable=True,
            description="Keyed by user_id string, value is memory text string."
        ),
        "respect_updates": Schema(
            type=Type.OBJECT,
            nullable=True,
            description="Keyed by user_id string, value is integer delta."
        ),
        "new_participants": Schema(
            type=Type.OBJECT,
            nullable=True,
            description="Keyed by user_id string, value is user name string.CRITICAL RULE: If a user_id appears in 'messages' but is NOT in 'participants_info', "
                        "you MUST add them to 'new_participants' with their user_name. This is the ONLY way I can remember them."
        ),
        "set_block": Schema(
            type=Type.ARRAY,
            items=Schema(type=Type.INTEGER),
            nullable=True
        ),
    }
)


class GeminiClient(BaseLLMClient):
    """LLM client that communicates with Google Gemini chat API."""

    def __init__(self, opts: LLMSettings):
        """Initialize Gemini client and internal session storage."""
        self.client = genai.Client(api_key=opts.api_key)
        self.model_name = opts.model_name
        self.sessions: dict[str | int, Any] = {}

    @staticmethod
    def _build_config(system_instruction: PersonalityManifest, prompt: LLMRequest) -> GenerateContentConfig:
        """Build Gemini generation config with system instruction and response schema."""
        system_text = (
                system_instruction.model_dump_json() +
                "\n\nFIELD DIRECTIVES:\n" +
                prompt.engine_directives
        )
        return GenerateContentConfig(
            system_instruction=system_text,
            response_mime_type="application/json",
            response_schema=RESPONSE_SCHEMA,
        )

    async def start_session(
            self,
            session_id: str | int,
            system_instruction: PersonalityManifest,
            prompt: LLMRequest,
    ) -> LLMResponse:
        """
        Start a new Gemini chat session.

        Sends the initial prompt together with the system instruction
        (PersonalityManifest) and stores the session internally.
        """
        chat = self.client.aio.chats.create(
            model=self.model_name,
            config=self._build_config(system_instruction, prompt),
        )
        self.sessions[session_id] = chat
        response = await chat.send_message(prompt.model_dump_json())
        return LLMResponse.model_validate_json(response.text)

    async def send_in_session(self, session_id: int, request: LLMRequest) -> LLMResponse:
        """
        Send a message to an existing chat session.

        Uses the stored Gemini chat instance associated with session_id.
        """
        chat = self.sessions[session_id]
        response = await chat.send_message(request.model_dump_json())
        return LLMResponse.model_validate_json(response.text)

    async def end_session(self, session_id: str | int) -> None:
        """
        End a chat session and remove it from the session registry.
        """
        self.sessions.pop(session_id, None)
