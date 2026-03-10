"""
Gemini LLM client implementation.

Provides an adapter between the internal LLM interface and
Google Gemini chat sessions. Manages session lifecycle and
handles JSON request/response serialization.
"""

from google import genai
from google.genai.types import GenerateContentConfig

from relict_core.config.llm_interface import BaseLLMClient
from relict_core.config.relict_settings import LLMSettings
from relict_core.config.schemas import PersonalityManifest, LLMRequest, LLMResponse


class GeminiClient(BaseLLMClient):
    """LLM client that communicates with Google Gemini chat API."""

    def __init__(self, opts: LLMSettings):
        """Initialize Gemini client and internal session storage."""
        self.client = genai.Client(api_key=opts.api_key)
        self.model_name = opts.model_name
        self.sessions: dict = {}

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
            config=GenerateContentConfig(
                system_instruction=system_instruction.model_dump_json(),
                response_mime_type="application/json"
            )
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
