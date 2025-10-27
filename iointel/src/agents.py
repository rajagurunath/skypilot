import dataclasses
import json
import uuid


from .memory import AsyncMemory
from .agent_methods.data_models.datamodels import (
    PersonaConfig,
    Tool,
    ToolUsageResult,
    AgentResult,
    OutputType,
    RunStreamMeta,
)
from .agent_methods.agents.tool_factory import (
    resolve_single_tool,
    instantiate_stateful_tool,
)
from .utilities.rich import pretty_output
from .utilities.constants import get_api_url, get_base_model, get_api_key
from .utilities.helpers import supports_tool_choice_required, flatten_union_types
from .ui.rich_panels import render_agent_result_panel

from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.profiles import ModelProfile
from pydantic_ai.profiles.openai import OpenAIModelProfile
from pydantic_ai import Agent as PydanticAgent, Tool as PydanticTool
from pydantic_ai.agent import AgentRunResult
from pydantic_ai.settings import ModelSettings

from pydantic import ConfigDict, Field, SecretStr, BaseModel, ValidationError
from pydantic_ai.messages import (
    PartDeltaEvent,
    TextPartDelta,
    ToolCallPart,
    UserContent,
    ModelMessage,
    TextPart,
)
from typing import Callable, Dict, Any, Optional, Union, Literal, Sequence


class StreamableAgentResult:
    """
    A wrapper that can act as both AgentResult (for backward compatibility)
    and AsyncGenerator (for streaming capability).
    """

    def __init__(self, stream_generator, blocking_result=None):
        self._stream_generator = stream_generator
        self._blocking_result = blocking_result
        self._consumed = False

    async def __aiter__(self):
        """Enable 'async for' iteration for streaming"""
        if self._consumed:
            raise RuntimeError("Stream can only be consumed once")
        self._consumed = True

        async for item in self._stream_generator:
            yield item

    def __await__(self):
        """Enable 'await' for getting the final result (backward compatibility)"""
        return self._get_blocking_result().__await__()

    async def _get_blocking_result(self):
        """Consume the stream and return the final AgentResult"""
        if self._blocking_result is not None:
            return self._blocking_result

        if self._consumed:
            raise RuntimeError("Stream already consumed, cannot get blocking result")

        self._consumed = True
        final_result = None

        async for item in self._stream_generator:
            if not isinstance(item, str):  # Final AgentResult
                final_result = item

        self._blocking_result = final_result
        return final_result


class PatchedValidatorTool(PydanticTool):
    _PATCH_ERR_TYPES = ("list_type",)

    async def run(self, message: ToolCallPart, *args, **kw):
        if (margs := message.args) and isinstance(margs, str):
            try:
                self.function_schema.validator.validate_json(margs)
            except ValidationError as e:
                try:
                    margs_dict = json.loads(margs)
                except json.JSONDecodeError:
                    pass
                else:
                    patched = False
                    for err in e.errors():
                        if (
                            err["type"] in self._PATCH_ERR_TYPES
                            and len(err["loc"]) == 1
                            and err["loc"][0] in margs_dict
                            and isinstance(err["input"], str)
                        ):
                            try:
                                margs_dict[err["loc"][0]] = json.loads(err["input"])
                                patched = True
                            except json.JSONDecodeError:
                                pass
                    if patched:
                        message = dataclasses.replace(
                            message, args=json.dumps(margs_dict)
                        )
        return await super().run(message, *args, **kw)


class OssAwareOpenAIProvider(OpenAIProvider):
    def model_profile(self, model_name: str) -> ModelProfile | None:
        result: OpenAIModelProfile = super().model_profile(model_name)
        if result and "gpt-oss" in model_name:
            # upstream pydantic isn't aware of gpt-oss models, so thinks it's
            # openai-hosted reasoning model that doesn't support any settings;
            # rectify its assumptions here, disable only penalties as they currently crash VLLM
            result.openai_unsupported_model_settings = [
                "frequency_penalty",
                "presence_penalty",
            ]
        return result


class JsonToolCall(BaseModel):
    name: str
    arguments: dict


class JsonToolCallBase(BaseModel):
    name: str | None = None
    function: str | None = None
    arguments: dict

    model_config = ConfigDict(extra="allow")

    def get_first_call(self) -> JsonToolCall:
        return JsonToolCall(name=(self.name or self.function), arguments=self.arguments)


class JsonToolCallsSection(BaseModel):
    tool_calls: list[JsonToolCallBase] = Field(min_length=1)

    def get_first_call(self) -> "JsonToolCall":
        return self.tool_calls[0].get_first_call()


class Agent(BaseModel):
    """
    A configurable agent that allows you to plug in different chat models,
    instructions, and tools. By default, it uses the pydantic OpenAIModel.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    name: str
    instructions: str
    persona: Optional[PersonaConfig] = None
    context: Optional[Any] = None
    tools: Optional[list] = None
    model: Optional[Union[OpenAIModel, str]] = None
    memory: Optional[AsyncMemory] = None
    model_settings: Optional[ModelSettings | Dict[str, Any]] = (
        None  # dict(extra_body=None), #can add json model schema here
    )
    api_key: SecretStr
    base_url: Optional[str] = None
    output_type: OutputType = str
    _runner: PydanticAgent
    conversation_id: Optional[str] = None
    show_tool_calls: bool = True
    tool_pil_layout: Literal["vertical", "horizontal"] = (
        "horizontal"  # 'vertical' or 'horizontal'
    )
    debug: bool = False
    _allow_unregistered_tools: bool

    # args must stay in sync with AgentParams, because we use that model
    # to reconstruct agents
    def __init__(
        self,
        name: str,
        instructions: str,
        persona: Optional[PersonaConfig] = None,
        context: Optional[Any] = None,
        tools: Optional[list] = None,
        model: Optional[Union[OpenAIModel, str]] = None,
        memory: Optional[AsyncMemory] = None,
        model_settings: Optional[
            ModelSettings | Dict[str, Any]
        ] = None,  # dict(extra_body=None), #can add json model schema here
        api_key: Optional[SecretStr | str] = None,
        base_url: Optional[str] = None,
        output_type: OutputType = str,
        conversation_id: Optional[str] = None,
        retries: int = 3,
        output_retries: int | None = None,
        show_tool_calls: bool = True,
        tool_pil_layout: Literal["vertical", "horizontal"] = "horizontal",
        debug: bool = False,
        allow_unregistered_tools: bool = False,
        tool_validation_kwargs=None,
        **model_kwargs,
    ) -> None:
        """
        :param name: The name of the agent.
        :param instructions: The instruction prompt for the agent.
        :param description: A description of the agent. Visible to other agents.
        :param persona: A PersonaConfig instance to use for the agent. Used to set persona instructions.
        :param tools: A list of Tool instances or @register_tool decorated functions.
        :param model: A callable that returns a configured model instance.
                              If provided, it should handle all model-related configuration.
        :param model_kwargs: Additional keyword arguments passed to the model factory or ChatOpenAI if no factory is provided.
        :param verbose: If True, displays detailed tool usage information during execution.
        :param tool_pil_layout: 'horizontal' (default) or 'vertical' for tool PIL stacking.

        If model_provider is given, you rely entirely on it for the model and ignore other model-related kwargs.
        If not, you fall back to ChatOpenAI with model_kwargs such as model="gpt-4o-mini", api_key="..."

        :param memory: A Memory instance to use for the agent. Memory module can store and retrieve data, and share context between agents.

        """
        resolved_api_key = (
            api_key
            if isinstance(api_key, SecretStr)
            else SecretStr(api_key or get_api_key())
        )
        resolved_base_url = base_url or get_api_url()

        if isinstance(model, OpenAIModel):
            resolved_model = model
        else:
            kwargs = dict(
                model_kwargs,
                provider=OssAwareOpenAIProvider(
                    base_url=resolved_base_url,
                    api_key=resolved_api_key.get_secret_value(),
                ),
            )
            resolved_model = OpenAIModel(
                model_name=model if isinstance(model, str) else get_base_model(),
                **kwargs,
            )

        resolved_tools = [
            self._get_registered_tool(tool, allow_unregistered_tools)
            for tool in (tools or ())
        ]

        model_supports_tool_choice = supports_tool_choice_required(
            resolved_model.model_name
        )

        model_settings = dict(model_settings or {})
        model_settings["supports_tool_choice_required"] = model_supports_tool_choice

        super().__init__(
            name=name,
            instructions=instructions,
            persona=persona,
            context=context,
            tools=resolved_tools,
            model=resolved_model,
            memory=memory,
            model_settings=model_settings,
            api_key=resolved_api_key,
            base_url=resolved_base_url,
            output_type=output_type,
            show_tool_calls=show_tool_calls,
            tool_pil_layout=tool_pil_layout,
            debug=debug,
            conversation_id=conversation_id,
        )
        # wtf pydantic, Y U LOSE values...
        # TODO: figure out why this dirty hack is needed :(
        self.model_settings = model_settings

        self._allow_unregistered_tools = allow_unregistered_tools
        self._runner = PydanticAgent(
            name=name,
            tools=[
                PatchedValidatorTool(
                    fn.get_wrapped_fn(),
                    name=fn.name,
                    description=fn.description,
                    **(tool_validation_kwargs or {}),
                )
                # this allows to pass takes_ctx parameter
                for fn in resolved_tools
            ],
            model=resolved_model,
            model_settings=model_settings,
            output_type=output_type,
            end_strategy="exhaustive",
            retries=retries,
            output_retries=output_retries,
        )
        self._runner.system_prompt(dynamic=True)(self._make_init_prompt)

    @classmethod
    def _get_registered_tool(
        cls, tool: str | Tool | Callable, allow_unregistered_tools: bool
    ) -> Tool:
        tool_name, tool_obj = resolve_single_tool(
            tool, instantiate_stateful_tool, allow_unregistered_tools
        )
        if tool_obj is None:
            raise ValueError(
                f"Tool '{tool_name}' not found in registry, did you forget to @register_tool?"
            )
        return tool_obj

    def _make_init_prompt(self) -> str:
        # Combine user instructions with persona content
        combined_instructions = self.instructions
        # Build a persona snippet if provided
        if isinstance(self.persona, PersonaConfig):
            if persona_instructions := self.persona.to_system_instructions().strip():
                combined_instructions += "\n\n" + persona_instructions

        if self.context:
            combined_instructions += f"""\n\n 
            this is added context, perhaps a previous run, or anything else of value,
            so you can understand what is going on: {self.context}"""
        return combined_instructions

    def add_tool(self, tool):
        registered_tool = self._get_registered_tool(
            tool, self._allow_unregistered_tools
        )
        self.tools += [registered_tool]
        self._runner._register_tool(
            PatchedValidatorTool(registered_tool.get_wrapped_fn())
        )

    def extract_tool_usage_results(
        self, messages: list[ModelMessage]
    ) -> list[ToolUsageResult]:
        """
        Given a list of messages, extract ToolUsageResult objects.
        Handles multiple tool calls/returns per message.
        Returns a list of ToolUsageResult.
        """
        # Collect all tool-calls and tool-returns by tool_call_id
        tool_calls = {}
        tool_returns = {}

        for msg in messages:
            if not hasattr(msg, "parts") or not msg.parts:
                continue
            for part in msg.parts:
                if getattr(part, "part_kind", None) == "tool-call":
                    tool_call_id = getattr(part, "tool_call_id", None)
                    if tool_call_id:
                        tool_calls[tool_call_id] = part
                elif getattr(part, "part_kind", None) == "tool-return":
                    tool_call_id = getattr(part, "tool_call_id", None)
                    if tool_call_id:
                        tool_returns[tool_call_id] = part

        tool_usage_results: list[ToolUsageResult] = []

        # Pair tool-calls with their returns
        for tool_call_id, call_part in tool_calls.items():
            tool_name = getattr(call_part, "tool_name", None)
            tool_args = getattr(call_part, "args", {})
            # Try to parse args if it's a JSON string
            if isinstance(tool_args, str):
                try:
                    tool_args = json.loads(tool_args)
                except Exception:
                    tool_args = {}
            tool_result = None
            if tool_call_id in tool_returns:
                tool_result = getattr(tool_returns[tool_call_id], "content", None)
            tool_usage_results.append(
                ToolUsageResult(
                    tool_name=tool_name,
                    tool_args=tool_args if isinstance(tool_args, dict) else {},
                    tool_result=tool_result,
                )
            )
        return tool_usage_results

    def _postprocess_agent_result(
        self,
        result: AgentRunResult,
        query: str,
        conversation_id: Union[str, int],
        pretty: bool = True,
    ):
        messages: list[ModelMessage] = (
            result.all_messages() if hasattr(result, "all_messages") else []
        )
        tool_usage_results = self.extract_tool_usage_results(messages)

        if pretty and (pretty_output is not None and pretty_output):
            render_agent_result_panel(
                result_output=result.output,
                query=query,
                agent_name=self.name,
                tool_usage_results=tool_usage_results,
                show_tool_calls=self.show_tool_calls,
                tool_pil_layout=self.tool_pil_layout,
            )
        return AgentResult(
            result=result.output,
            conversation_id=conversation_id,
            full_result=result,
            tool_usage_results=tool_usage_results,
        )

    async def _load_message_history(
        self, conversation_id: Optional[str], message_history_limit: int
    ) -> Optional[list[dict[str, Any]]]:
        if self.memory and conversation_id:
            try:
                return await self.memory.get_message_history(
                    conversation_id, message_history_limit
                )
            except Exception as e:
                print("Error loading message history:", e)
        return None

    def _resolve_conversation_id(self, conversation_id: Optional[str]) -> str:
        return conversation_id or self.conversation_id or str(uuid.uuid4())

    def _adjust_output_type(self, kwargs: dict[str, Any]) -> None:
        if not self.model_settings.get("supports_tool_choice_required"):
            output_type = kwargs.get("output_type")
            if output_type is not None and output_type is not str:
                flat_types = flatten_union_types(output_type)
                if str not in flat_types:
                    flat_types = [str] + flat_types
                kwargs["output_type"] = Union[tuple(flat_types)]

    async def run(
        self,
        query: str | Sequence[UserContent],
        conversation_id: Optional[str] = None,
        return_markdown: bool = False,
        pretty: bool = None,
        message_history_limit=100,
        **kwargs,
    ) -> AgentResult:
        """
        Run the agent asynchronously.
        :param query: The query to run the agent on. Can be a string or sequence of multimodal content.
        :param conversation_id: The conversation ID to use for the agent.
        :param return_markdown: Whether to return the result as markdown.
        :param pretty: Whether to pretty print the result as a rich panel, useful for cli or notebook.
        :param message_history_limit: The number of messages to load from the memory.
        :param kwargs: Additional keyword arguments to pass to the agent.
        :return: The result of the agent run.
        """
        return await self.run_stream(
            query,
            conversation_id,
            return_markdown,
            message_history_limit,
            pretty,
            **kwargs,
        )

    async def _stream_tokens(
        self,
        query: str | Sequence[UserContent],
        conversation_id: Optional[str] = None,
        message_history_limit=100,
        **kwargs,
    ):
        """
        Async generator that yields partial content as tokens are streamed from the model.
        At the end, yields RunStreamMeta() with '__final__', 'content', and 'agent_result'.

        If a misplaced tool call was detected, yields RunStreamMeta() with '__tool_retry__'=True.
        """
        self._adjust_output_type(kwargs)

        message_history = await self._load_message_history(
            conversation_id, message_history_limit
        )
        if message_history:
            kwargs["message_history"] = message_history
        known_tool_args = {
            tool.name: set(tool.parameters["properties"].keys()) for tool in self.tools
        }

        async with self._runner.iter(query, **kwargs) as agent_run:
            content = ""
            # instead of doing `async for node in agent_run` unroll the graph manually;
            # it allows mutating or replacing nodes if we need to
            node = agent_run.next_node
            while not self._runner.is_end_node(node):
                old_content = content  # save before streaming in case we'd replace streamed text later
                if self._runner.is_model_request_node(node):
                    async with node.stream(agent_run.ctx) as request_stream:
                        async for event in request_stream:
                            if isinstance(event, PartDeltaEvent) and isinstance(
                                event.delta, TextPartDelta
                            ):
                                delta = event.delta.content_delta or ""
                                content += delta
                                yield delta  # Yield individual delta, not accumulated content
                if self._runner.is_call_tools_node(node):
                    texts = "".join(
                        part.content
                        for part in node.model_response.parts
                        if isinstance(part, TextPart)
                    )
                    for call_model in (JsonToolCallBase, JsonToolCallsSection):
                        try:
                            call = call_model.model_validate_json(
                                texts
                            ).get_first_call()
                        except ValidationError:
                            continue
                        if known_args := known_tool_args.get(call.name):
                            if not (set(call.arguments.keys()) - known_args):
                                # this seems to be a problem that LLM produced a JSON tool call
                                # as output instead of proper tool call - we found a tool that
                                # has matching arguments, so replace TextPart with ToolCallPart
                                node.model_response.parts = [
                                    ToolCallPart(
                                        tool_name=call.name, args=call.arguments
                                    )
                                ]
                                yield RunStreamMeta(__tool_retry__=True)
                                content = old_content  # remove streamed bits that were misplaced by LLM
                                break

                node = await agent_run.next(node)
            # After streaming, yield a special marker with the final result
            yield RunStreamMeta(
                __final__=True,
                content=content,  # Still provide full content in final dict
                agent_result=agent_run.result,
            )

    def run_stream(
        self,
        query: str | Sequence[UserContent],
        conversation_id: Optional[str] = None,
        return_markdown=False,
        message_history_limit=100,
        pretty: bool = None,
        **kwargs,
    ) -> StreamableAgentResult:
        """
        Run the agent with streaming output that supports both streaming and blocking usage.

        Usage:
        # Streaming: async for chunk in agent.run_stream("query"):
        # Blocking: result = await agent.run_stream("query")

        :param query: The query to run the agent on.
        :param conversation_id: The optional conversation ID to use for the agent.
        :param return_markdown: Whether to return the result as markdown.
        :param message_history_limit: The number of messages to load from the memory.
        :param pretty: Whether to pretty print the result as a rich panel, useful for cli or notebook.
        :param kwargs: Additional keyword arguments to pass to the agent.
        :return: StreamableAgentResult that can be awaited or iterated.
        """

        async def stream_generator():
            resolved_conversation_id = self._resolve_conversation_id(conversation_id)
            agent_result = None
            markdown_content = ""

            async for partial in self._stream_tokens(
                query,
                conversation_id=conversation_id,
                message_history_limit=message_history_limit,
                **kwargs,
            ):
                if isinstance(partial, dict) and partial.get("__final__"):
                    markdown_content = partial["content"]
                    agent_result = partial["agent_result"]
                    break
                yield partial  # Stream tokens or some metadata as they arrive

            # Handle memory and postprocessing at the end
            if self.memory:
                try:
                    await self.memory.store_run_history(
                        resolved_conversation_id, agent_result
                    )
                except Exception as e:
                    print("Error storing run history:", e)

            result = self._postprocess_agent_result(
                agent_result, query, resolved_conversation_id, pretty=pretty
            )
            if return_markdown:
                result.result = markdown_content
            yield result  # Final AgentResult

        return StreamableAgentResult(stream_generator())

    def set_context(self, context: Any) -> None:
        """
        Set the context for the agent.
        :param context: The context to set for the agent.
        """
        self.context = context

    @classmethod
    def make_default(cls) -> "Agent":
        return cls(
            name="default-agent",
            instructions="you are a generalist who is good at everything.",
        )

    async def get_conversation_ids(self) -> list[str]:
        if hasattr(self, "memory") and self.memory:
            try:
                convos = await self.memory.list_conversation_ids()
                return convos or []
            except Exception as e:
                print(f"Error fetching conversation IDs: {e}")
        return []

    async def launch_chat_ui(
        self, interface_title: str = None, share: bool = False
    ) -> None:
        """
        Launches a Gradio UI for interacting with the agent as a chat interface.
        """
        try:
            from .ui.io_gradio_ui import IOGradioUI
        except ImportError as e:
            raise ImportError(
                "UI dependencies are not installed. Install with: pip install 'iointel[ui]'"
            ) from e

        ui = IOGradioUI(agent=self, interface_title=interface_title)
        return await ui.launch(share=share)


class LiberalToolAgent(Agent):
    """
    A subclass of iointel.Agent that allows passing in arbitrary callables as tools
    without requiring one to register them first
    """

    def __init__(
        self,
        name: str,
        instructions: str,
        persona: Optional[PersonaConfig] = None,
        context: Optional[Any] = None,
        tools: Optional[list] = None,
        model: Optional[Union[OpenAIModel, str]] = None,
        memory: Optional[AsyncMemory] = None,
        model_settings: Optional[Dict[str, Any]] = None,
        api_key: Optional[SecretStr | str] = None,
        base_url: Optional[str] = None,
        output_type: OutputType = str,
        retries: int = 3,
        output_retries: int | None = None,
        show_tool_calls: bool = True,
        tool_pil_layout: Literal["vertical", "horizontal"] = "horizontal",
        debug: bool = False,
        **model_kwargs,
    ):
        super().__init__(
            name=name,
            instructions=instructions,
            persona=persona,
            context=context,
            tools=tools,
            model=model,
            memory=memory,
            model_settings=model_settings,
            api_key=api_key,
            base_url=base_url,
            output_type=output_type,
            retries=retries,
            output_retries=output_retries,
            allow_unregistered_tools=True,
            show_tool_calls=show_tool_calls,
            tool_pil_layout=tool_pil_layout,
            debug=debug,
            **model_kwargs,
        )
